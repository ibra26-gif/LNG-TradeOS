#!/usr/bin/env python3
"""
Korea daily data scraper for LNG TradeOS.

Sources (all globally accessible, no API key, no Korean-IP requirement):
  - KHNP open NPP operations portal — LIVE per-reactor output % and MWe
    (npp.khnp.co.kr is reachable globally; only the cms. subdomain on the
    realTimeMgr path is blocked. The npp. subdomain serves the same data
    via a JSON API used by their public dashboard, refreshed every ~3 min.)
  - ECB euro reference rates — KRW/USD via cross-rate

KOGAS tariff Excel and KOGAS imports remain manual until either a Korean
proxy lands or the user enters values. DART quarterly filings need the
user's free OpenDART API key (1-week approval).

Output: data/korea.json
Cadence: daily 17:30 UTC (configurable; KHNP refreshes every ~3 min, but
daily commits are fine — site-level counts only change on outages, which
are rare. Could move to hourly later if needed.)
"""
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# Prefer requests (more robust SSL session handling than urllib for sites
# like IAEA PRIS that flake on TLS handshakes). Fall back to urllib if not.
try:
    import requests as _requests
    _USE_REQUESTS = True
except ImportError:
    import urllib.request
    _USE_REQUESTS = False

OUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'korea.json')
UA = 'Mozilla/5.0 (LNG-TradeOS Korea scraper)'

# Map PRIS reactor name prefix → site label shown in the dashboard.
# SHIN- variants merged into their parent site to mirror Korean industry usage.
SITE_MAP = {
    'HANUL':       'Hanul',
    'SHIN-HANUL':  'Hanul',
    'HANBIT':      'Hanbit',
    'KORI':        'Kori',
    'SHIN-KORI':   'Kori',
    'SAEUL':       'Saeul',
    'WOLSONG':     'Wolsong',
    'SHIN-WOLSONG':'Wolsong',
}


def fetch(url, timeout=20, retries=4):
    """HTTP GET with retries.

    Python's default SSL stack rejects the cipher suite IAEA PRIS serves
    (SSLZeroReturnError on every attempt), but the system curl works
    cleanly against the same URL. We try Python first (fast, in-process)
    and fall back to a curl subprocess if it fails — this keeps the
    common case efficient while making PRIS reliably reachable.
    """
    import subprocess
    last = None
    for attempt in range(retries):
        try:
            if _USE_REQUESTS:
                r = _requests.get(url, headers={'User-Agent': UA}, timeout=timeout)
                r.raise_for_status()
                return r.text
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode('utf-8')
        except Exception as e:
            last = e
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    # All Python attempts failed — try curl subprocess as a final fallback.
    try:
        out = subprocess.run(
            ['curl', '-fsS', '-A', UA, '--max-time', str(timeout), url],
            check=True, capture_output=True, text=True, timeout=timeout + 5,
        )
        return out.stdout
    except Exception as e:
        raise last if last else e


# KHNP unit codes per site. unitCd is the per-reactor identifier in the
# /branch-operation-info-by-plant API. Discovered by scraping each site's
# unit dropdown on npp.khnp.co.kr/ON004004002002NNN. branchCd3 (ho) is
# unused by the API for unit filtering — kept here as a label only.
KHNP_SITES = [
    {'site': 'Kori',    'branchCd': 'BR0302', 'units': [
        ('2112', 'Kori-2', 2),       ('2123', 'Kori-3', 3),
        ('2124', 'Kori-4', 4),       ('2135', 'Shin-Kori-1', 1),
        ('2136', 'Shin-Kori-2', 2),
    ]},
    {'site': 'Hanbit',  'branchCd': 'BR0303', 'units': [
        ('2311', 'Hanbit-1', 1),     ('2312', 'Hanbit-2', 2),
        ('2323', 'Hanbit-3', 3),     ('2324', 'Hanbit-4', 4),
        ('2335', 'Hanbit-5', 5),     ('2336', 'Hanbit-6', 6),
    ]},
    {'site': 'Hanul',   'branchCd': 'BR0304', 'units': [
        ('2411', 'Hanul-1', 1),      ('2412', 'Hanul-2', 2),
        ('2423', 'Hanul-3', 3),      ('2424', 'Hanul-4', 4),
        ('2435', 'Hanul-5', 5),      ('2436', 'Hanul-6', 6),
        ('2711', 'Shin-Hanul-1', 1), ('2712', 'Shin-Hanul-2', 2),
    ]},
    {'site': 'Wolsong', 'branchCd': 'BR0305', 'units': [
        ('2212', 'Wolsong-2', 2),    ('2223', 'Wolsong-3', 3),
        ('2224', 'Wolsong-4', 4),    ('2235', 'Shin-Wolsong-1', 1),
        ('2236', 'Shin-Wolsong-2', 2),
    ]},
    {'site': 'Saeul',   'branchCd': 'BR0312', 'units': [
        ('2811', 'Saeul-1', 1),      ('2812', 'Saeul-2', 2),
    ]},
]


def fetch_khnp_unit(branch_cd, unit_cd, ho, session_cookies=None):
    """Hit KHNP's /branch-operation-info-by-plant for a single reactor.

    Returns None on failure or empty response (e.g. unit shutdown / not in
    KHNP's monitored list any more). Live data refreshes every ~3 minutes
    on KHNP's side; we capture whatever the latest snapshot is.
    """
    import json as _json
    url = 'https://npp.khnp.co.kr/branch-operation-info-by-plant'
    payload = _json.dumps({'branchCd': branch_cd, 'branchCd2': unit_cd, 'branchCd3': str(ho)})
    headers = {
        'User-Agent': UA,
        'Accept': 'application/json, */*; q=0.01',
        'Content-Type': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://npp.khnp.co.kr/ON004004002002001',
        'Origin': 'https://npp.khnp.co.kr',
    }
    try:
        if _USE_REQUESTS:
            r = _requests.post(url, data=payload, headers=headers,
                               cookies=session_cookies, timeout=15)
            if r.status_code != 200 or not r.text.strip():
                return None
            return r.json()
        # urllib fallback
        req = urllib.request.Request(url, data=payload.encode('utf-8'), headers=headers, method='POST')
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode('utf-8')
        return _json.loads(body) if body.strip() else None
    except Exception:
        return None


def fetch_khnp_session():
    """Hit the npp.khnp.co.kr root once to obtain the WMONID session cookie
    that the API requires. Without this, the API returns empty 200s."""
    if not _USE_REQUESTS:
        return None
    s = _requests.Session()
    s.headers.update({'User-Agent': UA})
    try:
        s.get('https://npp.khnp.co.kr/', timeout=15)
        return s
    except Exception:
        return None


def fetch_khnp_live():
    """Pull live per-reactor output from KHNP's open ops portal.

    Returns a structure compatible with the front-end (reactors[], bySite[],
    totalOnlineGW, onlineCount, totalCount), plus the underlying KHNP
    timestamps so the front-end can display data freshness honestly.
    """
    session = fetch_khnp_session()
    cookies = session.cookies if session else None

    reactors = []
    timestamps = []
    for site in KHNP_SITES:
        for unit_cd, name, ho in site['units']:
            data = fetch_khnp_unit(site['branchCd'], unit_cd, ho,
                                    session_cookies=cookies)
            if not data:
                # Append a stub with status unknown so site totals stay sensible
                reactors.append({
                    'name': name, 'site': site['site'], 'unit_cd': unit_cd,
                    'status': 'Unknown', 'output_pct': None, 'output_mwe': None,
                    'asOf': None,
                })
                continue
            udo = data.get('unitDetailOutput') or {}
            no1 = udo.get('NO_1') or {}
            no8 = udo.get('NO_8') or {}
            ul = (data.get('unitInfoList') or [{}])[0]
            # Validate the response is for the requested reactor. plantCd
            # in unitDetailOutput is the authoritative identifier — the
            # unitInfoList[0] sometimes returns the first unit at the
            # site instead of the requested one (KHNP server-side quirk).
            api_unit = udo.get('plantCd')
            if api_unit and api_unit != unit_cd:
                reactors.append({
                    'name': name, 'site': site['site'], 'unit_cd': unit_cd,
                    'status': 'Unknown', 'output_pct': None, 'output_mwe': None,
                    'asOf': None,
                })
                continue
            try:
                output_pct = float(no1.get('VALUE')) if no1.get('VALUE') is not None else None
            except (TypeError, ValueError):
                output_pct = None
            try:
                output_mwe = float(no8.get('VALUE')) if no8.get('VALUE') is not None else None
            except (TypeError, ValueError):
                output_mwe = None
            # Status: derive from generator output, not the kh_status field
            # (which is unreliable for some units). Unit clearly operational
            # if MWe > 50, clearly offline if MWe is null/0.
            kh_status = ul.get('status') or ''
            if output_mwe is not None and output_mwe > 50:
                status = 'Operational'
            elif output_mwe is None or output_mwe < 5:
                status = 'Maintenance' if kh_status == 'KH1202' else 'Offline'
            else:
                status = 'Partial'
            ts = no1.get('TIME') or no8.get('TIME')
            if ts:
                timestamps.append(ts)
            reactors.append({
                'name':       name,
                'site':       site['site'],
                'unit_cd':    unit_cd,
                'status':     status,
                'output_pct': output_pct,
                'output_mwe': output_mwe,
                'asOf':       ts,
            })

    # Aggregate per site
    sites_dict = {}
    for r in reactors:
        s = sites_dict.setdefault(r['site'], {
            'site': r['site'], 'reactors': [], 'totalCount': 0, 'online': 0,
            'totalCap_GW': 0.0, 'onlineCap_GW': 0.0,
        })
        s['reactors'].append(r['name'])
        s['totalCount'] += 1
        # Totalcap = sum of all reactor MWe (when reported, these are nameplate-ish).
        # Onlinecap = sum of MWe for operational reactors.
        if r['output_mwe'] and r['output_mwe'] > 0:
            s['totalCap_GW'] += r['output_mwe'] / 1000.0
        if r['status'] == 'Operational':
            s['online'] += 1
            if r['output_mwe']:
                s['onlineCap_GW'] += r['output_mwe'] / 1000.0

    by_site = sorted(sites_dict.values(), key=lambda x: -x['onlineCap_GW'])
    for s in by_site:
        s['totalCap_GW'] = round(s['totalCap_GW'], 2)
        s['onlineCap_GW'] = round(s['onlineCap_GW'], 2)

    online_total_gw = round(sum(s['onlineCap_GW'] for s in by_site), 2)
    online_count = sum(s['online'] for s in by_site)
    total_count = sum(s['totalCount'] for s in by_site)
    # Latest underlying KHNP timestamp — what the user sees as "data as of"
    latest_ts = max(timestamps) if timestamps else None

    return {
        'asOf':         latest_ts,  # KHNP's own timestamp, NOT our scrape time
        'scrapedAt':    datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'source':       'KHNP Open NPP Operations Info',
        'sourceUrl':    'https://npp.khnp.co.kr/',
        'reactors':     reactors,
        'bySite':       by_site,
        'totalOnlineGW':online_total_gw,
        'onlineCount':  online_count,
        'totalCount':   total_count,
    }


def fetch_khnp_annual_util():
    """Annual fleet utilization + capacity factor from KHNP.

    Source: https://npp.khnp.co.kr/ON004004002001003 — '이용률·가동률' page.
    Single server-rendered table with a year-by-year history (2007 onwards).
    Returns: { asOf, byYear: [{year, util, capFactor}, ...], source }
    """
    url = 'https://npp.khnp.co.kr/ON004004002001003'
    html = fetch(url)
    tables = re.findall(r'<table[^>]*>.*?</table>', html, re.DOTALL)
    if not tables:
        return None
    tbl = tables[0]
    # Header: 구분 + year columns
    th_all = [re.sub(r'<[^>]+>', '', t).strip()
              for t in re.findall(r'<th[^>]*>(.*?)</th>', tbl, re.DOTALL)]
    years = [int(t) for t in th_all if t.isdigit() and len(t) == 4]
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbl, re.DOTALL)
    util_row, cap_row = None, None
    for r in rows:
        cells = [re.sub(r'<[^>]+>', '', c).strip()
                 for c in re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', r, re.DOTALL)]
        if not cells:
            continue
        if '이용률' in cells[0]:
            util_row = cells[1:]
        elif '가동률' in cells[0]:
            cap_row = cells[1:]
    by_year = []
    for i, y in enumerate(years):
        try:
            util = float(util_row[i]) if util_row and i < len(util_row) else None
        except ValueError:
            util = None
        try:
            cap = float(cap_row[i]) if cap_row and i < len(cap_row) else None
        except ValueError:
            cap = None
        if util is not None or cap is not None:
            by_year.append({'year': y, 'util': util, 'capFactor': cap})
    return {
        'asOf':      datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'source':    'KHNP 이용률·가동률',
        'sourceUrl': url,
        'byYear':    by_year,
    }


def fetch_khnp_trips():
    """Annual unplanned-trip counts from KHNP '불시정지' page.

    Source: https://npp.khnp.co.kr/ON004004002001004
    Three stacked tables (2001-09, 2010-19, 2020-29). Each row: year |
    operating reactors | trip count | trips per reactor.
    Returns: { asOf, byYear: [{year, reactors, trips, tripsPerReactor}, ...] }
    """
    url = 'https://npp.khnp.co.kr/ON004004002001004'
    html = fetch(url)
    tables = re.findall(r'<table[^>]*>.*?</table>', html, re.DOTALL)
    by_year = []
    for tbl in tables[:3]:
        th_all = [re.sub(r'<[^>]+>', '', t).strip()
                  for t in re.findall(r'<th[^>]*>(.*?)</th>', tbl, re.DOTALL)]
        years = [int(t) for t in th_all if t.isdigit() and len(t) == 4]
        if not years:
            continue
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbl, re.DOTALL)
        reactors_row = trips_row = per_row = None
        for r in rows:
            cells = [re.sub(r'<[^>]+>', '', c).strip()
                     for c in re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', r, re.DOTALL)]
            if not cells:
                continue
            head = cells[0]
            data = cells[1:]
            # Order matters: '기당 불시 정지 건수' contains '불시 정지 건수' as a
            # substring, so check the per-reactor row FIRST.
            if '운전기수' in head:
                reactors_row = data
            elif '기당' in head:
                per_row = data
            elif '불시 정지 건수' in head:
                trips_row = data
        for i, y in enumerate(years):
            def _i(arr):
                if not arr or i >= len(arr) or arr[i] in ('', '-'):
                    return None
                try: return int(arr[i])
                except ValueError:
                    try: return float(arr[i])
                    except ValueError: return None
            entry = {
                'year':            y,
                'reactors':        _i(reactors_row),
                'trips':           _i(trips_row),
                'tripsPerReactor': _i(per_row),
            }
            if entry['reactors'] is not None or entry['trips'] is not None:
                by_year.append(entry)
    return {
        'asOf':      datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'source':    'KHNP 불시정지',
        'sourceUrl': url,
        'byYear':    by_year,
    }


def fetch_ecb_fx():
    """ECB daily reference rates (cross-rate to derive KRW/USD)."""
    url = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml'
    xml_text = fetch(url)
    root = ET.fromstring(xml_text)
    ns = {'ec': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'}
    cube = root.find('.//ec:Cube[@time]', ns)
    if cube is None:
        raise RuntimeError('ECB feed missing time-stamped Cube')
    as_of = cube.get('time')
    rates = {c.get('currency'): float(c.get('rate'))
             for c in cube.findall('ec:Cube', ns)}
    eur_usd = rates.get('USD')
    eur_krw = rates.get('KRW')
    krw_usd = (eur_krw / eur_usd) if (eur_usd and eur_krw) else None
    return {
        'asOf':      as_of,
        'source':    'ECB euro reference rates',
        'sourceUrl': url,
        'eurUsd':    eur_usd,
        'eurKrw':    eur_krw,
        'krwUsd':    round(krw_usd, 2) if krw_usd else None,
    }


def round_for_diff_stability(out):
    """Round noisy numeric fields so hourly scrapes don't churn git when
    nothing material has changed. Reactor output drifting by 0.01% or 1 MWe
    isn't a meaningful event."""
    k = out.get('khnp') or {}
    for r in (k.get('reactors') or []):
        if r.get('output_pct') is not None:
            r['output_pct'] = round(r['output_pct'], 1)
        if r.get('output_mwe') is not None:
            r['output_mwe'] = round(r['output_mwe'])  # nearest integer MWe
    for s in (k.get('bySite') or []):
        s['totalCap_GW']  = round(s.get('totalCap_GW', 0), 2)
        s['onlineCap_GW'] = round(s.get('onlineCap_GW', 0), 2)
    if k.get('totalOnlineGW') is not None:
        k['totalOnlineGW'] = round(k['totalOnlineGW'], 2)


def append_history_row(out):
    """Append today's nuclear KPI to data/korea_nuclear_history.csv so we
    accumulate a current-year monthly utilization line over time. One row
    per scrape — duplicates per day are de-duped on read by the front-end."""
    k = out.get('khnp') or {}
    if not k or k.get('totalOnlineGW') is None:
        return
    history_path = os.path.join(os.path.dirname(OUT_PATH), 'korea_nuclear_history.csv')
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    # Compute aggregate util as online_GW / nominal_total (sum of all reactors'
    # nameplate). For a "true" utilization we'd need nameplate, not running output.
    # Approximation: sum of online_mwe + nameplate_for_offline. For now use
    # online_GW / nominal 25.0 GW (current Korean fleet nameplate) as the rough denominator.
    NOMINAL_FLEET_GW = 25.0
    util_pct = round(100 * k['totalOnlineGW'] / NOMINAL_FLEET_GW, 1)
    new_row = f"{today},{k['totalOnlineGW']},{NOMINAL_FLEET_GW},{util_pct},{k['onlineCount']},{k['totalCount']}\n"

    header = "date,online_GW,nominal_total_GW,util_pct,online_count,total_count\n"
    if not os.path.exists(history_path):
        with open(history_path, 'w', encoding='utf-8') as f:
            f.write(header)
            f.write(new_row)
        return
    # Skip if today's row already exists (overwrite with latest snapshot of the day)
    with open(history_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    today_idx = next((i for i, L in enumerate(lines) if L.startswith(today + ',')), None)
    if today_idx is not None:
        lines[today_idx] = new_row
    else:
        lines.append(new_row)
    with open(history_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)


def main():
    out = {
        'updated':       datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'fx':            None,
        'khnp':          None,
        'khnpAnnual':    None,
        'khnpTrips':     None,
        # KR-only sources — left null so the front-end falls back to seeded values + amber badge.
        'kogasTariff':   None,
        'kogasImports':  None,
        # Gated on user OpenDART API key.
        'dart':          None,
        'errors':        [],
    }

    try:
        out['fx'] = fetch_ecb_fx()
        print(f"FX: {out['fx']['krwUsd']} KRW/USD as of {out['fx']['asOf']}", file=sys.stderr)
    except Exception as e:
        out['errors'].append(f'ECB FX: {e}')
        print(f"ECB FX failed: {e}", file=sys.stderr)

    try:
        out['khnp'] = fetch_khnp_live()
        k = out['khnp']
        print(f"KHNP live: {k['onlineCount']}/{k['totalCount']} online · "
              f"{k['totalOnlineGW']} GW · {len(k['bySite'])} sites · "
              f"data as of {k.get('asOf')}", file=sys.stderr)
    except Exception as e:
        out['errors'].append(f'KHNP live: {e}')
        print(f"KHNP live failed: {e}", file=sys.stderr)

    try:
        out['khnpAnnual'] = fetch_khnp_annual_util()
        a = out['khnpAnnual']
        if a and a.get('byYear'):
            print(f"KHNP annual util: {len(a['byYear'])} years "
                  f"({a['byYear'][0]['year']}–{a['byYear'][-1]['year']})", file=sys.stderr)
    except Exception as e:
        out['errors'].append(f'KHNP annual util: {e}')
        print(f"KHNP annual util failed: {e}", file=sys.stderr)

    try:
        out['khnpTrips'] = fetch_khnp_trips()
        t = out['khnpTrips']
        if t and t.get('byYear'):
            print(f"KHNP unplanned trips: {len(t['byYear'])} years", file=sys.stderr)
    except Exception as e:
        out['errors'].append(f'KHNP trips: {e}')
        print(f"KHNP trips failed: {e}", file=sys.stderr)

    # Stabilize for git diff
    round_for_diff_stability(out)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_PATH}", file=sys.stderr)

    # History accumulator: append/update today's row in the CSV. The
    # workflow may pass --skip-history if it doesn't want this for some runs.
    if '--skip-history' not in sys.argv:
        try:
            append_history_row(out)
            print(f"Appended today's row to korea_nuclear_history.csv", file=sys.stderr)
        except Exception as e:
            out['errors'].append(f'history append: {e}')

    return 0 if not out['errors'] else 1


if __name__ == '__main__':
    sys.exit(main())
