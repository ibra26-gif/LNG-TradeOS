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
import calendar
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta


# ────────────────────────────────────────────────────────────────────
# Korean nuclear fleet nameplate capacity timeline (IAEA PRIS-anchored).
# Used to convert Ember monthly TWh → monthly utilization (capacity factor).
#
# Events (commercial-operation / permanent-shutdown dates):
#   2019-08-29  Shin Kori 4   commercial (+1.416 GW)
#   2019-12-24  Wolsong 1     retired    (-0.679 GW)
#   2022-12-07  Shin Hanul 1  commercial (+1.418 GW)
#   2023-04-08  Kori 2        retired    (-0.650 GW)
#   2024-04-05  Shin Hanul 2  commercial (+1.418 GW)
#   2025-03-28  Shin Kori 5   commercial (+1.416 GW)
# ────────────────────────────────────────────────────────────────────
def korea_nuc_capacity_gw(date_str):
    """Operating Korean nuclear nameplate (GW) for a YYYY-MM-DD month."""
    ym = int(date_str[:4]) * 100 + int(date_str[5:7])
    if ym < 201909: return 22.67   # pre-Shin-Kori-4
    if ym < 201912: return 24.08   # SK4 in, Wolsong 1 still
    if ym < 202212: return 23.40   # Wolsong 1 retired
    if ym < 202304: return 24.82   # Shin Hanul 1 added
    if ym < 202404: return 24.17   # Kori 2 retired
    if ym < 202503: return 25.59   # Shin Hanul 2 added
    return 27.00                    # Shin Kori 5 added

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


def fetch_ember_monthly():
    """Monthly Korean electricity by source from Ember.

    Source: files.ember-energy.org public CSV (the same dataset behind
    their Monthly Electricity Data explorer). Free, no API key needed.
    The file is large (~70 MB) so we cache the parsed Korea-nuclear rows
    in data/korea_ember_monthly.json and only re-download weekly.
    """
    import csv
    import io as _io

    cache_path = os.path.join(os.path.dirname(OUT_PATH), 'korea_ember_monthly.json')

    def _enrich(out):
        """Add utilizationPct + capacityGw to every series row.
        Idempotent — recomputes on every load so capacity-table edits flow through.
        """
        for r in (out.get('series') or []):
            d = r.get('date'); twh = r.get('twh')
            if not d or twh is None: continue
            cap_gw = korea_nuc_capacity_gw(d)
            y, m = int(d[:4]), int(d[5:7])
            hours = calendar.monthrange(y, m)[1] * 24
            max_twh = cap_gw * hours / 1000.0  # GW × h = GWh; /1000 → TWh
            if max_twh > 0:
                r['utilizationPct'] = round(twh / max_twh * 100, 1)
                r['capacityGw'] = cap_gw
        return out

    # Refresh if cache missing or > 7 days old
    refresh = True
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            cached_at = cached.get('cachedAt')
            if cached_at:
                from datetime import datetime as _dt
                age_days = (datetime.now(timezone.utc) - _dt.fromisoformat(cached_at)).days
                if age_days < 7:
                    return _enrich(cached)
        except Exception:
            pass

    if refresh:
        url = 'https://files.ember-energy.org/public-downloads/monthly_full_release_long_format.csv'
        try:
            text = fetch(url, timeout=60)
        except Exception as e:
            print(f'Ember monthly fetch failed: {e}', file=sys.stderr)
            # Return cached even if stale
            if os.path.exists(cache_path):
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
        rows = []
        reader = csv.DictReader(_io.StringIO(text))
        for r in reader:
            if r.get('Area') != 'South Korea': continue
            if r.get('Variable') != 'Nuclear': continue
            if r.get('Subcategory') != 'Fuel': continue
            unit = r.get('Unit')
            if unit not in ('TWh', '%'): continue
            try:
                val = float(r['Value']) if r.get('Value') else None
            except ValueError:
                val = None
            if val is None: continue
            rows.append({'date': r['Date'], 'unit': unit, 'value': val})

        # Pivot: date → {twh, share_pct}
        by_date = {}
        for r in rows:
            d = by_date.setdefault(r['date'], {})
            if r['unit'] == 'TWh': d['twh'] = r['value']
            elif r['unit'] == '%':  d['sharePct'] = r['value']
        series = sorted([{
            'date':     k,
            'twh':      v.get('twh'),
            'sharePct': v.get('sharePct'),
        } for k, v in by_date.items()], key=lambda x: x['date'])

        out = {
            'cachedAt': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'source':   'Ember Monthly Electricity Data',
            'sourceUrl':url,
            'series':   series,
        }
        _enrich(out)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(out, f, indent=2)
        return out


# ────────────────────────────────────────────────────────────────────
# DART (OpenDART) — KOGAS quarterly filings + sectoral sales volumes
#
# Free API key required (env var OPENDART_KEY). Without it this fetcher
# returns None gracefully — existing data flow unaffected.
#
# KOGAS corp metadata (verified via corpCode.xml dump):
#   corp_code  = 00261285
#   corp_name  = 한국가스공사 (Korea Gas Corporation)
#   stock_code = 036460
#
# What we extract from the latest quarterly/half-year/annual report:
#   1. Annual sales volume by sector (도시가스용 / 발전용 / 합계) — historical
#      time series from 1987 onwards, in thousand tons.
#   2. LNG procurement origins (e.g., Qatar, Australia, US).
#   3. Forward-contracted volumes by tariff type (3 most recent years).
#   4. Latest period summary (revenue, op profit) from fnlttSinglAcntAll.
# ────────────────────────────────────────────────────────────────────
DART_KOGAS_CORP_CODE = '00261285'
DART_API_BASE = 'https://opendart.fss.or.kr/api'


def _dart_get(url, timeout=30):
    """GET a DART JSON endpoint and parse — raises on non-200."""
    text = fetch(url, timeout=timeout)
    return json.loads(text)


def _dart_get_zip(url, timeout=600):
    """Download a DART ZIP archive (corpCode.xml or document.xml) and return raw bytes.
    DART throttles uploads to ~13 KB/s — a 10MB document.xml can take 10+ min.
    Falls back to curl with retries if Python streaming truncates."""
    headers = {'User-Agent': UA}
    if _USE_REQUESTS:
        try:
            r = _requests.get(url, timeout=timeout, stream=True, headers=headers)
            r.raise_for_status()
            # Read in chunks so the connection stays alive on slow streams.
            chunks = []
            for chunk in r.iter_content(chunk_size=64*1024):
                if chunk:
                    chunks.append(chunk)
            content = b''.join(chunks)
            cl = r.headers.get('Content-Length')
            if not cl or int(cl) == len(content):
                return content
            print(f'DART zip: requests got {len(content)}/{cl} bytes — retrying via curl',
                  file=sys.stderr)
        except Exception as e:
            print(f'DART zip via requests failed: {e} — retrying via curl', file=sys.stderr)

    # Curl fallback with retries — DART occasionally drops the connection.
    import subprocess, tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tf:
        tmp_path = tf.name
    try:
        subprocess.run(
            ['curl', '--http1.1', '-fsS', '-A', UA,
             '--max-time', str(timeout),
             '--connect-timeout', '30',
             '--retry', '3', '--retry-delay', '5',
             '--speed-time', '120', '--speed-limit', '300',
             '-o', tmp_path, url],
            check=True, timeout=timeout + 60,
        )
        with open(tmp_path, 'rb') as f:
            return f.read()
    finally:
        try: os.unlink(tmp_path)
        except: pass


def _dart_parse_period_report_xml(xml_text):
    """Parse a KOGAS period-report main XML. Returns dict with sectoral data.

    The report is a custom KICS XBRL-like document. We extract data from
    the embedded <TABLE> elements using regex (BeautifulSoup not available
    in the runtime). Three target tables:

      A) Annual sales volume by sector (multi-year, in kilo-tons):
         signature: contains '도시가스용' AND '발전용' AND '평균증가율'.
      B) Forward contracted volumes by tariff type (3 yrs × 3 cats):
         signature: '발전용 평균요금제' AND '도시가스용'.
      C) LNG procurement origins (single row — Qatar, Australia, US, ...):
         signature: 'LNG' AND ('카타르' OR '호주') AND short table.
    """
    import re
    out = {}

    tables = re.findall(r'<TABLE[\s\S]*?</TABLE>', xml_text)

    def _cells(table):
        """Strip XML tags from a table, collapse whitespace, split on '|'."""
        clean = re.sub(r'<[^>]+>', '|', table)
        clean = re.sub(r'\|+', '|', clean)
        clean = re.sub(r'\s+', ' ', clean).strip('| ').strip()
        return [c.strip() for c in clean.split('|')]

    def _to_int(s):
        s = re.sub(r'\([^)]*\)', '', s).replace(',', '').strip()
        try:    return int(float(s))
        except: return None

    SECTOR_SEPARATORS = {'도시가스용','발전용','발전용 평균요금제','발전용 개별요금제',
                          '합 계','합계','구 분','구분','용 도','용도'}

    def _row_after(cells, sector_kr, max_take=None):
        """Return ints from cells after sector_kr until next sector header.
        Strips parenthetical growth rates and decimals (CAGR), keeps only
        non-negative integer-looking numbers."""
        try:
            idx = cells.index(sector_kr)
        except ValueError:
            return None
        row_text_parts = []
        for c in cells[idx+1:]:
            if c in SECTOR_SEPARATORS:
                break
            row_text_parts.append(c)
        row_text = ' '.join(row_text_parts)
        # 1) Drop everything inside parens (growth rates like (-6.9), (2.7))
        row_text = re.sub(r'\([^)]*\)', ' ', row_text)
        # 2) Drop standalone decimal numbers like CAGR "15.7%" or "8.4%"
        row_text = re.sub(r'\b\d+\.\d+\s*%?', ' ', row_text)
        # 3) Now extract integer tokens (with optional thousands separator)
        ints = []
        for m in re.finditer(r'-?\d{1,3}(?:,\d{3})+|\d+', row_text):
            n = _to_int(m.group(0))
            if n is None or n < 0 or n > 1_000_000:
                continue
            ints.append(n)
            if max_take and len(ints) >= max_take:
                break
        return ints

    # ── Table A: annual sectoral sales volume ──────────────────────────
    for t in tables:
        if not ('도시가스용' in t and '발전용' in t and '평균증가율' in t):
            continue
        if len(t) > 12000:  # skip huge boilerplate tables
            continue
        cells = _cells(t)
        # Years are 4-digit ints between 1987 and current+1.
        years = []
        for c in cells:
            m = re.fullmatch(r'(19\d\d|20\d\d)', c)
            if m:
                y = int(m.group(1))
                if 1987 <= y <= datetime.now().year + 1 and y not in years:
                    years.append(y)
        if not years:
            continue

        rows_data = {}
        for sector_kr, key in [('도시가스용','cityGas_kt'),
                                ('발전용','power_kt'),
                                ('합 계','total_kt'),
                                ('합계','total_kt')]:
            ints = _row_after(cells, sector_kr, max_take=len(years) + 1)
            if ints and len(ints) >= len(years):
                rows_data[key] = ints[:len(years)]

        if rows_data:
            entries = []
            for i, y in enumerate(years):
                e = {'year': y}
                for k, vals in rows_data.items():
                    e[k] = vals[i]
                entries.append(e)
            out['sectorVolumesByYear'] = entries
            break

    # ── Table B: forward-contracted volumes by tariff type ─────────────
    for t in tables:
        if not ('발전용 평균요금제' in t and '도시가스용' in t):
            continue
        if len(t) > 6000:
            continue
        cells = _cells(t)
        # Years like '25년, '24년, '23년 → ints 2025/2024/2023
        years = []
        for c in cells:
            m = re.search(r"'(\d{2})년", c)
            if m:
                y = 2000 + int(m.group(1))
                if y not in years:
                    years.append(y)
        if not years or len(years) < 2:
            continue
        # Re-use the row helper (allow decimals here — values may be 962.5)
        def _row_floats(cells, sector_kr, n_years):
            try:
                idx = cells.index(sector_kr)
            except ValueError:
                return None
            text_parts = []
            for c in cells[idx+1:]:
                if c in SECTOR_SEPARATORS:
                    break
                text_parts.append(c)
            text = ' '.join(text_parts)
            text = re.sub(r'\([^)]*\)', ' ', text)
            nums = re.findall(r'-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?', text)
            vals = []
            for n in nums:
                try:    vals.append(float(n.replace(',', '')))
                except: pass
                if len(vals) >= n_years:
                    break
            return vals if len(vals) >= n_years else None

        rows_data = {}
        for sector_kr, key in [
            ('발전용 평균요금제', 'power_avg_tariff_kt'),
            ('발전용 개별요금제', 'power_individual_tariff_kt'),
            ('도시가스용',         'cityGas_kt'),
            ('합 계',              'total_kt'),
            ('합계',                'total_kt'),
        ]:
            vals = _row_floats(cells, sector_kr, len(years))
            if vals:
                rows_data[key] = vals[:len(years)]
        if rows_data:
            out['contractedVolumes'] = [
                {'year': y, **{k: vals[i] for k, vals in rows_data.items()}}
                for i, y in enumerate(years)
            ]
            break

    # ── Table C: LNG procurement origins ───────────────────────────────
    for t in tables:
        if 'LNG' in t and ('카타르' in t or '호주' in t) and len(t) < 3500:
            cells = _cells(t)
            for c in cells:
                if '카타르' in c or '호주' in c:
                    raw = re.sub(r'\s*등\s*$', '', c)
                    parts = re.split(r'[,、·\s]+', raw)
                    origins_kr = [p for p in parts if p and len(p) <= 6]
                    if origins_kr:
                        # Korean → English mapping for display
                        kr2en = {
                            '카타르':'Qatar','호주':'Australia','미국':'United States',
                            '오만':'Oman','말레이시아':'Malaysia','인도네시아':'Indonesia',
                            '러시아':'Russia','나이지리아':'Nigeria','브루나이':'Brunei',
                            '예멘':'Yemen','앙골라':'Angola','이집트':'Egypt',
                            '트리니다드':'Trinidad','UAE':'UAE','파푸아뉴기니':'Papua New Guinea',
                        }
                        origins_en = [kr2en.get(k, k) for k in origins_kr]
                        out['lngOrigins'] = origins_en
                        out['lngOriginsKr'] = origins_kr
                        break
            if 'lngOrigins' in out:
                break

    return out


def fetch_dart_kogas():
    """Pull KOGAS sectoral data from OpenDART API.

    Requires environment variable OPENDART_KEY. Returns None if missing.
    Caches the parsed report in data/dart_kogas.json — only re-parses
    when a new period report (분기/반기/사업보고서) shows up in the listing.
    """
    api_key = os.environ.get('OPENDART_KEY')
    if not api_key:
        print('OPENDART_KEY not set — skipping DART fetch', file=sys.stderr)
        return None

    cache_path = os.path.join(os.path.dirname(OUT_PATH), 'dart_kogas.json')
    today = datetime.now(timezone.utc)

    # 1. List recent KOGAS filings (last 2 years).
    bgn = (today - timedelta(days=730)).strftime('%Y%m%d')
    end = today.strftime('%Y%m%d')
    list_url = (f'{DART_API_BASE}/list.json'
                f'?crtfc_key={api_key}&corp_code={DART_KOGAS_CORP_CODE}'
                f'&bgn_de={bgn}&end_de={end}&page_count=100')
    try:
        list_data = _dart_get(list_url, timeout=30)
    except Exception as e:
        print(f'DART list fetch failed: {e}', file=sys.stderr)
        return None

    if list_data.get('status') != '000':
        print(f"DART list error: {list_data.get('message')}", file=sys.stderr)
        return None
    filings = list_data.get('list') or []

    # 2. Find latest period report (분기/반기/사업보고서 — full-form business report).
    latest_period = None
    for f in filings:
        nm = f.get('report_nm', '') or ''
        if '분기보고서' in nm or '반기보고서' in nm or '사업보고서' in nm:
            latest_period = f
            break

    # 3. Cache check — skip parse if same rcept_no.
    cached = None
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached = json.load(f)
        except Exception:
            cached = None

    if (cached and latest_period
            and cached.get('latestRceptNo') == latest_period.get('rcept_no')):
        cached['filings'] = [
            {'rcept_no': f['rcept_no'], 'rcept_dt': f['rcept_dt'],
             'report_nm': (f['report_nm'] or '').strip()}
            for f in filings[:30]
        ]
        cached['asOf'] = today.isoformat(timespec='seconds')
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cached, f, indent=2, ensure_ascii=False)
        return cached

    # 4. Download + parse latest period report main XML.
    sector_data = {}
    if latest_period:
        rcept_no = latest_period['rcept_no']
        doc_url = f'{DART_API_BASE}/document.xml?crtfc_key={api_key}&rcept_no={rcept_no}'
        try:
            buf = _dart_get_zip(doc_url, timeout=300)
            import zipfile, io as _io
            with zipfile.ZipFile(_io.BytesIO(buf)) as z:
                with z.open(z.namelist()[0]) as f:
                    xml_text = f.read().decode('utf-8', errors='replace')
            sector_data = _dart_parse_period_report_xml(xml_text)
            print(f"DART parsed report {rcept_no}: "
                  f"annual={len(sector_data.get('sectorVolumesByYear') or [])}y, "
                  f"contracted={len(sector_data.get('contractedVolumes') or [])}y, "
                  f"origins={sector_data.get('lngOrigins') or []}",
                  file=sys.stderr)
        except Exception as e:
            print(f'DART document.xml parse failed: {e}', file=sys.stderr)

    out = {
        'asOf':              today.isoformat(timespec='seconds'),
        'corpCode':          DART_KOGAS_CORP_CODE,
        'corpName':          '한국가스공사',
        'corpNameEn':        'Korea Gas Corporation',
        'stockCode':         '036460',
        'latestRceptNo':     latest_period['rcept_no'] if latest_period else None,
        'latestReportName':  (latest_period['report_nm'].strip() if latest_period else None),
        'latestReportDate':  latest_period['rcept_dt'] if latest_period else None,
        'filings': [
            {'rcept_no': f['rcept_no'], 'rcept_dt': f['rcept_dt'],
             'report_nm': (f['report_nm'] or '').strip()}
            for f in filings[:30]
        ],
        'sectorVolumesByYear': sector_data.get('sectorVolumesByYear'),
        'contractedVolumes':   sector_data.get('contractedVolumes'),
        'lngOrigins':          sector_data.get('lngOrigins'),
        'lngOriginsKr':        sector_data.get('lngOriginsKr'),
        'source':              'OpenDART (Financial Supervisory Service)',
        'sourceUrl':           'https://opendart.fss.or.kr',
    }

    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    return out


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
        'emberMonthly':  None,  # NEW: monthly nuclear gen 2019+ for seasonal chart
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

    try:
        out['emberMonthly'] = fetch_ember_monthly()
        em = out['emberMonthly']
        if em and em.get('series'):
            print(f"Ember monthly: {len(em['series'])} months "
                  f"({em['series'][0]['date'][:7]}–{em['series'][-1]['date'][:7]})", file=sys.stderr)
    except Exception as e:
        out['errors'].append(f'Ember monthly: {e}')
        print(f"Ember monthly failed: {e}", file=sys.stderr)

    try:
        out['dart'] = fetch_dart_kogas()
        d = out['dart']
        if d:
            yrs = len(d.get('sectorVolumesByYear') or [])
            origins = d.get('lngOrigins') or []
            print(f"DART KOGAS: {d.get('latestReportName')} ({d.get('latestReportDate')}), "
                  f"{yrs} yrs sectoral volume, origins={origins}", file=sys.stderr)
    except Exception as e:
        out['errors'].append(f'DART KOGAS: {e}')
        print(f"DART KOGAS failed: {e}", file=sys.stderr)

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
