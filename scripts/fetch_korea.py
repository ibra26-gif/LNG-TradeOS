#!/usr/bin/env python3
"""
Korea daily data scraper for LNG TradeOS.

Sources (all globally accessible, no API key, no Korean-IP requirement):
  - IAEA PRIS — Korean reactor status & capacity
  - ECB euro reference rates — KRW/USD via cross-rate

Geo-blocked sources (KOGAS tariff Excel, KEPCO power data, KHNP direct site)
are NOT scraped from this script — those would require a Korean proxy. The
output JSON has placeholder slots for them (kogasTariff, kogasImports, dart)
so the front-end renders a "manual entry · awaiting source" badge until
those feeds come online.

Output: data/korea.json
Cadence: daily 18:00 CET (after ECB publishes; ECB updates ~16:00 CET)
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


def fetch_pris():
    """Scrape IAEA PRIS country page for Korean reactor status."""
    url = 'https://pris.iaea.org/PRIS/CountryStatistics/CountryDetails.aspx?current=KR'
    html = fetch(url)
    # Each reactor row: <a>NAME</a></td><td>TYPE</td><td>STATUS</td>
    # <td>LOCATION</td><td>NET_MW</td><td>GROSS_MW</td>
    row_re = re.compile(
        r'>([A-Z][A-Z0-9\-]+)</a>\s*</td>\s*'
        r'<td[^>]*>\s*([A-Z]+)\s*</td>\s*'
        r'<td[^>]*>\s*([^<]+?)\s*</td>\s*'
        r'<td[^>]*>\s*([^<]+?)\s*</td>\s*'
        r'<td[^>]*>\s*(\d+)\s*</td>\s*'
        r'<td[^>]*>\s*(\d+)\s*</td>',
        re.DOTALL,
    )
    reactors = []
    for name, rtype, status, location, net_mw, gross_mw in row_re.findall(html):
        # Site = strip trailing "-N"
        m = re.match(r'(.+?)-(\d+)$', name)
        if not m:
            continue
        prefix, unit = m.groups()
        site = SITE_MAP.get(prefix, prefix.title())
        reactors.append({
            'name':         name,
            'site':         site,
            'unit':         int(unit),
            'type':         rtype,
            'status':       status.strip(),
            'location':     location.strip().replace('&amp;', '&'),
            'net_mw':       int(net_mw),
            'gross_mw':     int(gross_mw),
        })

    # Aggregate by site
    sites_dict = {}
    for r in reactors:
        s = sites_dict.setdefault(r['site'], {
            'site': r['site'], 'reactors': [], 'totalCount': 0, 'online': 0,
            'totalCap_GW': 0.0, 'onlineCap_GW': 0.0,
        })
        s['reactors'].append(r['name'])
        s['totalCount'] += 1
        if r['status'] == 'Operational':
            s['online'] += 1
            s['onlineCap_GW'] += r['gross_mw'] / 1000.0
        s['totalCap_GW'] += r['gross_mw'] / 1000.0
    by_site = sorted(sites_dict.values(), key=lambda x: -x['onlineCap_GW'])
    for s in by_site:
        s['totalCap_GW'] = round(s['totalCap_GW'], 2)
        s['onlineCap_GW'] = round(s['onlineCap_GW'], 2)

    online_total_gw = round(sum(s['onlineCap_GW'] for s in by_site), 2)
    online_count = sum(s['online'] for s in by_site)
    total_count = sum(s['totalCount'] for s in by_site)

    return {
        'asOf':         datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'source':       'IAEA PRIS',
        'sourceUrl':    url,
        'reactors':     reactors,
        'bySite':       by_site,
        'totalOnlineGW':online_total_gw,
        'onlineCount':  online_count,
        'totalCount':   total_count,
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


def main():
    out = {
        'updated':       datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'fx':            None,
        'khnp':          None,
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
        out['khnp'] = fetch_pris()
        k = out['khnp']
        print(f"PRIS: {k['onlineCount']}/{k['totalCount']} online · "
              f"{k['totalOnlineGW']} GW · {len(k['bySite'])} sites", file=sys.stderr)
    except Exception as e:
        out['errors'].append(f'IAEA PRIS: {e}')
        print(f"PRIS failed: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_PATH}", file=sys.stderr)
    return 0 if not out['errors'] else 1


if __name__ == '__main__':
    sys.exit(main())
