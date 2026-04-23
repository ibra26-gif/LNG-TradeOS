#!/usr/bin/env python3
"""
fetch_entsog.py — ENTSOG scraper (Phase 2)
Runs via GitHub Action. Writes TWO files:
  - /data/pipeline_daily.json    (Physical Flow, historical + recent)
  - /data/nominations_daily.json (Nomination, forward-looking, D-1 eve publication)

Both use same corridor point labels. Nominations published D-1 evening for D.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import requests

ENTSOG_BASE = "https://transparency.entsog.eu/api/v1/operationalDatas"
KWH_TO_MCM = 1 / (1e6 * 10.55)

PIPE_POINTS = {
    "norway": [
        "Zeebrugge ZPT", "Dornum GASPOOL", "Emden (EPT1) (OGE)",
        "Easington", "St. Fergus", "Nybro", "Dunkerque",
    ],
    "algeria_italy": ["Mazara del Vallo"],
    "libya": ["Gela"],
    "algeria_spain": ["Almería", "Tarifa"],
    "tap": ["Melendugno - IT / TAP"],  # Kipi removed - was double-counting (inside GR)
    "russia": [
        "Strandzha 2 (BG) / Malkoclar (TR)", "Budince",
        "Uzhhorod (UA) - Velke Kapusany (SK)",
        "Isaccea (RO) - Orlovka (UA) II",
    ],
    "uk_interconn": ["Zeebrugge IZT", "Bacton (BBL)"],
    # Iberian VIPs (verified labels via ENTSOG API probe 2026-04-23):
    "vip_pirineos": ["VIP PIRINEOS"],   # Spain <-> France
    "vip_iberico":  ["VIP IBERICO"],    # Spain <-> Portugal
}

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

TIMEOUT = 120
DELAY_S = 3.0
MAX_RETRIES = 3
RETRY_BACKOFF = 10.0

SKIP_CODES = {404}
RETRY_CODES = {429, 500, 502, 503, 504}


def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_point_month(session, label, indicator, year, month):
    """Fetch one point, one indicator, one month."""
    last_day = 28
    if month in (1,3,5,7,8,10,12): last_day = 31
    elif month in (4,6,9,11): last_day = 30
    elif month == 2:
        last_day = 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28

    from_date = f"{year}-{month:02d}-01"
    to_date = f"{year}-{month:02d}-{last_day}"

    # For nominations, allow up to TOMORROW (D-1 evening publishes for D+1)
    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)
    if datetime(year, month, 1).date() > tomorrow:
        return []

    params = {
        "indicator": indicator,
        "directionKey": "entry",
        "pointLabel": label,
        "from": from_date,
        "to": to_date,
        "periodType": "day",
        "timeZone": "WET",
        "limit": "500",
    }

    for attempt in range(1 + MAX_RETRIES):
        try:
            r = session.get(ENTSOG_BASE, params=params, timeout=TIMEOUT)
            if r.status_code in SKIP_CODES:
                return []
            if r.status_code in RETRY_CODES:
                if attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF * (attempt + 1)
                    print(f"  {r.status_code}\u2192retry{attempt+1}", end="", flush=True)
                    time.sleep(wait)
                    continue
                return []
            r.raise_for_status()
            data = r.json()
            return (
                data.get("operationaldatas")
                or data.get("operationalDatas")
                or data.get("operationalData")
                or data.get("data")
                or []
            )
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (attempt + 1)
                print(f"  timeout\u2192retry{attempt+1}", end="", flush=True)
                time.sleep(wait)
                continue
            return []
        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (attempt + 1)
                print(f"  {type(e).__name__}\u2192retry{attempt+1}", end="", flush=True)
                time.sleep(wait)
                continue
            return []
    return []


def scrape_indicator(session, indicator, year, months, out_name):
    """Scrape one indicator across all routes and months."""
    print(f"\n=== {indicator} ===")
    result = {}
    total_points = 0

    for route, labels in PIPE_POINTS.items():
        print(f"\n-- {route} ({len(labels)} pts x {len(months)} mo) --")
        route_daily = {}

        for label in labels:
            label_days = 0
            print(f"  {label}: ", end="", flush=True)

            for mo in months:
                rows = fetch_point_month(session, label, indicator, year, mo)
                if rows:
                    # Filter rows:
                    #   1. Match the requested indicator exactly. ENTSOG API sometimes
                    #      returns mixed indicator rows when `pointLabel` has multiple
                    #      indicator types (e.g. both Physical Flow and Nomination data).
                    #   2. Reject future dates for Physical Flow (only nominations can
                    #      be published forward). Allow D+1 for Nomination indicator.
                    today = datetime.now(timezone.utc).date()
                    if indicator == "Nomination":
                        max_date = (today + timedelta(days=2)).isoformat()
                    else:  # Physical Flow
                        max_date = today.isoformat()
                    rejected_indicator = 0
                    rejected_future = 0
                    for row in rows:
                        # Filter 1: indicator match (case-insensitive, flexible)
                        row_ind = (row.get("indicator") or "").strip()
                        if row_ind and row_ind.lower() != indicator.lower():
                            rejected_indicator += 1
                            continue
                        # Filter 2: date within expected bounds
                        date = (row.get("periodFrom") or row.get("from") or "")[:10]
                        if len(date) < 10:
                            continue
                        if date > max_date:
                            rejected_future += 1
                            continue
                        val_kwh = float(row.get("value", 0) or 0)
                        mcm = val_kwh * KWH_TO_MCM
                        route_daily[date] = route_daily.get(date, 0) + mcm
                        label_days += 1
                    if rejected_indicator or rejected_future:
                        print(f"M{mo}OK(rej:i={rejected_indicator},f={rejected_future})", end=" ", flush=True)
                    else:
                        print(f"M{mo}OK", end=" ", flush=True)
                else:
                    print(f"M{mo}-", end=" ", flush=True)
                time.sleep(DELAY_S)

            if label_days > 0:
                total_points += 1
                print(f"({label_days}d)")
            else:
                print("(no data)")

        result[route] = dict(sorted(route_daily.items()))
        print(f"  -> {route}: {len(route_daily)} unique days")

    result["_meta"] = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "indicator": indicator,
        "year": year,
        "months": max(months) if months else 0,
        "points_ok": total_points,
        "routes": list(PIPE_POINTS.keys()),
    }

    out_dir = os.environ.get("OUTPUT_DIR", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, out_name)
    with open(out_path, "w") as f:
        json.dump(result, f, separators=(",", ":"))

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\nDONE {out_path} ({size_kb:.1f} KB) - {total_points} points OK")
    return total_points


def main():
    today = datetime.now(timezone.utc).date()
    year = today.year
    current_month = today.month
    months_full = list(range(1, current_month + 1))

    # Physical Flow: full year to date (historical context)
    # Nominations: only last 2 months (forward-looking, smaller volume)
    months_noms = [m for m in months_full if m >= max(1, current_month - 1)]

    print(f"fetch_entsog.py Phase 2 - {year}")
    print(f"  Physical Flow: months 1-{current_month}")
    print(f"  Nominations  : months {months_noms[0]}-{current_month}")

    session = make_session()

    # Phase 1: Physical Flow (historical + recent)
    phys_ok = scrape_indicator(session, "Physical Flow", year, months_full, "pipeline_daily.json")

    # Phase 2: Nominations (forward)
    noms_ok = scrape_indicator(session, "Nomination", year, months_noms, "nominations_daily.json")

    print(f"\n==== SUMMARY ====")
    print(f"  Physical Flow : {phys_ok} points OK")
    print(f"  Nominations   : {noms_ok} points OK")
    print(f"=================")

    if phys_ok == 0:
        sys.exit(1)  # Physical flow is essential
    # Noms can be 0 if it's early morning before D-1 evening publication


if __name__ == "__main__":
    main()
