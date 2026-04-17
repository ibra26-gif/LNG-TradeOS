#!/usr/bin/env python3
"""
fetch_entsog.py — ENTSOG pipeline flow scraper for LNG TradeOS™
Runs via GitHub Action (daily cron). Outputs /data/pipeline_daily.json.

v2: Fixed for GitHub Actions — uses requests library, month-by-month chunking,
    120s timeout, and browser-like headers to handle ENTSOG's slow API.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta

# pip install requests
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
    "tap": ["Kipi (TR) / Kipi (GR)", "Melendugno - IT / TAP"],
    "russia": [
        "Strandzha 2 (BG) / Malkoclar (TR)", "Budince",
        "Uzhhorod (UA) - Velke Kapusany (SK)",
        "Isaccea (RO) - Orlovka (UA) II",
    ],
    "uk_interconn": ["Zeebrugge IZT", "Bacton (BBL)"],
}

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

TIMEOUT = 120          # seconds per request
DELAY_S = 3.0          # seconds between requests
MAX_RETRIES = 3        # retries on transient errors
RETRY_BACKOFF = 10.0   # base seconds before retry

# HTTP codes to skip permanently (point doesn't exist)
SKIP_CODES = {404}
# HTTP codes to retry
RETRY_CODES = {429, 500, 502, 503, 504}


def make_session():
    """Create a requests session with keep-alive and browser headers."""
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_point_month(session, label, year, month):
    """Fetch one point label for one month. Returns list of row dicts."""
    last_day = 28
    if month in (1,3,5,7,8,10,12): last_day = 31
    elif month in (4,6,9,11): last_day = 30
    elif month == 2:
        last_day = 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28

    from_date = f"{year}-{month:02d}-01"
    to_date = f"{year}-{month:02d}-{last_day}"

    # Don't query future months
    today = datetime.now(timezone.utc).date()
    if datetime(year, month, 1).date() > today:
        return []
    if datetime(year, month, last_day).date() > today:
        to_date = today.isoformat()

    params = {
        "indicator": "Physical Flow",
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
                    print(f"  {r.status_code} — retry {attempt+1} in {wait}s", end="", flush=True)
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
                print(f"  timeout — retry {attempt+1} in {wait}s", end="", flush=True)
                time.sleep(wait)
                continue
            return []
        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (attempt + 1)
                print(f"  {type(e).__name__} — retry {attempt+1} in {wait}s", end="", flush=True)
                time.sleep(wait)
                continue
            return []
    return []


def main():
    today = datetime.now(timezone.utc).date()
    year = today.year
    current_month = today.month

    # Which months to fetch: Jan through current month
    months = list(range(1, current_month + 1))

    print(f"fetch_entsog.py v2 — {year}, months 1–{current_month}")
    print(f"Routes: {', '.join(PIPE_POINTS.keys())}")
    print(f"Timeout: {TIMEOUT}s, delay: {DELAY_S}s, retries: {MAX_RETRIES}")

    session = make_session()
    result = {}
    total_points = 0

    for route, labels in PIPE_POINTS.items():
        print(f"\n── {route} ({len(labels)} points × {len(months)} months) ──")
        route_daily = {}

        for label in labels:
            label_days = 0
            print(f"  {label}: ", end="", flush=True)

            for mo in months:
                rows = fetch_point_month(session, label, year, mo)
                if rows:
                    for row in rows:
                        val_kwh = float(row.get("value", 0) or 0)
                        mcm = val_kwh * KWH_TO_MCM
                        date = (row.get("periodFrom") or row.get("from") or "")[:10]
                        if len(date) < 10:
                            continue
                        route_daily[date] = route_daily.get(date, 0) + mcm
                        label_days += 1
                    print(f"M{mo}✓", end=" ", flush=True)
                else:
                    print(f"M{mo}✗", end=" ", flush=True)

                time.sleep(DELAY_S)

            if label_days > 0:
                total_points += 1
                print(f" ({label_days} days)")
            else:
                print(" (no data)")

        result[route] = dict(sorted(route_daily.items()))
        print(f"  → {route}: {len(route_daily)} unique days")

    # Metadata
    result["_meta"] = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "year": year,
        "months": current_month,
        "points_ok": total_points,
        "routes": list(PIPE_POINTS.keys()),
    }

    # Write output
    out_dir = os.environ.get("OUTPUT_DIR", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "pipeline_daily.json")
    with open(out_path, "w") as f:
        json.dump(result, f, separators=(",", ":"))

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\n✓ {out_path} ({size_kb:.1f} KB) — {total_points} points OK")


if __name__ == "__main__":
    main()
