#!/usr/bin/env python3
"""
fetch_entsog.py — ENTSOG pipeline flow scraper for LNG TradeOS™
Runs via GitHub Action (daily cron). Outputs /data/pipeline_daily.json.

Features:
  - All EU pipeline corridors: Norway, Algeria→IT, Libya (Greenstream),
    Algeria→ES, TAP, Russia (TurkStream), UK Interconnectors (IUK/BBL)
  - 404 skip: gracefully skips points that return 404/502/504
  - Rate limiting: 2s delay between requests
  - Retry with backoff on 429/5xx (except 404)

Output format:
  {
    "_meta": {"updated": "2026-04-17T18:00:00Z", "points": 18},
    "norway": {"2026-01-01": 123.45, ...},
    "algeria_italy": {...},
    ...
  }
  Values are MCM/day (kWh/d ÷ 1e6 ÷ 10.55).
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ENTSOG_BASE = "https://transparency.entsog.eu/api/v1/operationalDatas"
KWH_TO_MCM = 1 / (1e6 * 10.55)

# ── Pipeline corridor point labels (must match lngtradeos HTML) ──────────────
PIPE_POINTS = {
    "norway": [
        "Zeebrugge ZPT",
        "Dornum GASPOOL",
        "Emden (EPT1) (OGE)",
        "Easington",
        "St. Fergus",
        "Nybro",
        "Dunkerque",
    ],
    "algeria_italy": [
        "Mazara del Vallo",
    ],
    "libya": [
        "Gela",
    ],
    "algeria_spain": [
        "Almería",
        "Tarifa",
    ],
    "tap": [
        "Kipi (TR) / Kipi (GR)",
        "Melendugno - IT / TAP",
    ],
    "russia": [
        "Strandzha 2 (BG) / Malkoclar (TR)",
        "Budince",
        "Uzhhorod (UA) - Velke Kapusany (SK)",
        "Isaccea (RO) - Orlovka (UA) II",
    ],
    "uk_interconn": [
        "Zeebrugge IZT",
        "Bacton (BBL)",
    ],
}

DELAY_S = 2.0       # seconds between requests
MAX_RETRIES = 2      # retries on transient errors (429, 500, 502, 503)
RETRY_BACKOFF = 5.0  # seconds before retry

# HTTP codes to skip permanently (point doesn't exist for this query)
SKIP_CODES = {404}
# HTTP codes to retry
RETRY_CODES = {429, 500, 502, 503, 504}


def fetch_point(label: str, from_date: str, to_date: str) -> list:
    """Fetch ENTSOG physical flow data for one point label.
    Returns list of {periodFrom, value} dicts. Skips on 404."""
    from urllib.parse import quote
    params = (
        f"indicator=Physical%20Flow&directionKey=entry"
        f"&pointLabel={quote(label)}"
        f"&from={from_date}&to={to_date}"
        f"&periodType=day&timeZone=WET&limit=5000"
    )
    url = f"{ENTSOG_BASE}?{params}"
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "LNG-TradeOS-Bot/1.0"})

    for attempt in range(1 + MAX_RETRIES):
        try:
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                # ENTSOG uses various key names for the data array
                rows = (
                    data.get("operationaldatas")
                    or data.get("operationalDatas")
                    or data.get("operationalData")
                    or data.get("data")
                    or []
                )
                return rows
        except HTTPError as e:
            code = e.code
            if code in SKIP_CODES:
                print(f"  ⏭  404 skip: {label}")
                return []
            if code in RETRY_CODES and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (attempt + 1)
                print(f"  ⚠  {code} on {label} — retry {attempt+1}/{MAX_RETRIES} in {wait}s")
                time.sleep(wait)
                continue
            print(f"  ✗  HTTP {code} on {label} — giving up")
            return []
        except (URLError, OSError, json.JSONDecodeError) as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (attempt + 1)
                print(f"  ⚠  {type(e).__name__} on {label} — retry {attempt+1}/{MAX_RETRIES} in {wait}s")
                time.sleep(wait)
                continue
            print(f"  ✗  {type(e).__name__} on {label}: {e} — giving up")
            return []
    return []


def aggregate_rows(rows: list) -> dict:
    """Convert ENTSOG rows to {YYYY-MM-DD: mcm} daily values."""
    daily = {}
    for row in rows:
        val_kwh = float(row.get("value", 0) or 0)
        mcm = val_kwh * KWH_TO_MCM
        date = (row.get("periodFrom") or row.get("from") or "")[:10]
        if len(date) < 10:
            continue
        daily[date] = daily.get(date, 0) + mcm
    return daily


def main():
    # Date range: current year to today
    today = datetime.utcnow().date()
    from_date = f"{today.year}-01-01"
    to_date = today.isoformat()

    # Optional: extend to previous year if env var set
    if os.environ.get("FETCH_PREV_YEAR", "").lower() in ("1", "true", "yes"):
        from_date = f"{today.year - 1}-01-01"
        print(f"Extended range: {from_date} → {to_date}")

    print(f"fetch_entsog.py — {from_date} to {to_date}")
    print(f"Routes: {', '.join(PIPE_POINTS.keys())}")

    result = {}
    total_points = 0
    total_days = 0

    for route, labels in PIPE_POINTS.items():
        print(f"\n── {route} ({len(labels)} points) ──")
        route_daily = {}
        for label in labels:
            print(f"  → {label}...", end=" ", flush=True)
            rows = fetch_point(label, from_date, to_date)
            if rows:
                daily = aggregate_rows(rows)
                # Merge into route daily (sum across points)
                for date, mcm in daily.items():
                    route_daily[date] = route_daily.get(date, 0) + mcm
                print(f"OK ({len(rows)} rows, {len(daily)} days)")
                total_points += 1
            else:
                print("no data")
            time.sleep(DELAY_S)

        result[route] = dict(sorted(route_daily.items()))
        total_days += len(route_daily)
        print(f"  ✓ {route}: {len(route_daily)} days")

    # Add metadata
    result["_meta"] = {
        "updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "from": from_date,
        "to": to_date,
        "points": total_points,
        "days": total_days,
        "routes": list(PIPE_POINTS.keys()),
    }

    # Write output
    out_dir = os.environ.get("OUTPUT_DIR", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "pipeline_daily.json")
    with open(out_path, "w") as f:
        json.dump(result, f, separators=(",", ":"))

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\n✓ Written {out_path} ({size_kb:.1f} KB)")
    print(f"  {total_points} points fetched, {total_days} total day-values")


if __name__ == "__main__":
    main()
