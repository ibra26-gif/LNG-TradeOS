#!/usr/bin/env python3
"""
ENTSOG Pipeline Data Fetcher
Runs as a GitHub Action daily. Fetches physical flow data for all EU pipeline
import corridors, converts to MCM, and writes a static JSON file.

The frontend loads this JSON instantly — zero ENTSOG API calls from the browser.

Usage:
    python3 scripts/fetch_entsog.py

Output:
    data/pipeline_daily.json
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path

# ─── Configuration ──────────────────────────────────────────────────────────────

ENTSOG_BASE = "https://transparency.entsog.eu/api/v1/operationaldatas"

# Pipeline point labels — must exactly match ENTSOG's pointLabel values
PIPE_POINTS = {
    "norway": [
        "Zeebrugge ZPT", "Dornum GASPOOL", "Emden (EPT1) (OGE)",
        "Easington", "St. Fergus", "Nybro", "Dunkerque"
    ],
    "algeria_italy": ["Mazara del Vallo", "Gela"],
    "algeria_spain": ["Almería", "Tarifa"],
    "tap": ["Kipi (TR) / Kipi (GR)", "Melendugno - IT / TAP"],
    "russia": [
        "Strandzha 2 (BG) / Malkoclar (TR)", "Budince",
        "Uzhhorod (UA) - Velke Kapusany (SK)",
        "Isaccea (RO) - Orlovka (UA) II"
    ],
}

# kWh/d → MCM conversion (ENTSOG reports in kWh/d)
KWH_TO_MCM = 1.0 / (1e6 * 10.55)

# Retry config
MAX_RETRIES = 5
BASE_DELAY = 3  # seconds
DELAY_BETWEEN_POINTS = 2  # seconds between API calls

# How many years of data to fetch
YEARS_TO_FETCH = [2025, 2026]


# ─── Helpers ────────────────────────────────────────────────────────────────────

def fetch_entsog(point_label: str, from_date: str, to_date: str) -> list:
    """Fetch physical flow data for one point with exponential backoff retry."""
    params = urllib.parse.urlencode({
        "indicator": "Physical Flow",
        "directionKey": "entry",
        "pointLabel": point_label,
        "from": from_date,
        "to": to_date,
        "periodType": "day",
        "timeZone": "WET",
        "limit": "5000",
    })
    url = f"{ENTSOG_BASE}?{params}"

    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    rows = (
                        data.get("operationaldatas")
                        or data.get("operationalDatas")
                        or data.get("operationalData")
                        or data.get("data")
                        or []
                    )
                    return rows
        except (urllib.error.HTTPError, urllib.error.URLError, Exception) as e:
            delay = BASE_DELAY * (2 ** attempt)
            status = getattr(e, "code", "?")
            print(f"  Attempt {attempt+1}/{MAX_RETRIES} failed ({status}): {point_label} — retrying in {delay}s")
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
            else:
                print(f"  FAILED after {MAX_RETRIES} attempts: {point_label}")
                return []
    return []


def fetch_route_monthly(route: str, points: list, year: int) -> dict:
    """Fetch one route for one year, month by month (ENTSOG best practice).
    Returns {date_str: mcm_value} for daily data.
    """
    today = datetime.utcnow()
    daily = {}

    # Determine month range
    if year < today.year:
        months = range(1, 13)
    else:
        months = range(1, today.month + 1)

    for month in months:
        # Date range for this month
        from_date = f"{year}-{month:02d}-01"
        if month == 12:
            to_date = f"{year}-12-31"
        else:
            # Last day of month
            next_month = datetime(year, month + 1, 1) - timedelta(days=1)
            to_date = next_month.strftime("%Y-%m-%d")

        # Don't fetch future dates
        if datetime.strptime(from_date, "%Y-%m-%d") > today:
            break
        if datetime.strptime(to_date, "%Y-%m-%d") > today:
            to_date = today.strftime("%Y-%m-%d")

        for label in points:
            print(f"  {route}/{label} {year}-{month:02d}...")
            rows = fetch_entsog(label, from_date, to_date)

            for row in rows:
                date = (row.get("periodFrom") or row.get("from") or "")[:10]
                if not date:
                    continue
                value = float(row.get("value", 0) or 0)
                mcm = value * KWH_TO_MCM
                daily[date] = daily.get(date, 0) + mcm

            time.sleep(DELAY_BETWEEN_POINTS)

    return daily


def aggregate_monthly(daily: dict) -> dict:
    """Aggregate daily MCM into monthly totals: {YYYY-MM: {mcm, days}}."""
    monthly = {}
    for date, mcm in daily.items():
        mo = date[:7]
        if mo not in monthly:
            monthly[mo] = {"mcm": 0, "days": 0}
        monthly[mo]["mcm"] += mcm
        monthly[mo]["days"] += 1
    # Round for cleaner JSON
    for mo in monthly:
        monthly[mo]["mcm"] = round(monthly[mo]["mcm"], 3)
    return monthly


# ─── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"ENTSOG Pipeline Fetcher — {datetime.utcnow().isoformat()}Z")
    print(f"Years: {YEARS_TO_FETCH}")
    print(f"Routes: {list(PIPE_POINTS.keys())}")
    print(f"Total points: {sum(len(v) for v in PIPE_POINTS.values())}")
    print()

    result = {
        "updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "daily": {},    # {route: {date: mcm}}
        "monthly": {},  # {route: {YYYY-MM: {mcm, days}}}
    }

    for route, points in PIPE_POINTS.items():
        print(f"\n═══ {route.upper()} ({len(points)} points) ═══")
        all_daily = {}

        for year in YEARS_TO_FETCH:
            daily = fetch_route_monthly(route, points, year)
            all_daily.update(daily)
            print(f"  → {year}: {len(daily)} days fetched")

        # Round daily values
        all_daily = {d: round(v, 3) for d, v in sorted(all_daily.items())}

        result["daily"][route] = all_daily
        result["monthly"][route] = aggregate_monthly(all_daily)

        total_mcm = sum(all_daily.values())
        print(f"  Total: {len(all_daily)} days, {total_mcm:.0f} MCM cumulative")

    # Write output
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "pipeline_daily.json"

    with open(output_path, "w") as f:
        json.dump(result, f, separators=(",", ":"))

    size_kb = output_path.stat().st_size / 1024
    print(f"\n✓ Written to {output_path} ({size_kb:.1f} KB)")
    print(f"  Updated: {result['updated']}")
    for route in result["daily"]:
        days = len(result["daily"][route])
        print(f"  {route}: {days} days")


if __name__ == "__main__":
    main()
