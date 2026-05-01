#!/usr/bin/env python3
"""
Build monthly ENTSOG physical-flow history for the EU Gas Balance table.

Output:
  data/pipeline_history_monthly.json

Shape:
  {
    "norway": {"2025-01": 12345.6, ...},  # mcm/month
    ...
    "_meta": {...}
  }

Only external supply corridors are included. Intra-European interconnectors
are deliberately excluded to avoid double-counting the EU+UK aggregate balance.
"""

import json
import os
import sys
import time
from datetime import date, datetime, timezone

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
    "tap": ["Melendugno - IT / TAP"],
    "russia": [
        "Strandzha 2 (BG) / Malkoclar (TR)", "Budince",
        "Uzhhorod (UA) - Velke Kapusany (SK)",
        "Isaccea (RO) - Orlovka (UA) II",
    ],
}

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (LNG TradeOS historical ENTSOG scraper)",
}

TIMEOUT = int(os.environ.get("ENTSOG_TIMEOUT", "90"))
DELAY_S = float(os.environ.get("ENTSOG_DELAY_S", "0.75"))
MAX_RETRIES = int(os.environ.get("ENTSOG_RETRIES", "3"))
RETRY_CODES = {429, 500, 502, 503, 504}


def last_day(year, month):
    if month == 12:
        return 31
    return (date(year, month + 1, 1) - date(year, month, 1)).days


def months_between(start_year, end_year):
    today = datetime.now(timezone.utc).date()
    for year in range(start_year, end_year + 1):
        max_month = 12 if year < today.year else today.month
        for month in range(1, max_month + 1):
            yield year, month


def fetch_point_month(session, label, year, month):
    from_date = f"{year}-{month:02d}-01"
    to_date = f"{year}-{month:02d}-{last_day(year, month):02d}"
    today = datetime.now(timezone.utc).date().isoformat()
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
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = session.get(ENTSOG_BASE, params=params, timeout=TIMEOUT)
            if r.status_code in RETRY_CODES and attempt < MAX_RETRIES:
                time.sleep((attempt + 1) * 5)
                continue
            if r.status_code == 404:
                return []
            r.raise_for_status()
            data = r.json()
            rows = (
                data.get("operationaldatas")
                or data.get("operationalDatas")
                or data.get("operationalData")
                or data.get("data")
                or []
            )
            out = []
            for row in rows:
                indicator = (row.get("indicator") or "").strip().lower()
                if indicator and indicator != "physical flow":
                    continue
                ds = (row.get("periodFrom") or row.get("from") or "")[:10]
                if not ds or ds > today:
                    continue
                out.append(row)
            return out
        except Exception as exc:
            if attempt < MAX_RETRIES:
                time.sleep((attempt + 1) * 5)
                continue
            print(f"  {label} {year}-{month:02d}: {type(exc).__name__}: {exc}")
            return []
    return []


def main():
    today = datetime.now(timezone.utc).date()
    start_year = int(os.environ.get("HIST_START_YEAR", str(today.year - 3)))
    end_year = int(os.environ.get("HIST_END_YEAR", str(today.year)))
    output_dir = os.environ.get("OUTPUT_DIR", "data")
    os.makedirs(output_dir, exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    result = {route: {} for route in PIPE_POINTS}
    points_ok = 0
    months = list(months_between(start_year, end_year))
    print(f"ENTSOG historical monthly {start_year}-{end_year} · {len(months)} months")

    for route, labels in PIPE_POINTS.items():
        print(f"\n-- {route} --")
        for label in labels:
            label_ok = 0
            print(f"  {label}: ", end="", flush=True)
            for year, month in months:
                rows = fetch_point_month(session, label, year, month)
                if rows:
                    mo = f"{year}-{month:02d}"
                    total = sum((float(r.get("value", 0) or 0) * KWH_TO_MCM) for r in rows)
                    result[route][mo] = result[route].get(mo, 0) + total
                    label_ok += 1
                    print(f"{mo} ", end="", flush=True)
                time.sleep(DELAY_S)
            if label_ok:
                points_ok += 1
            print(f"({label_ok} mo)")

    for route in list(result):
        result[route] = {k: round(v, 6) for k, v in sorted(result[route].items())}

    result["_meta"] = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "ENTSOG Transparency Platform operationalDatas · Physical Flow",
        "unit": "mcm/month",
        "start_year": start_year,
        "end_year": end_year,
        "routes": list(PIPE_POINTS.keys()),
        "points_ok": points_ok,
        "note": "External supply corridors only; intra-European interconnectors excluded.",
    }

    out_path = os.path.join(output_dir, "pipeline_history_monthly.json")
    with open(out_path, "w") as f:
        json.dump(result, f, separators=(",", ":"))
    print(f"\nDONE {out_path} ({os.path.getsize(out_path)/1024:.1f} KB), points_ok={points_ok}")
    if points_ok == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
