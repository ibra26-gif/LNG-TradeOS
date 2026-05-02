#!/usr/bin/env python3
"""Build Thailand CDD weather monitor data from Open-Meteo.

The trading signal is monthly cooling stress. We compute CDD ourselves from
daily mean temperature:

    CDD = max(mean_temp_c - base_temp_c, 0)

No weather values are seeded. If the API is unavailable, the script fails and
keeps the previous committed JSON untouched.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timezone
import calendar
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "thailand_weather_cdd.json"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
BASE_TEMP_C = 24.0
START_DATE = "2021-01-01"

CITIES = [
    {"id": "bangkok", "name": "Bangkok / Central", "lat": 13.7563, "lon": 100.5018, "weight": 0.40},
    {"id": "rayong", "name": "Rayong / EEC", "lat": 12.6814, "lon": 101.2816, "weight": 0.20},
    {"id": "chiang_mai", "name": "Chiang Mai / North", "lat": 18.7883, "lon": 98.9853, "weight": 0.15},
    {"id": "khon_kaen", "name": "Khon Kaen / Northeast", "lat": 16.4419, "lon": 102.8350, "weight": 0.15},
    {"id": "hat_yai", "name": "Hat Yai / South", "lat": 7.0086, "lon": 100.4747, "weight": 0.10},
]


def round_or_none(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(value, digits)


def fetch_open_meteo() -> list[dict[str, Any]]:
    params = {
        "latitude": ",".join(str(c["lat"]) for c in CITIES),
        "longitude": ",".join(str(c["lon"]) for c in CITIES),
        "start_date": START_DATE,
        "end_date": date.today().isoformat(),
        "daily": "temperature_2m_mean",
        "timezone": "Asia/Bangkok",
    }
    url = f"{OPEN_METEO_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "LNG-TradeOS Thailand CDD refresh"})
    with urlopen(req, timeout=90) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list) or len(raw) != len(CITIES):
        raise RuntimeError("Open-Meteo response did not return all Thailand city points")
    return raw


def build_daily(api_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = {}
    for city, payload in zip(CITIES, api_rows):
        daily = payload.get("daily") or {}
        times = daily.get("time") or []
        temps = daily.get("temperature_2m_mean") or []
        for ds, temp in zip(times, temps):
            if temp is None:
                continue
            rec = by_day.setdefault(ds, {"date": ds, "cities": {}, "_temp_w": 0.0, "_cdd_w": 0.0, "_w": 0.0})
            cdd = max(float(temp) - BASE_TEMP_C, 0.0)
            rec["cities"][city["id"]] = {
                "temp_c": round_or_none(float(temp), 2),
                "cdd": round_or_none(cdd, 2),
            }
            rec["_temp_w"] += float(temp) * city["weight"]
            rec["_cdd_w"] += cdd * city["weight"]
            rec["_w"] += city["weight"]

    out = []
    for ds in sorted(by_day):
        rec = by_day[ds]
        w = rec.pop("_w") or 1.0
        temp_w = rec.pop("_temp_w")
        cdd_w = rec.pop("_cdd_w")
        rec["index_temp_c"] = round_or_none(temp_w / w, 2)
        rec["index_cdd"] = round_or_none(cdd_w / w, 2)
        out.append(rec)
    return out


def build_monthly(daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "days": 0,
        "temp_sum": 0.0,
        "cdd_sum": 0.0,
        "cities": defaultdict(lambda: {"temp_sum": 0.0, "cdd_sum": 0.0, "days": 0}),
    })
    for row in daily_rows:
        ym = row["date"][:7]
        b = buckets[ym]
        b["days"] += 1
        b["temp_sum"] += row["index_temp_c"]
        b["cdd_sum"] += row["index_cdd"]
        for cid, vals in row["cities"].items():
            cb = b["cities"][cid]
            cb["days"] += 1
            cb["temp_sum"] += vals["temp_c"]
            cb["cdd_sum"] += vals["cdd"]

    rows = []
    for ym in sorted(buckets):
        b = buckets[ym]
        days = b["days"] or 1
        city_out = {}
        for cid, cb in b["cities"].items():
            cdays = cb["days"] or 1
            city_out[cid] = {
                "avg_temp_c": round_or_none(cb["temp_sum"] / cdays, 2),
                "cdd": round_or_none(cb["cdd_sum"], 2),
                "days": cb["days"],
            }
        rows.append({
            "month": ym,
            "days": b["days"],
            "avg_temp_c": round_or_none(b["temp_sum"] / days, 2),
            "cdd": round_or_none(b["cdd_sum"], 2),
            "cities": city_out,
        })
    return rows


def add_normals(monthly_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    current_year = date.today().year
    baseline_years = list(range(current_year - 5, current_year))
    by_cal_month: dict[int, list[float]] = defaultdict(list)
    for row in monthly_rows:
        y = int(row["month"][:4])
        m = int(row["month"][5:7])
        if y in baseline_years and row.get("cdd") is not None:
            by_cal_month[m].append(float(row["cdd"]))
    normals = {m: (sum(vals) / len(vals)) for m, vals in by_cal_month.items() if vals}
    for row in monthly_rows:
        m = int(row["month"][5:7])
        y = int(row["month"][:4])
        normal = normals.get(m)
        row["normal_cdd"] = round_or_none(normal, 2)
        dim = calendar.monthrange(y, m)[1]
        days = row.get("days") or dim
        prorated_normal = normal * min(days, dim) / dim if normal is not None else None
        row["is_partial_month"] = days < dim
        row["normal_cdd_to_date"] = round_or_none(prorated_normal, 2)
        if normal is None:
            row["cdd_anomaly"] = None
            row["cdd_anomaly_pct"] = None
            row["cdd_anomaly_to_date"] = None
            row["cdd_anomaly_to_date_pct"] = None
        else:
            anom = float(row["cdd"]) - normal
            anom_to_date = float(row["cdd"]) - prorated_normal
            row["cdd_anomaly"] = round_or_none(anom, 2)
            row["cdd_anomaly_pct"] = round_or_none(anom / normal * 100, 1) if normal else None
            row["cdd_anomaly_to_date"] = round_or_none(anom_to_date, 2)
            row["cdd_anomaly_to_date_pct"] = round_or_none(anom_to_date / prorated_normal * 100, 1) if prorated_normal else None
    return monthly_rows, f"{baseline_years[0]}-{baseline_years[-1]}"


def main() -> None:
    api_rows = fetch_open_meteo()
    daily_rows = build_daily(api_rows)
    if not daily_rows:
        raise SystemExit("No Thailand CDD rows returned by Open-Meteo")
    monthly_rows, normal_period = add_normals(build_monthly(daily_rows))
    latest_daily = daily_rows[-1]["date"]
    latest_month = monthly_rows[-1]["month"]
    out = {
        "schema": "thailand_weather_cdd_v1",
        "country": "Thailand",
        "frequency": "daily and monthly",
        "base_temp_c": BASE_TEMP_C,
        "normal_period": normal_period,
        "latest_date": latest_daily,
        "latest_month": latest_month,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "forecast_included": False,
        "sources": {
            "weather": "Open-Meteo Historical Weather API",
            "weather_url": OPEN_METEO_URL,
            "method": "CDD=max(daily mean temperature - 24C, 0); weighted city basket",
        },
        "city_weights": CITIES,
        "monthly": monthly_rows,
        "daily": daily_rows,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {OUT} · {len(monthly_rows)} months · latest {latest_month}")


if __name__ == "__main__":
    main()
