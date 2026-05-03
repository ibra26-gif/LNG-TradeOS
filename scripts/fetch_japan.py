#!/usr/bin/env python3
"""Build Japan analytics data for LNG TradeOS.

Current wired data:
- Japan nuclear generation from Ember Monthly Electricity Data.
- Japan city weather forecast from Open-Meteo, used only as a numeric
  forecast bridge while the official JMA long-range page remains manual.

Known gaps are written explicitly into data/japan.json; no gas-balance or
LNG-storage values are estimated.
"""

from __future__ import annotations

import csv
import io
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_PATH = os.path.join(ROOT, "data", "japan.json")
EMBER_URL = "https://files.ember-energy.org/public-downloads/monthly_full_release_long_format.csv"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_text(url: str, timeout: int = 60) -> str:
    req = Request(url, headers={"User-Agent": "LNG-TradeOS Japan analytics refresh"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def fetch_json(url: str, timeout: int = 45) -> dict:
    return json.loads(fetch_text(url, timeout=timeout))


def load_ember_japan_nuclear() -> dict:
    text = fetch_text(EMBER_URL, timeout=90)
    rows = []
    reader = csv.DictReader(io.StringIO(text))
    for r in reader:
        if r.get("Area") != "Japan":
            continue
        if r.get("Variable") != "Nuclear":
            continue
        if r.get("Subcategory") != "Fuel":
            continue
        unit = r.get("Unit")
        if unit not in ("TWh", "%"):
            continue
        try:
            val = float(r["Value"]) if r.get("Value") else None
        except ValueError:
            val = None
        if val is None:
            continue
        rows.append({"date": r["Date"], "unit": unit, "value": val})

    by_date: dict[str, dict] = {}
    for r in rows:
        d = by_date.setdefault(r["date"], {})
        if r["unit"] == "TWh":
            d["twh"] = r["value"]
        elif r["unit"] == "%":
            d["sharePct"] = r["value"]

    series = sorted(
        [
            {
                "date": date,
                "twh": vals.get("twh"),
                "sharePct": vals.get("sharePct"),
            }
            for date, vals in by_date.items()
        ],
        key=lambda x: x["date"],
    )
    return {
        "source": "Ember Monthly Electricity Data",
        "sourceUrl": EMBER_URL,
        "series": series,
    }


JAPAN_CITIES = [
    {"id": "tokyo", "name": "Tokyo", "lat": 35.6762, "lon": 139.6503, "weight": 0.34},
    {"id": "osaka", "name": "Osaka", "lat": 34.6937, "lon": 135.5023, "weight": 0.22},
    {"id": "nagoya", "name": "Nagoya", "lat": 35.1815, "lon": 136.9066, "weight": 0.16},
    {"id": "fukuoka", "name": "Fukuoka", "lat": 33.5902, "lon": 130.4017, "weight": 0.14},
    {"id": "sapporo", "name": "Sapporo", "lat": 43.0618, "lon": 141.3545, "weight": 0.14},
]


def load_weather_forecast() -> dict:
    city_rows = []
    weighted_by_date: dict[str, dict] = {}
    for city in JAPAN_CITIES:
        params = urlencode(
            {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "daily": "temperature_2m_mean",
                "timezone": "Asia/Tokyo",
                "forecast_days": 14,
            }
        )
        payload = fetch_json(f"{OPEN_METEO_URL}?{params}", timeout=45)
        dates = payload.get("daily", {}).get("time", [])
        temps = payload.get("daily", {}).get("temperature_2m_mean", [])
        points = []
        for date, temp in zip(dates, temps):
            if temp is None:
                continue
            hdd18 = max(0.0, 18.0 - float(temp))
            cdd24 = max(0.0, float(temp) - 24.0)
            points.append(
                {
                    "date": date,
                    "tempMeanC": round(float(temp), 1),
                    "hdd18": round(hdd18, 1),
                    "cdd24": round(cdd24, 1),
                }
            )
            agg = weighted_by_date.setdefault(date, {"temp": 0.0, "hdd18": 0.0, "cdd24": 0.0})
            agg["temp"] += float(temp) * city["weight"]
            agg["hdd18"] += hdd18 * city["weight"]
            agg["cdd24"] += cdd24 * city["weight"]
        city_rows.append({**city, "forecast": points})

    national = [
        {
            "date": date,
            "tempMeanC": round(vals["temp"], 1),
            "hdd18": round(vals["hdd18"], 1),
            "cdd24": round(vals["cdd24"], 1),
        }
        for date, vals in sorted(weighted_by_date.items())
    ]
    return {
        "source": "Open-Meteo Forecast API",
        "sourceUrl": OPEN_METEO_URL,
        "officialJmaLongRangeUrl": "https://www.jma.go.jp/en/longfcst/",
        "officialJmaRegionalUrl": "https://www.jma.go.jp/en/longfcst/000_1_11.html",
        "note": "Numeric city forecast is Open-Meteo. JMA long-range page is linked as official source until a stable JMA JSON feed is wired.",
        "baseHddC": 18,
        "baseCddC": 24,
        "cities": city_rows,
        "national": national,
    }


def main() -> None:
    out = {
        "schema": "japan_analytics_v1",
        "country": "Japan",
        "cachedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "gasBalance": {
            "status": "source_gap",
            "weeklyLngStorage": None,
            "lngImports": None,
            "gasForPower": None,
            "sourceGaps": [
                {
                    "item": "Weekly LNG storage",
                    "status": "not_wired",
                    "note": "Find stable METI/OCCTO/JOGMEC public table before plotting. Do not estimate.",
                },
                {
                    "item": "OCCTO monthly power/generation",
                    "status": "manual_navigation_required",
                    "sourceUrl": "http://occtonet.occto.or.jp/public/dfw/RP11/OCCTO/SD/LOGIN_login#",
                    "note": "User path: Option 3 > Option 3 > Option 1 > month tab > refresh. Needs scraper proof.",
                },
            ],
        },
        "nuclear": load_ember_japan_nuclear(),
        "weather": load_weather_forecast(),
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
