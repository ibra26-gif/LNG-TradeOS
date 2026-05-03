#!/usr/bin/env python3
"""Build Japan analytics data for LNG TradeOS.

Current wired data:
- Japan nuclear generation from Ember Monthly Electricity Data.
- Japan city weather forecast from Open-Meteo, used only as a numeric
  forecast bridge while the official JMA long-range page remains manual.

Known gaps and official gas-balance source candidates are written explicitly
into data/japan.json; no gas-balance or LNG-storage values are estimated.
"""

from __future__ import annotations

import csv
import html
import io
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_PATH = os.path.join(ROOT, "data", "japan.json")
EMBER_URL = "https://files.ember-energy.org/public-downloads/monthly_full_release_long_format.csv"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CUSTOMS_IMPORT_LIST_URL = (
    "https://www.e-stat.go.jp/en/stat-search/files?"
    "page=1&layout=datalist&toukei=00350300&tstat=000001013141&cycle=1&"
    "tclass1=000001013180&tclass2=000001013182&cycle_facet=cycle&"
    "tclass3val=0&metadata=1&data=1"
)
CUSTOMS_VALUE_UNIT_SOURCE_URL = "https://www.customs.go.jp/toukei/suii/html/time_e.htm"
CUSTOMS_HS_LNG = "271111000"

CUSTOMS_COUNTRY_NAMES = {
    "105": "China",
    "113": "Malaysia",
    "116": "Brunei",
    "118": "Indonesia",
    "140": "Qatar",
    "141": "Oman",
    "147": "United Arab Emirates",
    "224": "Russia",
    "302": "Canada",
    "304": "United States",
    "320": "Trinidad and Tobago",
    "407": "Peru",
    "524": "Nigeria",
    "530": "Equatorial Guinea",
    "545": "Mozambique",
    "601": "Australia",
    "602": "Papua New Guinea",
}

MONTHS = [
    ("Jan", 1),
    ("Feb", 2),
    ("Mar", 3),
    ("Apr", 4),
    ("May", 5),
    ("Jun", 6),
    ("Jul", 7),
    ("Aug", 8),
    ("Sep", 9),
    ("Oct", 10),
    ("Nov", 11),
    ("Dec", 12),
]

JAPAN_GAS_BALANCE_SOURCE_MAP = [
    {
        "item": "LNG imports by country, volume and value",
        "balanceUse": "LNG import supply and supplier mix",
        "source": "Japan Customs / Ministry of Finance Trade Statistics",
        "sourceUrl": "https://www.customs.go.jp/toukei/info/tsdl_e.htm",
        "searchUrl": "https://www.customs.go.jp/toukei/srch/indexe.htm",
        "downloadUrl": CUSTOMS_IMPORT_LIST_URL,
        "unitSourceUrl": CUSTOMS_VALUE_UNIT_SOURCE_URL,
        "pull": "Monthly Commodity by Country import table; HS 271111000 = liquefied natural gas. Quantity is Customs CSV Unit2; value is JPY1000 per Customs notes.",
        "fields": ["month", "partner_country", "quantity_mt", "value_jpy_000"],
        "status": "wired",
        "wiring": "csv_parser_wired",
    },
    {
        "item": "City gas production, purchases, sales and inventories",
        "balanceUse": "City-gas demand, LNG receipts/production and city-gas stock cross-check",
        "source": "METI / ANRE Gas Business Production Dynamic Statistics",
        "sourceUrl": "https://www.enecho.meti.go.jp/statistics/gas/ga001/results.html",
        "pull": "Monthly Excel files from the statistics table list.",
        "fields": ["month", "vaporized_lng", "production_purchases", "sales_by_sector", "inventory"],
        "status": "source_identified",
        "wiring": "excel_parser_pending",
    },
    {
        "item": "Power-sector LNG receipts, burn and stocks",
        "balanceUse": "Power LNG demand and utility inventory",
        "source": "METI / ANRE Electric Power Survey Statistics",
        "sourceUrl": "https://www.enecho.meti.go.jp/statistics/electric_power/ep002/results_archive.html",
        "currentUrl": "https://www.enecho.meti.go.jp/statistics/electric_power/ep002/results.html",
        "pull": "Monthly thermal power fuel statistics Excel table; LNG fuel consumption/receipts/stocks where reported.",
        "fields": ["month", "lng_receipts", "lng_consumption", "lng_inventory"],
        "status": "source_identified",
        "wiring": "excel_parser_pending",
    },
    {
        "item": "Ready-made LNG inventory cross-check",
        "balanceUse": "Published sanity check for total, city-gas and power LNG inventories",
        "source": "JOGMEC Natural Gas & LNG related information",
        "sourceUrl": "https://journal.jogmec.go.jp/oilgas/nglng-en/previous-articles/202412.html",
        "pull": "Monthly Japan inventory commentary compiled from METI Gas Business and Thermal Power Generation Statistics.",
        "fields": ["month", "total_lng_inventory", "city_gas_inventory", "power_inventory"],
        "status": "cross_check_only",
        "wiring": "manual_reference_until_table_feed_found",
    },
]


def fetch_text(url: str, timeout: int = 60) -> str:
    req = Request(url, headers={"User-Agent": "LNG-TradeOS Japan analytics refresh"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def fetch_bytes(url: str, timeout: int = 60) -> bytes:
    req = Request(url, headers={"User-Agent": "LNG-TradeOS Japan analytics refresh"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_json(url: str, timeout: int = 45) -> dict:
    return json.loads(fetch_text(url, timeout=timeout))


def safe_float(value: str | None) -> float:
    if value is None:
        return 0.0
    value = str(value).strip().replace(",", "")
    if not value or value in {"-", "—"}:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def customs_abs(url: str) -> str:
    return urljoin("https://www.e-stat.go.jp/en/stat-search/files", html.unescape(url))


def find_customs_cycle_links(list_html: str, limit: int = 2) -> list[dict]:
    """Return latest e-Stat cycle pages for Commodity by Country Import."""
    links: list[dict] = []
    for match in re.finditer(
        r'href="(?P<href>[^"]*year=(?P<year>\d{4})0[^"]*month=(?P<month>\d+)[^"]*)"\s+class="stat-item_child">(?P<label>[^<]+)</a>',
        list_html,
    ):
        url = customs_abs(match.group("href"))
        if any(item["url"] == url for item in links):
            continue
        links.append(
            {
                "year": int(match.group("year")),
                "monthToken": match.group("month"),
                "label": html.unescape(match.group("label")).replace(".", ""),
                "url": url,
            }
        )
        if len(links) >= limit:
            break
    return links


def find_section_v_csv(page_html: str) -> dict | None:
    """Find the Section V Chapter 25-27 CSV link that contains HS 271111000."""
    clean = html.unescape(page_html)
    idx = clean.find("Section V Chapter 25-27")
    if idx < 0:
        return None
    window = clean[max(0, idx - 1200) : idx + 2200]
    title_match = re.search(r">\s*([^<]*Section V Chapter 25-27)\s*<", window)
    survey_match = re.search(r"Survey date.*?([0-9]{4}[A-Za-z.]+)", window, re.S)
    update_match = re.search(r"Update date.*?([0-9]{4}-[0-9]{2}-[0-9]{2})", window, re.S)
    download_match = re.search(r'href="([^"]*file-download\?statInfId=(\d+)&fileKind=1)"', window)
    if not download_match:
        return None
    return {
        "title": " ".join((title_match.group(1) if title_match else "Section V Chapter 25-27").split()),
        "surveyDate": survey_match.group(1).replace(".", "") if survey_match else None,
        "updateDate": update_match.group(1) if update_match else None,
        "statInfId": download_match.group(2),
        "downloadUrl": customs_abs(download_match.group(1)),
    }


def parse_customs_lng_csv(csv_text: str, meta: dict) -> list[dict]:
    monthly: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        hs = (row.get("HS") or "").replace("'", "").strip()
        if hs != CUSTOMS_HS_LNG:
            continue
        if str(row.get("Exp or Imp") or "").strip() != "2":
            continue
        year = int(safe_float(row.get("Year")))
        country_code = str(row.get("Country") or "").strip()
        country = CUSTOMS_COUNTRY_NAMES.get(country_code, f"Country code {country_code}")
        unit = (row.get("Unit2") or row.get("Unit1") or "").strip()
        for month_name, month_num in MONTHS:
            quantity = safe_float(row.get(f"Quantity2-{month_name}") or row.get(f"Quantity1-{month_name}"))
            value = safe_float(row.get(f"Value-{month_name}"))
            if quantity <= 0 and value <= 0:
                continue
            key = f"{year}-{month_num:02d}"
            bucket = monthly.setdefault(
                key,
                {
                    "month": key,
                    "lngImportsTonnes": 0.0,
                    "lngImportsValueJpyThousand": 0.0,
                    "customsCountryCount": 0,
                    "customsCountries": [],
                    "customsUnit": unit or "MT",
                    "customsStatus": "parsed",
                    "customsStatInfId": meta.get("statInfId"),
                    "customsSurveyDate": meta.get("surveyDate"),
                    "customsUpdateDate": meta.get("updateDate"),
                    "customsDownloadUrl": meta.get("downloadUrl"),
                },
            )
            bucket["lngImportsTonnes"] += quantity
            bucket["lngImportsValueJpyThousand"] += value
            bucket["customsCountries"].append(
                {
                    "code": country_code,
                    "country": country,
                    "tonnes": round(quantity),
                    "valueJpyThousand": round(value),
                }
            )

    rows = []
    for key, row in sorted(monthly.items()):
        countries = sorted(row["customsCountries"], key=lambda x: x["tonnes"], reverse=True)
        row["customsCountries"] = countries
        row["topSuppliers"] = countries[:5]
        row["customsCountryCount"] = len(countries)
        row["lngImportsMillionTonnes"] = round(row["lngImportsTonnes"] / 1_000_000, 3)
        row["lngImportsValueJpyBillion"] = round(row["lngImportsValueJpyThousand"] / 1_000_000, 1)
        row["coverage"] = {
            "customsLngImports": True,
            "metiGasBusiness": False,
            "metiPowerFuel": False,
            "jogmecInventory": False,
        }
        row["cityGasDemand"] = None
        row["powerLngBurn"] = None
        row["lngInventory"] = None
        rows.append(row)
    return rows


def load_customs_lng_imports() -> dict:
    list_html = fetch_text(CUSTOMS_IMPORT_LIST_URL, timeout=60)
    cycles = find_customs_cycle_links(list_html, limit=2)
    all_rows: dict[str, dict] = {}
    files = []
    for cycle in cycles:
        page_html = fetch_text(cycle["url"], timeout=60)
        meta = find_section_v_csv(page_html)
        if not meta:
            files.append({**cycle, "status": "section_v_not_found"})
            continue
        csv_bytes = fetch_bytes(meta["downloadUrl"], timeout=60)
        csv_text = csv_bytes.decode("utf-8-sig")
        rows = parse_customs_lng_csv(csv_text, meta)
        files.append({**cycle, **meta, "status": "parsed", "rowCount": len(rows)})
        for row in rows:
            all_rows[row["month"]] = row

    rows = sorted(all_rows.values(), key=lambda r: r["month"])
    latest = rows[-1] if rows else None
    return {
        "source": "Japan Customs / Ministry of Finance Trade Statistics via e-Stat",
        "sourceUrl": CUSTOMS_IMPORT_LIST_URL,
        "unitSourceUrl": CUSTOMS_VALUE_UNIT_SOURCE_URL,
        "hs": CUSTOMS_HS_LNG,
        "quantityUnit": "metric tonnes",
        "valueUnit": "JPY1000",
        "status": "parsed" if rows else "source_gap",
        "latestMonth": latest["month"] if latest else None,
        "latestUpdateDate": latest.get("customsUpdateDate") if latest else None,
        "files": files,
        "rows": rows,
    }


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
        "capacityReference": {
            "operableGw": 31.679,
            "source": "World Nuclear Association",
            "sourceUrl": "https://world-nuclear.org/information-library/country-profiles/countries-g-n/japan-nuclear-power",
            "note": "Used only to convert monthly Ember TWh into an operable-fleet utilization proxy. It is not a live reactor availability feed.",
        },
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
    customs_lng = load_customs_lng_imports()
    monthly_rows = customs_lng.get("rows", [])
    out = {
        "schema": "japan_analytics_v1",
        "country": "Japan",
        "cachedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "gasBalance": {
            "status": "partial_balance" if monthly_rows else "source_gap",
            "monthlyRows": monthly_rows,
            "weeklyLngStorage": None,
            "lngImports": customs_lng,
            "gasForPower": None,
            "sourceMap": JAPAN_GAS_BALANCE_SOURCE_MAP,
            "sourceGaps": [
                {
                    "item": "City-gas demand and inventories",
                    "status": "source_identified_not_wired",
                    "sourceUrl": "https://www.enecho.meti.go.jp/statistics/gas/ga001/results.html",
                    "note": "METI Gas Business monthly Excel source is identified, but the Excel parser is not wired yet. Values stay blank until parsed from the official workbook.",
                },
                {
                    "item": "Power-sector LNG burn, receipts and stocks",
                    "status": "source_identified_not_wired",
                    "sourceUrl": "https://www.enecho.meti.go.jp/statistics/electric_power/ep002/results.html",
                    "note": "METI Electric Power Survey fuel statistics source is identified, but not parsed yet. No gas-for-power or power inventory values are estimated.",
                },
                {
                    "item": "Weekly LNG storage",
                    "status": "cross_check_available_monthly",
                    "sourceUrl": "https://journal.jogmec.go.jp/oilgas/nglng-en/previous-articles/202412.html",
                    "note": "JOGMEC publishes inventory commentary compiled from METI Gas Business and Thermal Power Generation Statistics. Use as cross-check, not invented weekly stock.",
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
