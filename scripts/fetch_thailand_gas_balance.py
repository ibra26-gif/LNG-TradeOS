#!/usr/bin/env python3
"""Fetch Thailand monthly gas balance from EPPO workbooks.

Source tables:
  - T03_01_01 / T03_01_01-1: Production and Import of Natural Gas
  - T03_02_02 / T03_02_02-1: Consumption of Natural Gas by sector

EPPO publishes Excel 97-2003 .xls files. Values are monthly average flows in
MMSCFD. Distribution of NGL (T03_02_01) is intentionally ignored.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import xlrd


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "thailand_gas_balance.json"
BASE = "https://www.eppo.go.th/epposite/images/Energy-Statistics/energyinformation/Energy_Statistics/Natural_Gas"
PAGE = "https://www.eppo.go.th/index.php/en/en-energystatistics/ngv-statistic"

FILES = {
    "production_current": f"{BASE}/T03_01_01.xls",
    "production_history": f"{BASE}/T03_01_01-1.xls",
    "consumption_current": f"{BASE}/T03_02_02.xls",
    "consumption_history": f"{BASE}/T03_02_02-1.xls",
}

MONTHS = {
    "JAN": "01",
    "FEB": "02",
    "MAR": "03",
    "APR": "04",
    "MAY": "05",
    "JUN": "06",
    "JUL": "07",
    "AUG": "08",
    "SEP": "09",
    "OCT": "10",
    "NOV": "11",
    "DEC": "12",
}


def norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def as_num(value: Any) -> float | None:
    if value in ("", None):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = norm(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 3)


def download(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "LNG-TradeOS data refresh"})
    with urlopen(req, timeout=45) as resp:
        return resp.read()


def open_sheet(blob: bytes):
    return xlrd.open_workbook(file_contents=blob).sheet_by_index(0)


def month_key(value: Any) -> str | None:
    text = norm(value).upper()
    if "YTD" in text:
        return None
    for token, month in MONTHS.items():
        if re.search(rf"\b{token}\b", text):
            return month
    return None


def year_value(value: Any) -> int | None:
    if isinstance(value, (int, float)) and 1900 <= int(value) <= 2100:
        return int(value)
    text = norm(value)
    m = re.match(r"^(\d{4})(?:\s|\(|$)", text)
    if m:
        return int(m.group(1))
    return None


def find_header(sheet, *needles: str) -> int:
    wants = [n.lower() for n in needles]
    for row in range(min(sheet.nrows, 20)):
        values = " ".join(norm(sheet.cell_value(row, col)).lower() for col in range(sheet.ncols))
        if all(n in values for n in wants):
            return row
    return 4


def parse_production(blob: bytes) -> dict[str, dict[str, Any]]:
    sheet = open_sheet(blob)
    header = find_header(sheet, "domestic production", "import")
    sub = [norm(sheet.cell_value(header + 1, c)).strip() for c in range(sheet.ncols)]
    main = [norm(sheet.cell_value(header, c)).strip() for c in range(sheet.ncols)]

    total_cols = [i for i, label in enumerate(sub) if label.lower() == "total"]
    domestic_total_col = total_cols[0] if total_cols else 15
    total_import_col = next((i for i, label in enumerate(sub) if "total import" in label.lower()), 20)
    lng_col = next((i for i, label in enumerate(sub) if label.lower().startswith("lng")), 19)
    grand_total_col = next((i for i, label in enumerate(main) if label.lower() == "grand total"), sheet.ncols - 1)
    pipeline_cols = [
        i
        for i, label in enumerate(sub)
        if label.lower() in {"yadana", "yetakun", "zawtika"}
    ]

    rows: dict[str, dict[str, Any]] = {}
    current_year: int | None = None
    for row in range(header + 2, sheet.nrows):
        first = sheet.cell_value(row, 0)
        yr = year_value(first)
        if yr:
            current_year = yr
            continue
        mo = month_key(first)
        if not mo or current_year is None:
            continue

        month = f"{current_year}-{mo}"
        domestic = as_num(sheet.cell_value(row, domestic_total_col))
        lng = as_num(sheet.cell_value(row, lng_col))
        pipeline_parts = {sub[c].strip(): as_num(sheet.cell_value(row, c)) for c in pipeline_cols}
        pipeline_vals = [v for v in pipeline_parts.values() if v is not None]
        pipeline = sum(pipeline_vals) if pipeline_vals else None
        total_import = as_num(sheet.cell_value(row, total_import_col))
        grand_total = as_num(sheet.cell_value(row, grand_total_col))
        computed_supply = None
        if domestic is not None and lng is not None and pipeline is not None:
            computed_supply = domestic + lng + pipeline

        rows[month] = {
            "domestic_production_mmscfd": round_or_none(domestic),
            "pipeline_imports_mmscfd": round_or_none(pipeline),
            "lng_imports_mmscfd": round_or_none(lng),
            "total_import_mmscfd": round_or_none(total_import),
            "total_supply_mmscfd": round_or_none(computed_supply if computed_supply is not None else grand_total),
            "eppo_grand_total_mmscfd": round_or_none(grand_total),
            "pipeline_fields_mmscfd": {k: round_or_none(v) for k, v in pipeline_parts.items()},
        }
    return rows


def parse_consumption(blob: bytes) -> dict[str, dict[str, Any]]:
    sheet = open_sheet(blob)
    rows: dict[str, dict[str, Any]] = {}
    current_year: int | None = None

    # EPPO keeps these columns stable across current and historical files.
    COLS = {
        "egat_mmscfd": 1,
        "ipp_mmscfd": 2,
        "spp_mmscfd": 3,
        "electricity_mmscfd": 4,
        "industry_mmscfd": 5,
        "gsp_mmscfd": 6,
        "ngv_mmscfd": 7,
        "total_sector_demand_mmscfd": 8,
    }

    for row in range(sheet.nrows):
        first = sheet.cell_value(row, 0)
        yr = year_value(first)
        if yr:
            current_year = yr
            continue
        mo = month_key(first)
        if not mo or current_year is None:
            continue
        month = f"{current_year}-{mo}"
        rows[month] = {key: round_or_none(as_num(sheet.cell_value(row, col))) for key, col in COLS.items()}
    return rows


def merge_rows(production: dict[str, dict[str, Any]], consumption: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    months = sorted(set(production) | set(consumption))
    merged = []
    for month in months:
        p = production.get(month, {})
        c = consumption.get(month, {})
        supply = p.get("total_supply_mmscfd")
        demand = c.get("total_sector_demand_mmscfd")
        residual = supply - demand if supply is not None and demand is not None else None
        residual_pct = residual / demand * 100 if residual is not None and demand else None
        merged.append(
            {
                "month": month,
                "supply": p,
                "demand": c,
                "residual_mmscfd": round_or_none(residual),
                "residual_pct_demand": round_or_none(residual_pct),
            }
        )
    return merged


def main() -> None:
    blobs = {key: download(url) for key, url in FILES.items()}
    production = parse_production(blobs["production_history"])
    production.update(parse_production(blobs["production_current"]))
    consumption = parse_consumption(blobs["consumption_history"])
    consumption.update(parse_consumption(blobs["consumption_current"]))
    rows = merge_rows(production, consumption)
    if not rows:
        raise SystemExit("No Thailand gas balance rows parsed from EPPO files")

    data = {
        "schema": "thailand_gas_balance_v1",
        "country": "Thailand",
        "unit": "MMSCFD",
        "frequency": "monthly average daily flow",
        "latest_month": rows[-1]["month"],
        "row_count": len(rows),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "ngl_distribution_ignored": True,
        "sources": {
            "page": PAGE,
            "production_import": {
                "table": "EPPO Table 3.1-1: Production and Import of Natural Gas",
                "current": FILES["production_current"],
                "historical_monthly": FILES["production_history"],
            },
            "sector_demand": {
                "table": "EPPO Table 3.2-2: Consumption of Natural Gas by sector",
                "current": FILES["consumption_current"],
                "historical_monthly": FILES["consumption_history"],
            },
        },
        "components": {
            "supply": [
                "domestic_production_mmscfd",
                "pipeline_imports_mmscfd",
                "lng_imports_mmscfd",
                "total_supply_mmscfd",
            ],
            "demand": [
                "electricity_mmscfd",
                "industry_mmscfd",
                "gsp_mmscfd",
                "ngv_mmscfd",
                "total_sector_demand_mmscfd",
            ],
        },
        "rows": rows,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {OUT} · {len(rows)} rows · latest {rows[-1]['month']}")


if __name__ == "__main__":
    main()
