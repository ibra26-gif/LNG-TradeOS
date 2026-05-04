#!/usr/bin/env python3
"""Build Taiwan monthly gas balance from MOEA Energy Administration open data.

Source logic:
  - Table 9-1: NG2 grid, regasified/imported LNG.
  - Table 9-3: NG1 grid, indigenous gas plus calorific-value adjusted LNG.

The open-data CSV set 135 exposes the monthly NG1 and NG2 components in one
file. We still compute the combined balance by explicitly adding NG1 + NG2
components. No balancing item is estimated.
"""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "taiwan_gas_balance.json"

SOURCE_PAGE = "https://www.moeaboe.gov.tw/ECW/populace/web_book/WebReports.aspx?book=M_CH&menu_id=142"
OPEN_DATA_URL = "https://www.moeaea.gov.tw/ECW/populace/opendata/wHandOpenData_File.ashx?set_id=135"
LEGACY_OPEN_DATA_URL = "https://www.moeaboe.gov.tw/ECW/populace/opendata/wHandOpenData_File.ashx?set_id=135"


def source_map(status: str = "source_identified", error: str | None = None) -> list[dict[str, Any]]:
    return [
        {
            "item": "Taiwan monthly natural-gas balance",
            "balanceUse": "Combined Taiwan grid gas supply and demand",
            "source": "Taiwan Ministry of Economic Affairs, Energy Administration monthly energy statistics",
            "sourceUrl": SOURCE_PAGE,
            "downloadUrl": OPEN_DATA_URL,
            "pull": "Monthly open-data CSV set_id=135. Table 9-1 fields cover NG2 regasified LNG; Table 9-3 fields cover NG1 indigenous gas plus LNG calorific-value adjustment.",
            "fields": ["date", "unit", "NG1 supply/demand components", "NG2 supply/demand components"],
            "status": status,
            "wiring": "csv_parser_wired" if status == "wired" else "csv_parser_waiting",
            **({"error": error} if error else {}),
        },
        {
            "item": "Gassco UMM",
            "balanceUse": "Not used for Taiwan",
            "source": "Gassco UMM",
            "sourceUrl": "https://umm.gassco.no",
            "pull": "Norwegian maintenance/outage notices. Useful for Europe/Norway context, not Taiwan gas balance.",
            "status": "not_used",
            "wiring": "excluded_wrong_geography",
        },
    ]


def empty_payload(status: str, error: str | None = None) -> dict[str, Any]:
    return {
        "schema": "taiwan_gas_balance_v1",
        "country": "Taiwan",
        "status": status,
        "latest_month": None,
        "cachedAt": datetime.now(timezone.utc).isoformat(),
        "sourcePage": SOURCE_PAGE,
        "sourceDataUrl": OPEN_DATA_URL,
        "sourceUnit": "千立方公尺",
        "unit": "bcm/month",
        "rows": [],
        "sourceMap": source_map("source_gap", error),
        "notes": [
            "No fallback rows are estimated.",
            "Table 9-1 NG2 and Table 9-3 NG1 components must be added by component.",
            "Indigenous production is only included in the NG1 grid.",
        ],
        **({"error": error} if error else {}),
    }


def fetch_text(url: str, timeout: int = 60) -> str:
    req = Request(url, headers={"User-Agent": "LNG-TradeOS Taiwan gas balance refresh"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return raw.decode("utf-8-sig")


def download_source() -> tuple[str, str]:
    last_error: Exception | None = None
    for url in (OPEN_DATA_URL, LEGACY_OPEN_DATA_URL):
        try:
            return fetch_text(url), url
        except Exception as exc:  # pragma: no cover - only hit on network/source outage
            last_error = exc
    raise RuntimeError(str(last_error or "Taiwan MOEA source unavailable"))


def num(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "—", "NA", "N/A"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def bcm_from_thousand_m3(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value / 1_000_000, 6)


def add(*values: float | None) -> float | None:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return round(sum(vals), 6)


def find_value(row: dict[str, str], *tokens: str) -> float | None:
    for key, value in row.items():
        clean = key.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
        if all(token in clean for token in tokens):
            return num(value)
    return None


def month_key(value: str) -> str | None:
    text = str(value or "").strip()
    m = re.match(r"^(\d{4})(\d{2})$", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.match(r"^(\d{2,3})(\d{2})$", text)
    if m:
        # Some Taiwan data uses ROC years. Four-digit years are handled above.
        return f"{int(m.group(1)) + 1911}-{m.group(2)}"
    m = re.match(r"^(\d{4})[/-](\d{1,2})", text)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"
    return None


def parse_csv(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(text))
    for raw in reader:
        month = month_key(raw.get("日期(年/月)") or raw.get("日期") or raw.get("年月") or "")
        if not month:
            continue
        unit = (raw.get("單位") or "").strip()
        if "千立方公尺" not in unit:
            continue

        ng1_supply = bcm_from_thousand_m3(find_value(raw, "自產天然氣可供市場銷售之NG1", "小計"))
        ng1_domestic = bcm_from_thousand_m3(find_value(raw, "自產天然氣可供市場銷售之NG1", "自產量"))
        ng1_lng_adj = bcm_from_thousand_m3(find_value(raw, "自產天然氣可供市場銷售之NG1", "LNG", "調整量"))
        ng1_power = bcm_from_thousand_m3(find_value(raw, "自產天然氣轉變投入", "發電"))
        ng1_final_total = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "合計"))
        ng1_energy_own = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "能源部門自用"))
        ng1_industry = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "工業"))
        ng1_transport = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "運輸"))
        ng1_agri = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "農業"))
        ng1_service = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "服務業"))
        ng1_residential = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "住宅"))
        ng1_non_energy = bcm_from_thousand_m3(find_value(raw, "自產天然氣NG1消費", "非能源"))

        ng2_physical_import = bcm_from_thousand_m3(find_value(raw, "液化天然氣進口量"))
        ng2_supply = bcm_from_thousand_m3(find_value(raw, "液化天然氣可供市場銷售"))
        ng2_transform_total = bcm_from_thousand_m3(find_value(raw, "液化天然氣轉變投入", "小計"))
        ng2_refinery = bcm_from_thousand_m3(find_value(raw, "液化天然氣轉變投入", "煉油"))
        ng2_power = bcm_from_thousand_m3(find_value(raw, "液化天然氣轉變投入", "發電"))
        ng2_final_total = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "小計"))
        ng2_energy_own = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "能源部門自用"))
        ng2_industry = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "工業"))
        ng2_transport = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "運輸"))
        ng2_agri = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "農業"))
        ng2_service = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "服務業"))
        ng2_residential = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "住宅"))
        ng2_non_energy = bcm_from_thousand_m3(find_value(raw, "液化天然氣NG2消費", "非能源"))

        total_supply = add(ng1_supply, ng2_supply)
        total_transform = add(ng1_power, ng2_transform_total)
        total_final = add(ng1_final_total, ng2_final_total)
        total_demand = add(total_transform, total_final)
        residual = total_supply - total_demand if total_supply is not None and total_demand is not None else None

        rows.append(
            {
                "month": month,
                "sourceUnit": unit,
                "totalSupplyBcm": total_supply,
                "domesticProductionBcm": ng1_domestic,
                "lngAdjustedSupplyBcm": add(ng1_lng_adj, ng2_supply),
                "physicalLngImportsBcm": ng2_physical_import,
                "totalTransformationBcm": total_transform,
                "powerDemandBcm": add(ng1_power, ng2_power),
                "refineryDemandBcm": ng2_refinery,
                "totalFinalConsumptionBcm": total_final,
                "energyOwnUseBcm": add(ng1_energy_own, ng2_energy_own),
                "industryDemandBcm": add(ng1_industry, ng2_industry),
                "transportDemandBcm": add(ng1_transport, ng2_transport),
                "agricultureDemandBcm": add(ng1_agri, ng2_agri),
                "serviceDemandBcm": add(ng1_service, ng2_service),
                "residentialDemandBcm": add(ng1_residential, ng2_residential),
                "residentialCommercialDemandBcm": add(ng1_service, ng2_service, ng1_residential, ng2_residential),
                "nonEnergyDemandBcm": add(ng1_non_energy, ng2_non_energy),
                "totalDemandBcm": total_demand,
                "residualBcm": round(residual, 6) if residual is not None else None,
                "ng1": {
                    "supplyBcm": ng1_supply,
                    "domesticProductionBcm": ng1_domestic,
                    "lngAdjustmentBcm": ng1_lng_adj,
                    "powerDemandBcm": ng1_power,
                    "finalConsumptionBcm": ng1_final_total,
                },
                "ng2": {
                    "physicalLngImportsBcm": ng2_physical_import,
                    "marketableSupplyBcm": ng2_supply,
                    "totalTransformationBcm": ng2_transform_total,
                    "powerDemandBcm": ng2_power,
                    "refineryDemandBcm": ng2_refinery,
                    "finalConsumptionBcm": ng2_final_total,
                },
                "coverage": {
                    "boeTable91Ng2": ng2_supply is not None or ng2_physical_import is not None,
                    "boeTable93Ng1": ng1_supply is not None or ng1_domestic is not None,
                    "noEstimatedBalancingItem": True,
                },
            }
        )
    rows.sort(key=lambda r: r["month"])
    return rows


def build() -> dict[str, Any]:
    try:
        text, used_url = download_source()
        rows = parse_csv(text)
    except Exception as exc:  # pragma: no cover - network/source outage
        return empty_payload("source_gap", str(exc))

    if not rows:
        return empty_payload("source_gap", "MOEA CSV returned no monthly Taiwan gas rows")

    latest = rows[-1]
    return {
        "schema": "taiwan_gas_balance_v1",
        "country": "Taiwan",
        "status": "parsed",
        "latest_month": latest["month"],
        "cachedAt": datetime.now(timezone.utc).isoformat(),
        "sourcePage": SOURCE_PAGE,
        "sourceDataUrl": used_url,
        "sourceUnit": "千立方公尺",
        "unit": "bcm/month",
        "conversion": {
            "bcm_from_thousand_m3": "value / 1,000,000",
            "mcm_d": "bcm/month * 1000 / days_in_month",
            "mtpm": "bcm/month * 0.735 LNG-equivalent",
        },
        "notes": [
            "Combined rows are computed by adding Table 9-3 NG1 components and Table 9-1 NG2 components.",
            "Indigenous production is only counted inside the NG1 grid.",
            "Physical LNG imports are shown separately; grid supply uses NG1 LNG adjustment plus NG2 marketable gas.",
            "Residual equals total supply minus transformation plus final consumption; no statistical difference is invented.",
        ],
        "sourceMap": source_map("wired"),
        "rows": rows,
    }


def main() -> None:
    OUT.write_text(json.dumps(build(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    data = json.loads(OUT.read_text(encoding="utf-8"))
    print(f"Taiwan gas balance {data.get('status')} rows={len(data.get('rows', []))} latest={data.get('latest_month')}")


if __name__ == "__main__":
    main()
