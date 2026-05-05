#!/usr/bin/env python3
"""Build India monthly gas balance and gas-price references from PPAC sources.

Source logic:
  - PPAC current/historical Natural Gas Production workbooks.
  - PPAC current/historical LNG Import workbooks.
  - PPAC current/historical Sectoral Consumption workbooks.
  - PPAC Domestic Natural Gas Price / Gas Price Ceiling PDFs.

No row is estimated. Blank PPAC months remain null. If historical workbooks are
not available during a scheduled run, existing committed history is preserved
and only newly available current rows are upserted.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen

import pandas as pd
from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_BALANCE = DATA_DIR / "india_gas_balance.json"
OUT_PRICES = DATA_DIR / "india_gas_prices.json"

PPAC = "https://ppac.gov.in"
PRODUCTION_PAGE = f"{PPAC}/natural-gas/production"
IMPORT_PAGE = f"{PPAC}/natural-gas/import"
SECTORAL_PAGE = f"{PPAC}/natural-gas/sectoral-consumption"
GAS_PRICE_PAGE = f"{PPAC}/natural-gas/gas-price"
PRODUCTION_AJAX = f"{PPAC}/AjaxController/getGasProduction"

LOCAL_PPAC_DIR = Path(
    os.environ.get(
        "INDIA_PPAC_DIR",
        "/Users/iboumar/Desktop/LNG Database/India/ppac",
    )
)

MONTHS = [
    ("April", 4),
    ("May", 5),
    ("June", 6),
    ("July", 7),
    ("August", 8),
    ("September", 9),
    ("October", 10),
    ("November", 11),
    ("December", 12),
    ("January", 1),
    ("February", 2),
    ("March", 3),
]

SECTOR_MAP = {
    "Power": ["Power"],
    "CGD": ["CGD"],
    "Fertilizer": ["Fertilizer"],
    "Refinery": ["Refinery"],
    "Petrochem": ["Petrochemical"],
    "LPGShrink": ["LPG Shrinkage"],
    "SteelDRI": ["Sponge Iron/Steel"],
    "PipelineIC": ["I/C for P/L System"],
}
OTHER_LABELS = ["Agriculture", "Industrial", "Manufacturing", "Other/Misc"]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def request(url: str, data: bytes | None = None, timeout: int = 45) -> bytes:
    req = Request(
        url,
        data=data,
        headers={
            "User-Agent": "LNG-TradeOS India PPAC refresh",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_text(url: str, timeout: int = 45) -> str:
    return request(url, timeout=timeout).decode("utf-8", errors="replace")


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(request(url))
    return dest


def clean_num(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text or text in {"-", "—", "NA", "N/A", "nan"}:
            return None
        try:
            value = float(text)
        except ValueError:
            return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if not pd.notna(n):
        return None
    return round(n, 3)


def month_key(fy_start: int, month_num: int) -> str:
    year = fy_start if month_num >= 4 else fy_start + 1
    return f"{year}-{month_num:02d}"


def fy_start_from_sheet(name: str) -> int | None:
    m = re.search(r"(20\d{2})\s*[-–]\s*(?:20)?\d{2}", name)
    if m:
        return int(m.group(1))
    m = re.search(r"(20\d{2})", name)
    if m:
        return int(m.group(1))
    return None


def fy_start_from_df(df: pd.DataFrame) -> int | None:
    sample = " ".join("" if pd.isna(v) else str(v) for v in df.head(15).to_numpy().flatten())
    m = re.search(r"Financial Year\s+(20\d{2})\s*[-–]\s*(?:20)?\d{2}", sample, flags=re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(20\d{2})\s*[-–]\s*(?:20)?\d{2}\b", sample)
    if m:
        return int(m.group(1))
    for value in df.head(12).to_numpy().flatten():
        if isinstance(value, datetime):
            return value.year if value.month >= 4 else value.year - 1
    return None


def find_row(df: pd.DataFrame, labels: list[str], start: int = 0) -> int | None:
    wanted = [label.lower() for label in labels]
    for i in range(start, len(df)):
        text = " ".join("" if pd.isna(v) else str(v) for v in df.iloc[i].tolist()).lower()
        if all(label in text for label in wanted):
            return i
    return None


def find_header_row(df: pd.DataFrame) -> int | None:
    for i in range(len(df)):
        raw_vals = [v for v in df.iloc[i].tolist() if not pd.isna(v)]
        date_like = sum(1 for v in raw_vals if hasattr(v, "month") and hasattr(v, "year"))
        if date_like >= 6:
            return i
        vals = [str(v).strip().lower() for v in raw_vals]
        if "april" in vals and "march" in vals:
            return i
    return None


def month_columns(df: pd.DataFrame, header_row: int, fy_start: int) -> dict[str, int]:
    out: dict[str, int] = {}
    for col, val in enumerate(df.iloc[header_row].tolist()):
        text = "" if pd.isna(val) else str(val).strip()
        if isinstance(val, datetime):
            out[f"{val.year}-{val.month:02d}"] = col
            continue
        for name, month_num in MONTHS:
            if text.lower() == name.lower():
                out[month_key(fy_start, month_num)] = col
    return out


def row_values(df: pd.DataFrame, row_idx: int, cols: dict[str, int]) -> dict[str, float | None]:
    values: dict[str, float | None] = {}
    for month, col in cols.items():
        values[month] = clean_num(df.iat[row_idx, col] if col < df.shape[1] else None)
    return values


def parse_production_book(path: Path) -> tuple[dict[str, float | None], list[str]]:
    xls = pd.ExcelFile(path)
    rows: dict[str, float | None] = {}
    sources: list[str] = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=object)
        fy_start = fy_start_from_sheet(sheet) or fy_start_from_df(df)
        if fy_start is None:
            continue
        header = find_header_row(df)
        if header is None:
            continue
        net_row = find_row(df, ["Net Production"])
        search_start = net_row + 1 if net_row is not None else 0
        total_row = find_row(df, ["Total (A+B)"], start=search_start)
        if total_row is None and net_row is not None:
            total_row = net_row
        if total_row is None:
            continue
        rows.update(row_values(df, total_row, month_columns(df, header, fy_start)))
        sources.append(path.name + ":" + sheet)
    return rows, sources


def parse_lng_book(path: Path) -> tuple[dict[str, float | None], set[str], list[str]]:
    xls = pd.ExcelFile(path)
    rows: dict[str, float | None] = {}
    prorated: set[str] = set()
    sources: list[str] = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=object)
        fy_start = fy_start_from_sheet(sheet) or fy_start_from_df(df)
        if fy_start is None:
            continue
        header = find_header_row(df)
        value_row = find_row(df, ["MMSCM"])
        if header is None or value_row is None:
            continue
        vals = row_values(df, value_row, month_columns(df, header, fy_start))
        rows.update(vals)
        note = " ".join("" if pd.isna(v) else str(v) for v in df.to_numpy().flatten())
        if "prorat" in note.lower():
            for month, value in vals.items():
                if value is not None:
                    prorated.add(month)
        sources.append(path.name + ":" + sheet)
    return rows, prorated, sources


def label_row(df: pd.DataFrame, label: str) -> int | None:
    target = label.lower()
    for i in range(len(df)):
        first = "" if pd.isna(df.iat[i, 0]) else str(df.iat[i, 0]).strip().lower()
        if target == first or target in first:
            return i
    return None


def parse_sectoral_book(path: Path) -> tuple[dict[str, dict[str, float | None]], list[str]]:
    xls = pd.ExcelFile(path)
    rows: dict[str, dict[str, float | None]] = {}
    sources: list[str] = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=object)
        fy_start = fy_start_from_sheet(sheet) or fy_start_from_df(df)
        if fy_start is None:
            continue
        header = find_header_row(df)
        if header is None:
            continue
        cols = month_columns(df, header, fy_start)
        # Sector sheets use Domestic/RLNG/Total triples. Prefer the Total column.
        total_cols: dict[str, int] = {}
        for month, col in cols.items():
            total_cols[month] = col + 2 if col + 2 < df.shape[1] else col

        per_month: dict[str, dict[str, float | None]] = {m: {} for m in total_cols}
        for key, labels in SECTOR_MAP.items():
            row_idx = None
            for label in labels:
                row_idx = label_row(df, label)
                if row_idx is not None:
                    break
            if row_idx is None:
                continue
            for month, col in total_cols.items():
                per_month[month][key] = clean_num(df.iat[row_idx, col])

        other_rows = [label_row(df, label) for label in OTHER_LABELS]
        other_rows = [idx for idx in other_rows if idx is not None]
        for month, col in total_cols.items():
            parts = [clean_num(df.iat[idx, col]) for idx in other_rows]
            vals = [v for v in parts if v is not None]
            per_month[month]["Other"] = round(sum(vals), 3) if vals else None

        total_idx = label_row(df, "Total")
        if total_idx is not None:
            for month, col in total_cols.items():
                per_month[month]["total_demand"] = clean_num(df.iat[total_idx, col])

        for month, values in per_month.items():
            if any(v is not None for v in values.values()):
                rows.setdefault(month, {}).update(values)
        sources.append(path.name + ":" + sheet)
    return rows, sources


def first_existing(patterns: list[str]) -> Path | None:
    for pattern in patterns:
        hits = sorted(LOCAL_PPAC_DIR.glob(pattern))
        if hits:
            return hits[-1]
    return None


def existing_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def existing_balance_rows() -> dict[str, dict[str, Any]]:
    payload = existing_payload(OUT_BALANCE) or {}
    rows = payload.get("rows") or []
    return {row["month"]: dict(row) for row in rows if row.get("month")}


def first_nonblank(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def current_fy_for_today() -> str:
    today = datetime.now(timezone.utc)
    start = today.year if today.month >= 4 else today.year - 1
    return f"{start}-{start + 1}"


def selected_fy_from_production_page() -> str:
    try:
        html = fetch_text(PRODUCTION_PAGE)
        m = re.search(r'<option value="([^"]+)"\s+selected>', html, flags=re.I)
        if m:
            return m.group(1)
    except Exception:
        pass
    return current_fy_for_today()


def scrape_current_production_file(tmp: Path) -> Path | None:
    try:
        data = f"financialYear={selected_fy_from_production_page()}&reportBy=4&pageId=170".encode()
        payload = json.loads(request(PRODUCTION_AJAX, data=data).decode("utf-8"))
        result = payload.get("result") or {}
        for item in result.values():
            url = item.get("file_name")
            if url and "NG-C-Production" in url:
                return download(url, tmp / Path(url).name)
    except Exception as exc:
        print(f"[india] production current scrape gap: {exc}")
    return None


def scrape_current_link(page_url: str, token: str, tmp: Path) -> Path | None:
    try:
        html = fetch_text(page_url)
        links = re.findall(r'href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, flags=re.I | re.S)
        for href, label_html in links:
            label = re.sub(r"<[^>]+>", " ", label_html)
            label = " ".join(label.split())
            if token in href or token in label:
                url = urljoin(PPAC, href)
                return download(url, tmp / Path(url).name)
    except Exception as exc:
        print(f"[india] current link gap for {token}: {exc}")
    return None


def month_from_valid_text(text: str) -> tuple[str, str] | None:
    months = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    lower = text.lower().replace("sept ", "september ")
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4}).*?"
        r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})",
        lower,
        flags=re.S,
    )
    if m:
        start = f"{int(m.group(2))}-{months[m.group(1)]:02d}"
        end = f"{int(m.group(4))}-{months[m.group(3)]:02d}"
        return start, end
    m = re.search(r"(apr(?:il)?)[-_ ]+(?:sep|september)[-_ ]+20(\d{2})", lower)
    if m:
        year = 2000 + int(m.group(2))
        return f"{year}-04", f"{year}-09"
    m = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+20(\d{2})", lower)
    if m:
        month = months[m.group(1)]
        year = 2000 + int(m.group(2))
        return f"{year}-{month:02d}", f"{year}-{month:02d}"
    return None


def ocr_pdf(path: Path) -> str:
    if not shutil.which("tesseract"):
        return ""
    with tempfile.TemporaryDirectory(prefix="india_pdf_ocr_") as td:
        tmp = Path(td)
        img = tmp / "page.png"
        if shutil.which("pdftoppm"):
            subprocess.run(
                ["pdftoppm", "-png", "-singlefile", "-r", "180", str(path), str(tmp / "page")],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif shutil.which("sips"):
            subprocess.run(
                ["sips", "-s", "format", "png", str(path), "--out", str(img)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        if not img.exists():
            return ""
        proc = subprocess.run(
            ["tesseract", str(img), "stdout", "-l", "eng"],
            check=False,
            capture_output=True,
            text=True,
        )
        return proc.stdout or ""


def pdf_text(path: Path) -> str:
    try:
        reader = PdfReader(str(path))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        text = ""
    if len(text.strip()) < 80:
        text = ocr_pdf(path)
    return text


def parse_price_pdf(path: Path, source_url: str | None = None, title: str | None = None) -> dict[str, Any] | None:
    text = pdf_text(path)
    blob = " ".join([title or "", path.name, text]).replace("\n", " ")
    price_match = re.search(r"US\S?\s*\$?\s*([0-9]+(?:\.[0-9]+)?)\s*/\s*MMBTU", blob, flags=re.I)
    if not price_match:
        return None
    price = float(price_match.group(1))
    validity = month_from_valid_text(blob)
    if not validity:
        return None
    source_hint = " ".join([title or "", path.name]).lower()
    kind = "ceiling" if "ceiling" in source_hint else "apm"
    published = None
    m = re.search(r"Dated:\s*(\d{1,2})[./-](\d{1,2})[./-](\d{4})", blob, flags=re.I)
    if m:
        published = f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return {
        "kind": kind,
        "valid_from": validity[0],
        "valid_to": validity[1],
        "price_usd_mmbtu": price,
        "basis": "GCV",
        "source_file": path.name,
        "source_url": source_url,
        "published_date": published,
    }


def gas_price_links() -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for page in range(1, 4):
        try:
            html = fetch_text(f"{GAS_PRICE_PAGE}?page={page}")
        except Exception:
            continue
        for m in re.finditer(r'<h3[^>]*>.*?<span title="([^"]+)".*?</h3>.*?href="([^"]*download\.php\?file=gasprice/[^"]+)"', html, flags=re.I | re.S):
            title, href = m.group(1), m.group(2)
            links.append((title, urljoin(PPAC, href)))
        for m in re.finditer(r'href="([^"]*download\.php\?file=menu/[^"]+(?:Dom_Gas|Gas_Price)[^"]+\.pdf)" title="([^"]+)"', html, flags=re.I):
            href, title = m.group(1), m.group(2)
            links.append((title, urljoin(PPAC, href)))
    # De-dupe while preserving order.
    seen = set()
    out = []
    for title, url in links:
        url = quote(url, safe=":/?=&%")
        if url in seen:
            continue
        seen.add(url)
        out.append((title, url))
    return out


def build_balance() -> dict[str, Any]:
    rows = existing_balance_rows()
    source_files: list[str] = []
    source_gaps: list[str] = []

    hist_prod = first_existing(["Gas Balance Data/*NG-H-Production*.xls*"])
    hist_lng = first_existing(["Gas Balance Data/*NG-H-LNG-Import*.xls*"])
    hist_sector = first_existing(["Gas Balance Data/*NG-H-Sectoral*.xls*"])

    if hist_prod:
        prod, src = parse_production_book(hist_prod)
        for month, value in prod.items():
            rows.setdefault(month, {})["domesticProductionMmscm"] = value
        source_files.extend(src)
    else:
        source_gaps.append("historical production workbook unavailable; preserved committed history")

    if hist_lng:
        lng, prorated, src = parse_lng_book(hist_lng)
        for month, value in lng.items():
            row = rows.setdefault(month, {})
            row["rlngImportsMmscm"] = value
            if month in prorated:
                row["prorated"] = True
        source_files.extend(src)
    else:
        source_gaps.append("historical LNG import workbook unavailable; preserved committed history")

    if hist_sector:
        sector, src = parse_sectoral_book(hist_sector)
        for month, values in sector.items():
            rows.setdefault(month, {}).update(values)
        source_files.extend(src)
    else:
        source_gaps.append("historical sectoral workbook unavailable; preserved committed history")

    with tempfile.TemporaryDirectory(prefix="india_ppac_") as td:
        tmp = Path(td)
        current_prod = scrape_current_production_file(tmp) or first_existing(["Gas Balance Data/*NG-C-Production*.xls*"])
        current_lng = scrape_current_link(IMPORT_PAGE, "NG-C-LNG-Import", tmp) or first_existing(["Gas Balance Data/*NG-C-LNG-Import*.xls*"])
        current_sector = scrape_current_link(SECTORAL_PAGE, "NG-C-Sectoral-Consumption", tmp) or first_existing(["Gas Balance Data/*NG-C-Sectoral*.xls*"])

        if current_prod:
            prod, src = parse_production_book(current_prod)
            for month, value in prod.items():
                rows.setdefault(month, {})["domesticProductionMmscm"] = value
            source_files.extend(src)
        else:
            source_gaps.append("current production source unavailable")

        if current_lng:
            lng, prorated, src = parse_lng_book(current_lng)
            for month, value in lng.items():
                row = rows.setdefault(month, {})
                row["rlngImportsMmscm"] = value
                if month in prorated:
                    row["prorated"] = True
            source_files.extend(src)
        else:
            source_gaps.append("current LNG import source unavailable")

        if current_sector:
            sector, src = parse_sectoral_book(current_sector)
            for month, values in sector.items():
                rows.setdefault(month, {}).update(values)
            source_files.extend(src)
        else:
            source_gaps.append("current sectoral source unavailable")

    final_rows: list[dict[str, Any]] = []
    for month in sorted(rows):
        row = dict(rows[month])
        prod = clean_num(first_nonblank(row, ["domesticProductionMmscm", "prod"]))
        rlng = clean_num(first_nonblank(row, ["rlngImportsMmscm", "rlng"]))
        total_supply = round(prod + rlng, 3) if prod is not None and rlng is not None else None
        out = {
            "month": month,
            "prod": prod,
            "rlng": rlng,
            "domesticProductionMmscm": prod,
            "rlngImportsMmscm": rlng,
            "total_supply": total_supply,
            "totalSupplyMmscm": total_supply,
        }
        for key in ["Power", "CGD", "Fertilizer", "Refinery", "Petrochem", "LPGShrink", "SteelDRI", "PipelineIC", "Other"]:
            out[key] = clean_num(row.get(key))
        total_demand = clean_num(first_nonblank(row, ["total_demand", "totalDemandMmscm"]))
        out["total_demand"] = total_demand
        out["totalDemandMmscm"] = total_demand
        if total_supply is not None and total_demand is not None:
            out["balanceMmscm"] = round(total_supply - total_demand, 3)
        else:
            out["balanceMmscm"] = None
        if row.get("prorated"):
            out["provisional"] = True
            out["note"] = "PPAC provisional/prorated source row"
        if any(v is not None for k, v in out.items() if k not in {"month", "balanceMmscm"}):
            final_rows.append(out)

    latest = final_rows[-1]["month"] if final_rows else None
    return {
        "schema": "india_gas_balance_v1",
        "country": "India",
        "status": "parsed" if final_rows else "source_gap",
        "cachedAt": now_iso(),
        "latest_month": latest,
        "unit": "MMSCM/month",
        "sourceUrls": {
            "production": PRODUCTION_PAGE,
            "lngImports": IMPORT_PAGE,
            "sectoralConsumption": SECTORAL_PAGE,
        },
        "sourceFiles": sorted(set(source_files)),
        "sourceGaps": source_gaps,
        "rows": final_rows,
        "notes": [
            "No missing PPAC month is estimated.",
            "Domestic production uses PPAC net production / natural gas available for sale.",
            "RLNG imports use PPAC LNG imports in MMSCM.",
            "Other demand is Agriculture + Industrial + Manufacturing + Other/Misc where PPAC provides the split.",
            "Blank or zero formula months in current-year workbooks stay null.",
        ],
    }


def build_prices() -> dict[str, Any]:
    records: dict[tuple[str, str, str], dict[str, Any]] = {}
    source_gaps: list[str] = []

    existing = existing_payload(OUT_PRICES) or {}
    for rec in existing.get("records") or []:
        source_hint = " ".join(str(rec.get(k) or "") for k in ("source_file", "source_url")).lower()
        if rec.get("kind") == "ceiling" and "ceiling" not in source_hint:
            continue
        if rec.get("kind") == "apm" and "ceiling" in source_hint:
            continue
        if rec.get("kind") == "ceiling" and rec.get("valid_from") == rec.get("valid_to") and "sep" in source_hint:
            continue
        key = (rec.get("kind"), rec.get("valid_from"), rec.get("valid_to"))
        if all(key):
            records[key] = rec

    local_dir = LOCAL_PPAC_DIR / "Domestic Gas Prices"
    if local_dir.exists():
        for path in sorted(local_dir.glob("*.pdf")):
            rec = parse_price_pdf(path)
            if rec:
                records[(rec["kind"], rec["valid_from"], rec["valid_to"])] = rec

    with tempfile.TemporaryDirectory(prefix="india_ppac_price_") as td:
        tmp = Path(td)
        links = gas_price_links()
        if not links:
            source_gaps.append("PPAC gas price page unavailable; preserved committed/local price records")
        for title, url in links[:30]:
            try:
                dest = download(url, tmp / Path(url.split("file=")[-1]).name)
                rec = parse_price_pdf(dest, source_url=url, title=title)
                if rec:
                    records[(rec["kind"], rec["valid_from"], rec["valid_to"])] = rec
            except Exception as exc:
                source_gaps.append(f"price pdf gap: {title}: {exc}")

    out_records = sorted(records.values(), key=lambda r: (r["valid_from"], r["kind"]))
    latest_apm = next((r for r in reversed(out_records) if r["kind"] == "apm"), None)
    latest_ceiling = next((r for r in reversed(out_records) if r["kind"] == "ceiling"), None)
    return {
        "schema": "india_gas_prices_v1",
        "country": "India",
        "status": "parsed" if out_records else "source_gap",
        "cachedAt": now_iso(),
        "latest_apm_month": latest_apm["valid_to"] if latest_apm else None,
        "latest_ceiling_period": f"{latest_ceiling['valid_from']} to {latest_ceiling['valid_to']}" if latest_ceiling else None,
        "unit": "$/MMBtu",
        "basis": "GCV",
        "sourceUrl": GAS_PRICE_PAGE,
        "records": out_records,
        "sourceGaps": source_gaps,
        "notes": [
            "Domestic/APM gas price and gas price ceiling are valid only inside the PPAC notification period.",
            "Expired price records are not rolled forward as current values.",
            "Scanned PPAC PDFs require OCR when text extraction is blank.",
        ],
    }


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    balance = build_balance()
    prices = build_prices()
    OUT_BALANCE.write_text(json.dumps(balance, indent=2, ensure_ascii=False) + "\n")
    OUT_PRICES.write_text(json.dumps(prices, indent=2, ensure_ascii=False) + "\n")
    print(f"India gas balance rows: {len(balance['rows'])}, latest {balance['latest_month']}")
    print(f"India gas price records: {len(prices['records'])}, latest APM {prices['latest_apm_month']}")


if __name__ == "__main__":
    main()
