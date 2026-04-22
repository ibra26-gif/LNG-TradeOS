#!/usr/bin/env python3
"""
colombia_bmc_fetch.py — monthly Colombia gas balance scraper for LNG TradeOS.

Source: BMC monthly reports — https://www.bmcbec.com.co/informes/informes-mensuales

Each PDF gives ONE month's snapshot with the same structure:
  - Page 2  : supply by field (GBTUD avg, current month)
  - Page 4  : supply 13-month rolling totals (Suministro Prom)
  - Page 24 : demand by sector (current month, TOTAL Nacional row)

The script discovers the latest published PDF from the BMC index, extracts
the three slices, appends a long-form row block to data/colombia_gas_balance.csv,
then commits + pushes if changes exist. Designed for monthly cron on the 21st.

CSV schema (long, accumulating one block per month):
  month,kind,id,gbtud
  2026-02,supply,piedemonte_llanero,414
  2026-02,supply,gibraltar,22
  ...
  2026-02,demand,industrial,173
  ...

The API at /api/colombia-bmc.js maps field/sector ids to CO_D rows + converts
GBTUD → MCM/D using 1 GBTUD ≈ 0.0283 MCM/D.

Backfill mode:
  python3 colombia_bmc_fetch.py --backfill 12     # last 12 PDFs
Single-PDF mode:
  python3 colombia_bmc_fetch.py --pdf <url>
Default (latest only):
  python3 colombia_bmc_fetch.py
"""

# Use postponed annotation evaluation so newer typing syntax (X | Y, list[T])
# works on the system python (Apple's /usr/bin/python3 is 3.9).
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import pdfplumber

REPO   = Path(__file__).resolve().parent.parent
CSV    = REPO / "data" / "colombia_gas_balance.csv"
INDEX  = "https://www.bmcbec.com.co/informes/informes-mensuales"
SITE   = "https://www.bmcbec.com.co"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA, "Accept": "*/*"}

ES_MONTHS = {
    "enero":1, "febrero":2, "marzo":3, "abril":4, "mayo":5, "junio":6,
    "julio":7, "agosto":8, "septiembre":9, "octubre":10, "noviembre":11, "diciembre":12,
}

# Field name → stable lowercase id used in the CSV
FIELD_ID = {
    "Piedemonte llanero":         "piedemonte_llanero",
    "Gibraltar":                  "gibraltar",
    "Istanbul":                   "istanbul",
    "Otros campos interior":      "otros_interior",
    "Arrecife":                   "arrecife",
    "Bloque VIM 5, 21 y esperanza": "bloque_vim",
    "Ballena y Chuchupa":         "ballena_chuchupa",
    "Bonga/Mamey":                "bonga_mamey",
    "Bullerengue":                "bullerengue",
    "Mágico":                     "magico",
    "Otros campos costa":         "otros_costa",
    "Planta Regasificación Cartagena***": "lng_cartagena",
    "Planta Regasificación Cartagena":    "lng_cartagena",
}

# Page 24 sector header order (left → right in the COSTA/INTERIOR/TOTAL table)
P24_SECTORS = [
    "termoelectrica", "industrial", "residencial", "refineria",
    "gnv", "comercial", "petroquimica", "petrolero", "compresoras",
]


# ─── Discovery ───────────────────────────────────────────────────────────────
def fetch_index_html() -> str:
    req = urllib.request.Request(INDEX, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="replace")


def discover_pdfs(html: str) -> list[tuple[str, str]]:
    """Return [(yyyy_mm_label, full_url), ...] sorted newest-first."""
    seen = []
    pat = re.compile(r'href="(/sites/default/files/[^"]+\.pdf)"')
    for m in pat.finditer(html):
        path = m.group(1)
        # Decode %20
        decoded = urllib.parse.unquote(path)
        # Skip the FAQ pdf
        if "Preguntas-Frecuentes" in decoded:
            continue
        # Try to extract the month + year from the filename, e.g.
        #   "Informe Mensual 2026 Febrero.pdf" or "Informe Mensual 2025 diciembre.pdf"
        fn = decoded.split("/")[-1]
        match = re.search(r"(\d{4})\s+([A-Za-zñáéíóúÁÉÍÓÚ]+)", fn)
        if not match:
            continue
        yr = int(match.group(1))
        mo_name = match.group(2).lower()
        mo_name = mo_name.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
        mo = ES_MONTHS.get(mo_name)
        if mo is None:
            # try 3-letter abbreviation
            for full, num in ES_MONTHS.items():
                if full.startswith(mo_name) and len(mo_name) >= 3:
                    mo = num
                    break
        if mo is None:
            continue
        label = f"{yr:04d}-{mo:02d}"
        url = SITE + path
        seen.append((label, url))
    # Dedupe by label, keep the first (newest URL discovery order is usually correct)
    out = {}
    for lbl, url in seen:
        out.setdefault(lbl, url)
    return sorted(out.items(), key=lambda x: x[0], reverse=True)


# ─── Parsing ─────────────────────────────────────────────────────────────────
NUM = r"-?[\d,]+(?:\.\d+)?"


def to_num(s: str | None) -> float | None:
    if s is None:
        return None
    s = s.strip().replace(",", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_page2_supply(page) -> dict[str, float]:
    """Page 2 — supply by field. Returns {field_id: gbtud_total}.
    The PDF has rotated region labels ("Interior" / "Costa") that pdfplumber
    inserts as stray characters at line start (e.g. "t Istanbul ..."), so we
    locate labels via substring not startswith.
    """
    out = {}
    txt = page.extract_text() or ""
    for line in txt.split("\n"):
        for label, fid in FIELD_ID.items():
            idx = line.find(label)
            if idx >= 0:
                rest = line[idx + len(label):]
                nums = re.findall(NUM, rest)
                # Page 2 columns: Potencial | Entregado SNT | Entregado a otros | Total | %
                # We want column "Total" (index 3 of nums)
                if len(nums) >= 4:
                    val = to_num(nums[3])
                    if val is not None:
                        out[fid] = val
                break
    return out


def parse_page4_history(page) -> dict[str, float]:
    """Page 4 — 13-month rolling. Returns {YYYY-MM: suministro_prom_gbtud}."""
    txt = page.extract_text() or ""
    lines = txt.split("\n")
    header = None
    for line in lines:
        if "VARIABLE" in line and "GBTUD" in line:
            header = line
            break
    if not header:
        return {}
    months = re.findall(r"([A-Z]{3})(\d{2})", header)
    if len(months) < 12:
        return {}

    # Spanish month abbrev → 1-12
    SPA_ABBR = {
        "ENE":1,"FEB":2,"MAR":3,"ABR":4,"MAY":5,"JUN":6,
        "JUL":7,"AGO":8,"SEP":9,"OCT":10,"NOV":11,"DIC":12,
    }
    period_keys = []
    for ab, yy in months:
        m = SPA_ABBR.get(ab)
        if not m:
            continue
        yr = 2000 + int(yy)
        period_keys.append(f"{yr:04d}-{m:02d}")

    # Find Suministro Prom row — 13 numbers after the label
    out = {}
    for i, line in enumerate(lines):
        if "Suministro" in line and "Prom" in line:
            nums = re.findall(NUM, line)
            if len(nums) >= len(period_keys):
                for k, v in zip(period_keys, nums[: len(period_keys)]):
                    val = to_num(v)
                    if val is not None:
                        out[k] = val
            break
    return out


def parse_page24_demand(page) -> dict[str, float]:
    """Page 24 — demand by sector. Returns {sector_id: gbtud}.

    Page 24 layout in the text is:
        COSTA      162 60 30 70 9 8 8 0 1
        INTERIOR     1 113 143 65 49 59 0 32 3
        TOTAL
        163 173 173 135 58 67 8 32 4
        Nacional
        % Segmento ...

    So we find the line equal to "TOTAL" and take the NEXT line as the
    9-number Nacional row. Also confirmed by checking that the line after
    contains exactly "Nacional" (sanity check to avoid grabbing a "TOTAL Potencial..." row).
    """
    txt = page.extract_text() or ""
    out = {}
    lines = txt.split("\n")
    for i, line in enumerate(lines):
        if line.strip() == "TOTAL" and i + 2 < len(lines):
            nums_line = lines[i + 1]
            tail_line = lines[i + 2]
            if "Nacional" not in tail_line:
                continue
            nums = re.findall(NUM, nums_line)
            if len(nums) >= len(P24_SECTORS):
                for sid, v in zip(P24_SECTORS, nums[: len(P24_SECTORS)]):
                    val = to_num(v)
                    if val is not None:
                        out[sid] = val
                break
    return out


# ─── PDF download ────────────────────────────────────────────────────────────
def download_pdf(url: str, dest: Path) -> Path:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as r:
        dest.write_bytes(r.read())
    return dest


def scrape_one(url: str, label: str, tmpdir: Path) -> list[tuple[str, str, str, float]]:
    """Returns rows [(month, kind, id, gbtud), ...] for this single PDF."""
    pdf_path = tmpdir / f"bmc_{label}.pdf"
    if not pdf_path.exists():
        download_pdf(url, pdf_path)
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) < 24:
            print(f"  WARN: PDF for {label} has only {len(pdf.pages)} pages, skipping.")
            return rows
        # Supply per field (current month)
        sup = parse_page2_supply(pdf.pages[1])
        for fid, val in sup.items():
            rows.append((label, "supply", fid, val))
        # Supply rolling 13mo (newest = current)
        hist = parse_page4_history(pdf.pages[3])
        for ym, val in hist.items():
            # Tag historical totals as "supply_total" so they don't collide
            rows.append((ym, "supply_total", "national", val))
        # Demand per sector (current month)
        dem = parse_page24_demand(pdf.pages[23])
        for sid, val in dem.items():
            rows.append((label, "demand", sid, val))
    return rows


# ─── CSV merge ───────────────────────────────────────────────────────────────
def load_existing() -> dict[tuple[str, str, str], float]:
    if not CSV.exists():
        return {}
    out = {}
    for line in CSV.read_text(encoding="utf-8").strip().split("\n")[1:]:
        parts = line.split(",")
        if len(parts) != 4:
            continue
        m, k, i, v = parts
        out[(m, k, i)] = float(v)
    return out


def write_csv(rows_dict: dict[tuple[str, str, str], float]):
    CSV.parent.mkdir(parents=True, exist_ok=True)
    keys = sorted(rows_dict.keys(), key=lambda k: (k[0], k[1], k[2]))
    lines = ["month,kind,id,gbtud"]
    for m, k, i in keys:
        v = rows_dict[(m, k, i)]
        lines.append(f"{m},{k},{i},{v:g}")
    CSV.write_text("\n".join(lines) + "\n", encoding="utf-8")


def merge_rows(existing: dict, new_rows: list) -> dict:
    """New rows OVERWRITE existing entries with the same (month, kind, id)."""
    merged = dict(existing)
    for m, k, i, v in new_rows:
        merged[(m, k, i)] = v
    return merged


# ─── Git ─────────────────────────────────────────────────────────────────────
def commit_and_push(latest_label: str):
    subprocess.run(["git", "-C", str(REPO), "add", str(CSV)], check=True)
    rc = subprocess.run(
        ["git", "-C", str(REPO), "diff", "--cached", "--quiet"]
    ).returncode
    if rc == 0:
        print("Colombia: no changes after add — nothing to commit.")
        return
    msg = f"data: Colombia BMC gas balance refresh (latest month {latest_label})"
    subprocess.run(["git", "-C", str(REPO), "commit", "-m", msg], check=True)
    subprocess.run(["git", "-C", str(REPO), "push", "origin", "main"], check=True)
    print(f"Colombia: pushed → {msg}")


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", type=int, default=0,
                    help="Scrape the last N PDFs (instead of just the latest)")
    ap.add_argument("--pdf", type=str, default=None,
                    help="Scrape this specific PDF URL only")
    ap.add_argument("--no-push", action="store_true",
                    help="Skip git commit + push (for local testing)")
    args = ap.parse_args()

    tmpdir = Path("/tmp")
    targets: list[tuple[str, str]] = []  # [(label, url), ...]

    if args.pdf:
        # Derive label from URL
        fn = urllib.parse.unquote(args.pdf.split("/")[-1])
        m = re.search(r"(\d{4})\s+([A-Za-zñáéíóúÁÉÍÓÚ]+)", fn)
        label = "unknown"
        if m:
            mo_name = m.group(2).lower()
            mo_name = mo_name.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
            mo_n = ES_MONTHS.get(mo_name)
            if not mo_n:
                for full, num in ES_MONTHS.items():
                    if full.startswith(mo_name) and len(mo_name) >= 3:
                        mo_n = num; break
            if mo_n:
                label = f"{int(m.group(1)):04d}-{mo_n:02d}"
        targets = [(label, args.pdf)]
    else:
        try:
            html = fetch_index_html()
        except Exception as e:
            print(f"BMC index fetch failed: {e}", file=sys.stderr)
            sys.exit(1)
        all_pdfs = discover_pdfs(html)
        if not all_pdfs:
            print("No PDFs found in BMC index.", file=sys.stderr)
            sys.exit(1)
        n = max(1, args.backfill or 1)
        targets = all_pdfs[:n]

    print(f"Targets ({len(targets)}):")
    for lbl, url in targets:
        print(f"  {lbl}  {url}")

    existing = load_existing()
    new_rows: list = []
    for lbl, url in targets:
        try:
            rows = scrape_one(url, lbl, tmpdir)
            print(f"  ✓ {lbl}: {len(rows)} rows")
            new_rows.extend(rows)
        except Exception as e:
            print(f"  ✗ {lbl}: {e}", file=sys.stderr)

    if not new_rows:
        print("No rows extracted — aborting.", file=sys.stderr)
        sys.exit(1)

    merged = merge_rows(existing, new_rows)
    write_csv(merged)
    print(f"CSV written: {len(merged)} rows total -> {CSV}")

    if not args.no_push:
        latest = max(lbl for lbl, _ in targets)
        commit_and_push(latest)


if __name__ == "__main__":
    main()
