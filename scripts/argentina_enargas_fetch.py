#!/usr/bin/env python3
"""
argentina_enargas_fetch.py — monthly Argentina gas balance from ENARGAS.

Source: https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-operativos.php
Three XLSX files, always at the same URL (overwritten monthly):
  GRT.xlsx            — Supply by basin (sheet "Cuenca")
  GETD.xlsx           — Demand by user type (sheet "TipoUsuario")
  Expo/Exportaciones.xlsx — Exports by pipeline destination

All values are in thousand m³ @ 9300 kcal per month. We preserve that unit
in the CSV; the API converts to MCM/day using actual days in month.

Output: data/argentina_gas_balance.csv in long format:
    month,kind,id,thousand_m3_month
    2026-02,supply,tgn_neuquina,924329
    2026-02,demand,dist_residencial,282894
    2026-02,export,chile_gasandes,190051

Run:
    python3 scripts/argentina_enargas_fetch.py [--no-push] [--months N]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

import openpyxl

REPO = Path(__file__).resolve().parent.parent
CSV  = REPO / "data" / "argentina_gas_balance.csv"

BASE = "https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-estadisticos"
FILES = {
    "GRT":  f"{BASE}/GRT/GRT.xlsx",
    "GETD": f"{BASE}/GETD/GETD.xlsx",
    "EXP":  f"{BASE}/Expo/Exportaciones.xlsx",
}

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
HEADERS = {"User-Agent": UA, "Accept": "*/*"}

# GRT.xlsx sheet "Cuenca" — column order (data row 15 onwards, A=date)
#   B TGN_Neuquina · C TGN_Noroeste · D TGN_Otros · E TGS_Neuquina
#   F TGS_San_Jorge · G TGS_Austral · H TGS_Otros · I Dist_Propios
#   J Dist_Otros · K Total_general
GRT_COLS = [
    (1, "tgn_neuquina"),  (2, "tgn_noroeste"), (3, "tgn_otros"),
    (4, "tgs_neuquina"),  (5, "tgs_san_jorge"), (6, "tgs_austral"), (7, "tgs_otros"),
    (8, "dist_propios"),  (9, "dist_otros"),
]

# GETD.xlsx sheet "TipoUsuario" — column order (A=date)
#   B Dist_Residencial · C Dist_Comercial · D Dist_Entes_Oficiales
#   E Dist_Industria · F Dist_Centrales · G Dist_Subdistribuidor · H Dist_GNC
#   I Trans_Industria · J Trans_RTP · K Trans_Centrales · L Trans_Subdistribuidor
GETD_COLS = [
    (1,  "dist_residencial"),   (2,  "dist_comercial"),        (3, "dist_entes_oficiales"),
    (4,  "dist_industria"),     (5,  "dist_centrales"),        (6, "dist_subdistribuidor"),
    (7,  "dist_gnc"),           (8,  "trans_industria"),       (9, "trans_rtp"),
    (10, "trans_centrales"),    (11, "trans_subdistribuidor"),
]

# Exportaciones.xlsx sheet "Exportaciones" — column order (A=date)
#   B Brasil_YPF_Uruguayana · C Chile_Gasandes · D Chile_Norandino
#   E Uruguay_Petrouruguay · F Chile_EGS · G Chile_METHANEX_YPF · H Uruguay_GAS_LINK
EXP_COLS = [
    (1, "brasil_ypf_uruguayana"), (2, "chile_gasandes"),    (3, "chile_norandino"),
    (4, "uruguay_petrouruguay"),  (5, "chile_egs"),         (6, "chile_methanex_ypf"),
    (7, "uruguay_gas_link"),
]


def download(url: str, dest: Path) -> Path:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as r:
        dest.write_bytes(r.read())
    return dest


def parse_sheet(path: Path, sheet_name: str, col_map: list[tuple[int, str]]) -> dict[str, dict[str, float]]:
    """Returns { 'YYYY-MM': { id: thousand_m3_month } }."""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb[sheet_name]
    out: dict[str, dict[str, float]] = {}
    for row in ws.iter_rows(values_only=True):
        if not row or not row[0]:
            continue
        if not isinstance(row[0], datetime):
            continue
        d = row[0]
        ym = f"{d.year:04d}-{d.month:02d}"
        bucket = out.setdefault(ym, {})
        for off, key in col_map:
            if off < len(row):
                v = row[off]
                if v is None:
                    continue
                try:
                    bucket[key] = float(v)
                except (TypeError, ValueError):
                    continue
    return out


# ─── CSV I/O ────────────────────────────────────────────────────────────────
def load_existing() -> dict[tuple[str, str, str], float]:
    if not CSV.exists():
        return {}
    out = {}
    for line in CSV.read_text(encoding="utf-8").strip().split("\n")[1:]:
        parts = line.split(",")
        if len(parts) != 4:
            continue
        out[(parts[0], parts[1], parts[2])] = float(parts[3])
    return out


def write_csv(rows: dict[tuple[str, str, str], float]):
    CSV.parent.mkdir(parents=True, exist_ok=True)
    keys = sorted(rows.keys())
    lines = ["month,kind,id,thousand_m3_month"]
    for m, k, i in keys:
        lines.append(f"{m},{k},{i},{rows[(m,k,i)]:g}")
    CSV.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ─── Git ────────────────────────────────────────────────────────────────────
def commit_and_push(latest_month: str):
    subprocess.run(["git", "-C", str(REPO), "add", str(CSV)], check=True)
    rc = subprocess.run(["git", "-C", str(REPO), "diff", "--cached", "--quiet"]).returncode
    if rc == 0:
        print("Argentina: no changes after add — nothing to commit.")
        return
    msg = f"data: Argentina ENARGAS gas balance refresh (latest month {latest_month})"
    subprocess.run(["git", "-C", str(REPO), "commit", "-m", msg], check=True)
    subprocess.run(["git", "-C", str(REPO), "push", "origin", "main"], check=True)
    print(f"Argentina: pushed → {msg}")


# ─── Main ───────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-push", action="store_true", help="Skip git commit + push")
    ap.add_argument("--since", type=str, default="2024-01",
                    help="Keep months from this YYYY-MM onward (default 2024-01).")
    args = ap.parse_args()

    tmp = Path("/tmp")
    paths = {}
    for label, url in FILES.items():
        dest = tmp / f"enargas_{label}.xlsx"
        try:
            download(url, dest)
            print(f"  ✓ {label}: {url} → {dest} ({dest.stat().st_size} bytes)")
        except Exception as e:
            print(f"  ✗ {label} download failed: {e}", file=sys.stderr)
            sys.exit(1)
        paths[label] = dest

    supply = parse_sheet(paths["GRT"],  "Cuenca",     GRT_COLS)
    demand = parse_sheet(paths["GETD"], "TipoUsuario", GETD_COLS)
    exports = parse_sheet(paths["EXP"], "Exportaciones", EXP_COLS)

    # Union of months across all three sources, keep from --since onward
    all_months = sorted(set(supply) | set(demand) | set(exports))
    keep = [m for m in all_months if m >= args.since]
    if not keep:
        print(f"No months >= {args.since} in source data (latest: {all_months[-1] if all_months else 'none'}).", file=sys.stderr)
        sys.exit(1)
    print(f"Parsed: supply={len(supply)} demand={len(demand)} exports={len(exports)} months; keeping {len(keep)} from {args.since}: {keep[0]} → {keep[-1]}")

    # Start fresh — fully regenerated from source each run
    rows: dict[tuple[str, str, str], float] = {}
    for m in keep:
        for k, v in (supply.get(m) or {}).items():
            rows[(m, "supply", k)] = v
        for k, v in (demand.get(m) or {}).items():
            rows[(m, "demand", k)] = v
        for k, v in (exports.get(m) or {}).items():
            rows[(m, "export", k)] = v

    write_csv(rows)
    print(f"CSV written: {len(rows)} rows -> {CSV}")

    if not args.no_push:
        commit_and_push(keep[-1])


if __name__ == "__main__":
    main()
