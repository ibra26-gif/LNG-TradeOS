#!/usr/bin/env python3
"""
acer_fetch.py — daily refresh of ACER TERMINAL physical price assessment CSV
LNG TradeOS™

Fetches https://aegis.acer.europa.eu/terminal/price_assessments/historical_data
and writes it to data/acer_historical.csv at the repo root, then commits and
pushes if there are changes. Designed to be run by launchd at ~19:00 CET each
business day (after ACER's EOD ~18:30 CET publication).

Why on the laptop and not on Vercel: Vercel serverless cannot reach
aegis.acer.europa.eu reliably (502 / fetch failed), but residential IPs do.
Same pattern as the EEX daily scrape.

Install launchd:
  cp scripts/com.lngtradeos.acer.plist ~/Library/LaunchAgents/
  launchctl load ~/Library/LaunchAgents/com.lngtradeos.acer.plist

Manual run:
  python3 scripts/acer_fetch.py
"""

import subprocess
import sys
import urllib.request
from pathlib import Path

URL = "https://aegis.acer.europa.eu/terminal/price_assessments/historical_data"
REPO = Path(__file__).resolve().parent.parent
CSV  = REPO / "data" / "acer_historical.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch():
    req = urllib.request.Request(URL, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        if r.status != 200:
            raise RuntimeError(f"ACER returned HTTP {r.status}")
        return r.read().decode("utf-8", errors="replace")


def changed(new_text: str) -> bool:
    if not CSV.exists():
        return True
    return CSV.read_text(encoding="utf-8") != new_text


def write(new_text: str):
    CSV.parent.mkdir(parents=True, exist_ok=True)
    CSV.write_text(new_text, encoding="utf-8")


def commit_and_push():
    # Detect latest trade date from the freshly written CSV (line 2 col 1).
    with CSV.open() as f:
        f.readline()  # header
        first = f.readline()
    latest = first.split(",")[0].strip('"') if first else "unknown"

    msg = f"data: ACER physical curves refresh (latest trade date {latest})"
    subprocess.run(["git", "-C", str(REPO), "add", str(CSV)], check=True)
    rc = subprocess.run(
        ["git", "-C", str(REPO), "diff", "--cached", "--quiet"]
    ).returncode
    if rc == 0:
        print("ACER: no changes after add — nothing to commit.")
        return
    subprocess.run(["git", "-C", str(REPO), "commit", "-m", msg], check=True)
    subprocess.run(["git", "-C", str(REPO), "push", "origin", "main"], check=True)
    print(f"ACER: pushed → {msg}")


def main():
    try:
        text = fetch()
    except Exception as e:
        print(f"ACER fetch failed: {e}", file=sys.stderr)
        sys.exit(1)

    if not changed(text):
        print("ACER: CSV unchanged, no commit needed.")
        return

    write(text)
    commit_and_push()


if __name__ == "__main__":
    main()
