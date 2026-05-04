"""
fetch_china_shpgx.py — GitHub Actions port of shpgx_fetch.py v6.2
LNG TradeOS™ — by Ibrahim Mar

Runs weekly on GitHub Actions. Scrapes 5 series from SHPGX,
consolidates into a single JSON file at data/china_shpgx.json compatible with
the v190 dashboard's csIngestPayload() function.

Differences from the Mac version (shpgx_fetch.py v6.2):
  * Output: single JSON file (not 4-5 CSVs on Desktop)
  * Output schema matches dashboard's existing expectation:
      { updated: ISO8601, data: { <key>: { unit, rows: [{date, region, grade, value}] } } }
  * Historical merge: new rows upsert into existing JSON by date/region/grade.
  * Repo-relative path (./data/china_shpgx.json from repo root)

Usage (local test on Ubuntu/Linux):
    pip install playwright
    playwright install chromium
    python3 scripts/fetch_china_shpgx.py

Usage on GitHub Actions:
    Triggered by .github/workflows/china-weekly.yml
"""

import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
# Resolve output path relative to the repo root (script lives in scripts/).
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = DATA_DIR / "china_shpgx.json"

TODAY = date.today()
TODAY_ISO = TODAY.isoformat()
YEAR_AGO_ISO = (TODAY - timedelta(days=365)).isoformat()
UTC_NOW = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

SERIES = [
    {
        'key':  'truck',
        'name': 'LNG Truck Retail — Province',
        'url':  'https://www.shpgx.com/html/sjg.html',
        'unit': 'cny_kg',
        'method': 'date_filter',
    },
    {
        'key':  'cld',
        'name': 'China Import Spot LNG (CLD) — daily, 30d chart',
        'url':  'https://www.shpgx.com/html/zgjkxhLNGdajg.html',
        'unit': 'usd_mmbtu',
        'method': 'echarts_main',
    },
    {
        'key':  'exterm',
        'name': 'LNG Ex-Terminal Price',
        'url':  'https://www.shpgx.com/html/czjg.html',
        'unit': 'cny_tonne',
        'method': 'date_filter',
    },
    {
        'key':  'cnp',
        'name': 'PetroChina CNP Pipeline Gas',
        'url':  'https://www.shpgx.com/html/qgjgydjj.html',
        'unit': 'cny_m3',
        'method': 'table_default',
    },
    {
        'key':  'diesel',
        'name': 'China Diesel Wholesale (0# diesel, weekly)',
        'url':  'https://www.shpgx.com/html/zgqcypfjg.html',
        'unit': 'cny_tonne',
        'method': 'echarts_main',
        'series_filter': ['柴油', 'diesel', '0#', '0号'],
    },
]


# ═════════════════════════════════════════════════════════════════════════════
# Extraction primitives (ported from shpgx_fetch.py v6.2)
# ═════════════════════════════════════════════════════════════════════════════

async def extract_echarts(page):
    """Extract ALL ECharts series from the page (dates + values + name)."""
    return await page.evaluate(r"""
    () => {
        const results = [];
        const divs = document.querySelectorAll('div[_echarts_instance_]');
        for (const div of divs) {
            try {
                const inst = echarts.getInstanceByDom(div);
                if (!inst) continue;
                const opt = inst.getOption();
                if (!opt?.xAxis?.[0]?.data?.length) continue;
                const xData = opt.xAxis[0].data;
                for (const s of (opt.series || [])) {
                    if (s.data?.length > 0) {
                        results.push({ dates: xData, values: s.data, name: s.name || '' });
                    }
                }
            } catch(e) {}
        }
        return results;
    }
    """)


async def scrape_table(page):
    """Scrape visible table body rows as array of cell arrays."""
    return await page.evaluate(r"""
    () => {
        const out = [];
        const tbl = document.querySelector('table tbody');
        if (!tbl) return out;
        tbl.querySelectorAll('tr').forEach(tr => {
            const cells = [...tr.querySelectorAll('td')].map(td => td.innerText.trim());
            if (cells.length >= 2 && /\d{4}/.test(cells[0])) {
                out.push(cells);
            }
        });
        return out;
    }
    """)


async def click_next_page(page):
    """Click pagination 'next' button via Playwright locators (supports :has-text)."""
    candidates = [
        'a[title="下一页"]:not(.disabled)',
        'a.next:not(.disabled)',
        '.paginate_button.next:not(.disabled)',
        'a:has-text("下一页")',
        'a:has-text("下页")',
        'li.next:not(.disabled) a',
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if await loc.count() == 0:
                continue
            first = loc.first
            try:
                if await first.is_disabled():
                    continue
            except Exception:
                pass
            try:
                cls = await first.get_attribute('class') or ''
                if 'disabled' in cls.lower():
                    continue
            except Exception:
                pass
            await first.click(timeout=5000)
            return True
        except Exception:
            continue
    return False


# ═════════════════════════════════════════════════════════════════════════════
# Series fetchers
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_with_date_filter(page, series):
    """For truck/exterm: fill date range, click 检索, scrape paginated table."""
    key = series['key']
    rows = []
    print(f"\n[{key}] {series['name']}")
    try:
        await page.goto(series['url'], wait_until='domcontentloaded', timeout=45000)
        await page.wait_for_timeout(3000)

        filled = await page.evaluate(r"""
        ([startDate, endDate]) => {
            const inputs = [...document.querySelectorAll('input[type="text"]')];
            const dateInputs = inputs.filter(i => {
                const w = i.offsetWidth;
                return w > 50 && w < 250;
            });
            if (dateInputs.length < 2) return { ok: false };
            const s = dateInputs[dateInputs.length - 2];
            const e = dateInputs[dateInputs.length - 1];
            s.value = startDate; e.value = endDate;
            s.dispatchEvent(new Event('change', {bubbles: true}));
            e.dispatchEvent(new Event('change', {bubbles: true}));
            return { ok: true };
        }
        """, [YEAR_AGO_ISO, TODAY_ISO])
        print(f"  Date fill: {filled}")

        if filled.get('ok'):
            for selector in [
                'input[value="检索"]',
                'button:has-text("检索")',
                'a:has-text("检索")',
                'text=检索',
            ]:
                try:
                    btn = page.locator(selector)
                    if await btn.count() > 0:
                        await btn.first.click(timeout=5000)
                        print(f"  Clicked 检索 (via {selector})")
                        await page.wait_for_timeout(4000)
                        try:
                            await page.wait_for_selector('table tbody tr', timeout=15000)
                        except Exception:
                            pass
                        await page.wait_for_timeout(1000)
                        break
                except Exception:
                    continue

        headers_raw = await page.evaluate(r"""
        () => [...document.querySelectorAll('table thead th, table tr:first-child th')]
            .map(e => e.innerText.trim())
        """)
        HEADER_MAP = {
            '日期': 'date', '地区': 'region', '品种': 'grade',
            '价格（元/千克）': 'value', '价格（美元/百万英热）': 'value',
            '价格（元/吨）': 'value', '价格（元/方）': 'value', '价格': 'value',
        }
        headers = [HEADER_MAP.get(h, h) for h in headers_raw]

        raw_rows = await scrape_table(page)
        print(f"  Rows page 1: {len(raw_rows)}")

        for pg in range(100):
            has_next = await click_next_page(page)
            if not has_next:
                break
            await page.wait_for_timeout(1500)
            more = await scrape_table(page)
            if not more:
                break
            raw_rows.extend(more)
            if (pg + 2) % 10 == 0:
                print(f"    Page {pg+2}: total {len(raw_rows)} rows")
        print(f"  Total rows scraped: {len(raw_rows)}")

        for cells in raw_rows:
            record = {headers[i]: cells[i] for i in range(min(len(headers), len(cells)))}
            if record.get('date') and record.get('value'):
                try:
                    price = float(str(record['value']).replace(',', ''))
                    rows.append({
                        'date':   record['date'],
                        'region': record.get('region', ''),
                        'grade':  record.get('grade', ''),
                        'value':  price,
                    })
                except ValueError:
                    continue

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()

    return rows


async def fetch_echarts_main(page, series):
    """For CLD + diesel: extract ECharts from main page with optional series_filter.
    Includes diesel-specific text-regex fallback if ECharts is empty."""
    key = series['key']
    rows = []
    print(f"\n[{key}] {series['name']}")
    try:
        await page.goto(series['url'], wait_until='domcontentloaded', timeout=45000)
        await page.wait_for_timeout(3000)

        # If 历史数据 is an in-page toggle (no href), click it. Never follow hrefs
        # to the dead zgjkxhLNGdajglssj.html.
        try:
            hist_loc = page.locator('a:has-text("历史数据"), button:has-text("历史数据")')
            if await hist_loc.count() > 0:
                href = await hist_loc.first.get_attribute('href')
                if not href or href in ('#', 'javascript:void(0)', 'javascript:;'):
                    await hist_loc.first.click(timeout=3000)
                    await page.wait_for_timeout(3000)
                    print("  Clicked 历史数据 (in-page toggle)")
                else:
                    print(f"  历史数据 is a navigation link ({href}) — skipping")
        except Exception:
            pass

        await page.wait_for_timeout(1000)
        all_series = await extract_echarts(page)

        if all_series:
            for sr in all_series:
                print(f"  ECharts series: '{sr['name']}' — {len(sr['dates'])} points")

            chosen = None
            series_filter = series.get('series_filter')
            if series_filter:
                for sr in all_series:
                    name = (sr.get('name') or '').lower()
                    if any(f.lower() in name for f in series_filter):
                        chosen = sr
                        print(f"  -> Filtered to series '{sr['name']}' ({len(sr['dates'])} pts)")
                        break
                if chosen is None:
                    print(f"  WARN: series_filter {series_filter} matched nothing; falling back to first")
                    chosen = all_series[0]
            else:
                chosen = all_series[0]

            region_tag = 'CN_IMPORT' if key == 'cld' else ('CN_NATIONAL' if key == 'diesel' else '')
            for d, v in zip(chosen['dates'], chosen['values']):
                try:
                    price = float(v) if not isinstance(v, list) else float(v[-1])
                    rows.append({
                        'date':   str(d),
                        'region': region_tag,
                        'grade':  '',
                        'value':  price,
                    })
                except (TypeError, ValueError):
                    continue
        else:
            print("  No ECharts instances found on page")

        # Diesel-specific text fallback (Option C): if ECharts gave nothing,
        # regex today's snapshot from page body.
        if not rows and key == 'diesel':
            print("  Attempting diesel text-extract fallback for latest snapshot...")
            text_data = await page.evaluate(r"""
            () => {
                const fullText = document.title + ' ' + document.body.innerText;
                const dieselMatch = fullText.match(/0\s*[号#]?\s*柴油[^:：0-9]*[:：]\s*(\d{3,6})/);
                const dateMatch   = fullText.match(/(\d{4}-\d{2}-\d{2})/);
                if (dieselMatch && dateMatch) {
                    return { date: dateMatch[1], price: parseFloat(dieselMatch[1]) };
                }
                return null;
            }
            """)
            if text_data and text_data.get('price'):
                print(f"  -> Text fallback: {text_data['date']} = {text_data['price']} CNY/t")
                rows.append({
                    'date':   text_data['date'],
                    'region': 'CN_NATIONAL',
                    'grade':  '0# diesel',
                    'value':  text_data['price'],
                })
            else:
                print("  -> Text fallback found nothing")

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()

    return rows


async def fetch_table_default(page, series):
    """For CNP: simple table scrape, no pagination."""
    key = series['key']
    rows = []
    print(f"\n[{key}] {series['name']}")
    try:
        nav_ok = False
        for attempt in range(2):
            try:
                await page.goto(series['url'], wait_until='domcontentloaded', timeout=45000)
                nav_ok = True
                break
            except Exception as e:
                print(f"  goto attempt {attempt+1}/2 failed: {type(e).__name__}")
                if attempt == 0:
                    await page.wait_for_timeout(3000)
        if not nav_ok:
            raise Exception(f"Could not load {series['url']} after 2 attempts")

        try:
            await page.wait_for_selector('table tbody tr', timeout=15000)
        except Exception:
            print("  table didn't appear within 15s — proceeding anyway")
        await page.wait_for_timeout(2000)

        headers_raw = await page.evaluate(r"""
        () => [...document.querySelectorAll('table thead th')].map(e => e.innerText.trim())
        """)
        HEADER_MAP = {
            '日期': 'date', '地区': 'region',
            '价格（元/方）': 'value', '价格': 'value',
        }
        headers = [HEADER_MAP.get(h, h) for h in headers_raw]

        raw_rows = await scrape_table(page)
        for cells in raw_rows:
            record = {headers[i]: cells[i] for i in range(min(len(headers), len(cells)))}
            if record.get('date') and record.get('value'):
                try:
                    rows.append({
                        'date':   record['date'],
                        'region': record.get('region', ''),
                        'grade':  '',
                        'value':  float(str(record['value']).replace(',', '')),
                    })
                except ValueError:
                    continue

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()

    return rows


# ═════════════════════════════════════════════════════════════════════════════
# Post-processing & output
# ═════════════════════════════════════════════════════════════════════════════

def dedupe_rows(rows):
    """Sort desc and dedupe by (date, region, grade)."""
    if not rows:
        return []
    rows.sort(key=lambda r: r.get('date', ''), reverse=True)
    seen = set()
    deduped = []
    for r in rows:
        k = f"{r.get('date','')}|{r.get('region','')}|{r.get('grade','')}"
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    return deduped


def load_existing_payload():
    if not OUTPUT_PATH.exists():
        return {"data": {}}
    try:
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            payload.setdefault("data", {})
            return payload
    except Exception as exc:
        print(f"Warning: could not read existing {OUTPUT_PATH}: {exc}")
    return {"data": {}}


def merge_rows(existing_rows, new_rows):
    """Upsert current scrape rows into existing history, preserving older rows."""
    merged = {}
    for row in existing_rows or []:
        key = f"{row.get('date','')}|{row.get('region','')}|{row.get('grade','')}"
        if row.get('date'):
            merged[key] = row
    for row in new_rows or []:
        key = f"{row.get('date','')}|{row.get('region','')}|{row.get('grade','')}"
        if row.get('date'):
            merged[key] = row
    return dedupe_rows(list(merged.values()))


def series_metadata(rows):
    dates = sorted({r.get('date', '') for r in rows if r.get('date')})
    return {
        "row_count": len(rows),
        "first_date": dates[0] if dates else None,
        "latest_date": dates[-1] if dates else None,
        "last_scraped": UTC_NOW,
    }


async def main():
    print("=" * 60)
    print("fetch_china_shpgx.py — GitHub Actions port of v6.2")
    print(f"UTC now:  {UTC_NOW}")
    print(f"Date:     {TODAY_ISO}")
    print(f"Lookback: {YEAR_AGO_ISO} -> {TODAY_ISO}")
    print(f"Output:   {OUTPUT_PATH}")
    print("=" * 60)

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("\nPlaywright not installed. Run:")
        print("  pip install playwright")
        print("  playwright install chromium")
        sys.exit(1)

    existing_payload = load_existing_payload()
    result = {
        "updated": UTC_NOW,
        "source": "SHPGX (www.shpgx.com)",
        "notes": "Generated by fetch_china_shpgx.py on GitHub Actions. Historical rows are merged by date/region/grade; missing scrape rows do not delete history.",
        "data": {},
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1400, "height": 900})

        for s in SERIES:
            method = s.get('method', 'table_default')
            if method == 'date_filter':
                rows = await fetch_with_date_filter(page, s)
            elif method == 'echarts_main':
                rows = await fetch_echarts_main(page, s)
            else:
                rows = await fetch_table_default(page, s)

            scraped = dedupe_rows(rows)
            existing_rows = existing_payload.get("data", {}).get(s['key'], {}).get("rows", [])
            merged = merge_rows(existing_rows, scraped)
            meta = series_metadata(merged)
            result["data"][s['key']] = {
                "name":  s['name'],
                "unit":  s['unit'],
                "url":   s['url'],
                **meta,
                "rows":  merged,
            }
            if merged:
                print(f"  -> {s['key']}: scraped {len(scraped)} rows, merged {len(merged)} history rows "
                      f"({meta['first_date']} -> {meta['latest_date']})")
            else:
                print(f"  -> {s['key']}: 0 rows")

        await browser.close()

    # Write JSON
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # Summary
    total_rows = sum(len(v.get('rows', [])) for v in result['data'].values())
    populated = sum(1 for v in result['data'].values() if v.get('rows'))
    print(f"\n{'=' * 60}")
    print(f"Wrote {OUTPUT_PATH}")
    print(f"  Series: {populated}/{len(SERIES)} populated, {total_rows} total rows")
    print(f"  File size: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")
    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
