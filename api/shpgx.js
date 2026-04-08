// api/shpgx.js — Vercel Serverless Function
// Fetches SHPGX price data server-side, parses HTML tables, returns JSON
// Endpoint: /api/shpgx?series=truck,cld,exterm,diesel,cnp,pipe_spot&pages=3

const SERIES_CONFIG = {
  truck: {
    url:      'https://www.shpgx.com/html/sjg.html',
    unit:     'cny_kg',
    priceCol: ['价格（元/千克）', '价格'],
  },
  cld: {
    url:      'https://www.shpgx.com/html/zgjkxhLNGdajg.html',
    unit:     'usd_mmbtu',
    priceCol: ['价格（美元/百万英热）', '价格'],
  },
  exterm: {
    url:      'https://www.shpgx.com/html/czjg.html',
    unit:     'cny_tonne',
    priceCol: ['价格（元/吨）', '价格'],
  },
  diesel: {
    url:      'https://www.shpgx.com/html/zgqcypfjg.html',
    unit:     'cny_tonne',
    priceCol: ['价格（元/吨）', '价格'],
  },
  cnp: {
    url:      'https://www.shpgx.com/html/qgjgydjj.html',
    unit:     'cny_m3',
    priceCol: ['价格（元/方）', '价格'],
  },
  pipe_spot: {
    url:      'https://www.shpgx.com/html/zdjg.html',
    unit:     'cny_m3',
    priceCol: ['价格（元/方）', '价格'],
  },
};

// ── HTML table parser (no external dependencies) ─────────────────────────────
function stripTags(s) {
  return s
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .trim();
}

function parseHtmlTable(html) {
  const tableRx = /<table[\s\S]*?<\/table>/gi;
  const tables  = html.match(tableRx) || [];
  if (!tables.length) return { headers: [], rows: [] };

  // Use the largest table (data table, not nav)
  const dataTbl = tables.sort((a, b) => b.length - a.length)[0];

  const trRx    = /<tr[\s\S]*?<\/tr>/gi;
  const trBlocks = dataTbl.match(trRx) || [];

  let headers = [];
  const rows  = [];

  for (let i = 0; i < trBlocks.length; i++) {
    const cellRx = /<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/gi;
    const cells  = [];
    let m;
    while ((m = cellRx.exec(trBlocks[i])) !== null) {
      cells.push(stripTags(m[1]));
    }
    if (!cells.length) continue;

    // Rows without a yyyy-mm-dd style cell are treated as headers
    const hasDate = cells.some(c => /^\d{4}[-/]\d{1,2}/.test(c));
    if (!hasDate) {
      if (!headers.length) headers = cells;
      continue;
    }

    const row = {};
    headers.forEach((h, j) => { if (cells[j] !== undefined) row[h] = cells[j]; });
    rows.push(row);
  }

  return { headers, rows };
}

// ── Extract numeric price from a parsed row ──────────────────────────────────
function extractPrice(row, priceCols) {
  for (const col of priceCols) {
    if (row[col] !== undefined) {
      const v = parseFloat(String(row[col]).replace(/,/g, ''));
      if (!isNaN(v) && v > 0) return v;
    }
  }
  // Fallback: find first plausible numeric value
  for (const raw of Object.values(row)) {
    const v = parseFloat(String(raw).replace(/,/g, ''));
    if (!isNaN(v) && v > 0 && v < 200000) return v;
  }
  return null;
}

// ── Extract ISO date string from a parsed row ────────────────────────────────
function extractDate(row) {
  const candidates = [row['日期'], row['date']].filter(Boolean);
  for (const c of candidates) {
    const match = String(c).match(/(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
    if (match) return `${match[1]}-${match[2].padStart(2,'0')}-${match[3].padStart(2,'0')}`;
  }
  // Scan all cell values
  for (const v of Object.values(row)) {
    const match = String(v).match(/(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
    if (match) return `${match[1]}-${match[2].padStart(2,'0')}-${match[3].padStart(2,'0')}`;
  }
  return null;
}

// ── Fetch + parse one series across N pages ──────────────────────────────────
async function fetchSeries(key, config, pages) {
  const allRows = [];
  const errs    = [];

  for (let page = 1; page <= pages; page++) {
    const pageUrl = page === 1
      ? config.url
      : config.url + (config.url.includes('?') ? '&' : '?') + 'pageNum=' + page;

    try {
      const resp = await fetch(pageUrl, {
        headers: {
          'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
          'Accept':          'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
          'Accept-Encoding': 'gzip, deflate',
          'Referer':         'https://www.shpgx.com/',
          'Connection':      'keep-alive',
        },
        signal: AbortSignal.timeout(15000),
      });

      if (!resp.ok) {
        errs.push(`p${page}:HTTP${resp.status}`);
        break;
      }

      const html = await resp.text();
      const { rows: parsed } = parseHtmlTable(html);

      if (!parsed.length) {
        if (page > 1) break; // ran past last page
        errs.push(`p${page}:no_rows`);
        break;
      }

      let pageRows = 0;
      for (const r of parsed) {
        const date  = extractDate(r);
        const price = extractPrice(r, config.priceCol);
        if (!date || price === null) continue;
        allRows.push({
          date,
          value:  price,
          region: r['地区'] || r['region'] || '',
          grade:  r['品种'] || r['grade']  || '',
        });
        pageRows++;
      }

      if (pageRows === 0) break; // empty page

      // Polite delay between pages of the same series
      if (page < pages) await new Promise(res => setTimeout(res, 600));

    } catch (e) {
      errs.push(`p${page}:${e.message.slice(0,40)}`);
      break;
    }
  }

  // Deduplicate by date + region + grade
  const seen    = new Set();
  const deduped = allRows.filter(r => {
    const k = `${r.date}|${r.region}|${r.grade}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  // Sort newest first
  deduped.sort((a, b) => (b.date > a.date ? 1 : -1));

  return { rows: deduped, unit: config.unit, errors: errs };
}

// ── Vercel handler ───────────────────────────────────────────────────────────
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // JSON debug: ?debugjson=1&series=truck — fetch the actual JSON data endpoint
  if (req.query.debugjson === '1') {
    const key    = (req.query.series || 'truck').split(',')[0].trim();
    const config = SERIES_CONFIG[key];
    if (!config) return res.status(400).json({ error: 'unknown series' });
    try {
      // First get the HTML to extract data-ajax-url
      const pageResp = await fetch(config.url, {
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html', 'Referer': 'https://www.shpgx.com/' },
        signal: AbortSignal.timeout(10000),
      });
      const html = await pageResp.text();
      const ajaxMatch = html.match(/data-ajax-url=["']([^"']+)["']/);
      const ajaxPath  = ajaxMatch ? ajaxMatch[1] : null;
      if (!ajaxPath) {
        return res.status(200).send('No data-ajax-url found in HTML\n' + html.slice(15000));
      }
      const jsonUrl = 'https://www.shpgx.com' + ajaxPath + '?draw=1&start=0&length=25';
      const jsonResp = await fetch(jsonUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Accept': 'application/json, text/javascript, */*',
          'X-Requested-With': 'XMLHttpRequest',
          'Referer': config.url,
        },
        signal: AbortSignal.timeout(10000),
      });
      const raw = await jsonResp.text();
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      return res.status(200).send(
        `JSON endpoint: ${jsonUrl}\nHTTP: ${jsonResp.status}\n\n` + raw.slice(0, 5000)
      );
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // Debug mode: ?debug=1&series=truck
  if (req.query.debug === '1') {
    const key    = (req.query.series || 'truck').split(',')[0].trim();
    const config = SERIES_CONFIG[key];
    if (!config) return res.status(400).json({ error: 'unknown series' });
    try {
      const resp = await fetch(config.url, {
        headers: {
          'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
          'Accept':          'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
          'Referer':         'https://www.shpgx.com/',
        },
        signal: AbortSignal.timeout(15000),
      });
      const html = await resp.text();
      // No cache on debug responses
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      const tableIdx = html.indexOf('<table');
      const tableEnd = html.lastIndexOf('</table>');
      // Extract all script src and ajax patterns
      const scripts = (html.match(/src=["']([^"']+\.js[^"']*)/g)||[]).map(s=>s.replace(/src=["']/,'')).slice(0,8);
      const jspx    = (html.match(/["']([^"']*\.jspx[^"']*)/g)||[]).map(s=>s.replace(/['"]/g,'')).slice(0,8);
      const ajaxGet = (html.match(/\$\.(?:get|post|ajax|getJSON)\s*\(["']([^"']+)/g)||[]).slice(0,8);
      const fullHtml = html; // Return full HTML — only 19KB
      return res.status(200).send([
        `=== SHPGX DEBUG ===`,
        `HTTP: ${resp.status} | Length: ${html.length} | tableIdx: ${tableIdx} | tableEnd: ${tableEnd}`,
        `Scripts: ${scripts.join(' | ')}`,
        `JSPX endpoints: ${jspx.join(' | ')}`,
        `Ajax calls: ${ajaxGet.join(' | ')}`,
        ``,
        `=== TABLE SECTION (${tableIdx} to ${tableEnd}) ===`,
        tableIdx > -1 ? html.slice(tableIdx, tableEnd + 8) : 'NO <table> FOUND IN HTML',
        ``,
        `=== LAST 2000 CHARS ===`,
        html.slice(-2000),
      ].join('\n'));
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  const {
    series = 'truck,cld,exterm,diesel,cnp,pipe_spot',
    pages  = '3',
  } = req.query;

  const seriesKeys = series.split(',').map(s => s.trim()).filter(k => SERIES_CONFIG[k]);
  const pageCount  = Math.min(Math.max(parseInt(pages) || 3, 1), 10);

  if (!seriesKeys.length) {
    return res.status(400).json({
      ok: false,
      error: 'No valid series. Valid keys: ' + Object.keys(SERIES_CONFIG).join(', '),
    });
  }

  const data   = {};
  const errors = {};

  // Fetch all series in parallel
  await Promise.allSettled(
    seriesKeys.map(async key => {
      try {
        const result  = await fetchSeries(key, SERIES_CONFIG[key], pageCount);
        data[key]     = { rows: result.rows, unit: result.unit };
        if (result.errors.length) errors[key] = result.errors.join('; ');
      } catch (e) {
        errors[key] = e.message;
        data[key]   = { rows: [], unit: SERIES_CONFIG[key]?.unit || '' };
      }
    })
  );

  const rowCounts = Object.fromEntries(
    Object.entries(data).map(([k, v]) => [k, v.rows.length])
  );

  // CDN cache 1h, serve stale up to 24h while revalidating in background
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=86400');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  return res.status(200).json({
    ok:           true,
    data,
    errors,
    fetched_at:   new Date().toISOString(),
    series_count: seriesKeys.length,
    row_counts:   rowCounts,
    total_rows:   Object.values(rowCounts).reduce((s, n) => s + n, 0),
  });
}
