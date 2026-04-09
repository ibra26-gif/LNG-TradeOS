// api/china-gas-balance.js — Vercel Serverless Function
// Fetches China gas balance data from:
//   NBS (National Bureau of Statistics) — domestic production
//   GACC (China Customs) — LNG + pipeline imports
// Returns monthly rows: { m, prod, lng, pipe } all in BCM

// ── NBS API ──────────────────────────────────────────────────────────────────
// Series A0E0C01 = natural gas production (100 million cubic meters / 亿立方米)
// 1 亿立方米 = 0.1 BCM
const NBS_URL = 'https://data.stats.gov.cn/english/easyquery.htm';

async function fetchNBSProduction() {
  const params = new URLSearchParams({
    m: 'QueryData',
    dbcode: 'hgyd',
    rowcode: 'zb',
    colcode: 'sj',
    wds: '[]',
    dfwds: JSON.stringify([
      { wdcode: 'zb', valuecode: 'A0E0C01' },
      { wdcode: 'sj', valuecode: 'last60' },
    ]),
    k1: Date.now(),
  });

  const url = `${NBS_URL}?${params}`;
  const resp = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
      'Accept': 'application/json, text/javascript, */*',
      'Referer': 'https://data.stats.gov.cn/',
    },
    signal: AbortSignal.timeout(12000),
  });

  if (!resp.ok) throw new Error(`NBS HTTP ${resp.status}`);
  const json = await resp.json();

  // NBS returns: { returndata: { datanodes: [ { wds: [{valuecode: 'YYYY-MM'}], data: {hasdata, data} } ] } }
  const nodes = json?.returndata?.datanodes || [];
  const result = {};

  for (const node of nodes) {
    const period = node.wds?.find(w => w.wdcode === 'sj')?.valuecode;
    const value  = node.data?.data;
    if (period && value != null && value !== '') {
      // Convert 亿立方米 → BCM (1 亿m³ = 0.1 BCM)
      const bcm = parseFloat(value) * 0.1;
      if (!isNaN(bcm)) result[period] = +bcm.toFixed(3);
    }
  }

  return result; // { 'YYYY-MM': bcm_value }
}

// ── GACC imports ─────────────────────────────────────────────────────────────
// GACC publishes monthly HTML tables at english.customs.gov.cn
// Table 6: "China's Major Imports by Quantity and Value"
// LNG is HS 2711.11, pipeline gas is HS 2711.21
// Published ~10th of following month
// We scrape the monthly statistics index to find the latest report

async function fetchGACCIndex() {
  const resp = await fetch('http://english.customs.gov.cn/statics/report/monthly.html', {
    headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html' },
    signal: AbortSignal.timeout(10000),
  });
  if (!resp.ok) throw new Error(`GACC index HTTP ${resp.status}`);
  return await resp.text();
}

function parseGACCLinks(html) {
  // Find links to monthly import/export tables (table 6)
  const links = [];
  const rx = /href="([^"]*Statics[^"]*\.html)"[^>]*>[^<]*(?:Major Imports|Import.*Quantity|China.*Import)/gi;
  let m;
  while ((m = rx.exec(html)) !== null) {
    links.push(m[1]);
  }
  // Also try direct pattern matching
  const rx2 = /href="(\/Statics\/[a-f0-9-]+\.html)"/gi;
  while ((m = rx2.exec(html)) !== null) {
    if (!links.includes(m[1])) links.push(m[1]);
  }
  return links.slice(0, 24); // last 24 months
}

async function fetchGACCTable(path) {
  const base = 'http://english.customs.gov.cn';
  const url = path.startsWith('http') ? path : base + path;

  const resp = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html' },
    signal: AbortSignal.timeout(10000),
  });
  if (!resp.ok) return null;
  const html = await resp.text();

  // Extract period from page title or heading
  const titleMatch = html.match(/(\w+ \d{4})|(\d{1,2}\.\d{4})|(\d{4}-\d{2})/);
  let period = null;
  if (titleMatch) {
    const raw = titleMatch[0];
    // Convert to YYYY-MM
    const months = {January:'01',February:'02',March:'03',April:'04',May:'05',June:'06',
                    July:'07',August:'08',September:'09',October:'10',November:'11',December:'12'};
    for (const [name, num] of Object.entries(months)) {
      if (raw.includes(name)) {
        const yr = raw.match(/\d{4}/)?.[0];
        if (yr) period = `${yr}-${num}`;
        break;
      }
    }
    if (!period && /\d{4}-\d{2}/.test(raw)) period = raw.slice(0, 7);
  }

  // Parse table rows for LNG (liquefied natural gas) and pipeline gas
  const result = { lng_mt: null, pipe_mt: null, period };

  // Look for LNG: "Liquefied natural gas" with quantity in 10,000 tonnes
  const rows = html.match(/<tr[^>]*>[\s\S]*?<\/tr>/gi) || [];
  for (const row of rows) {
    const cells = (row.match(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi) || [])
      .map(c => c.replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').trim());

    const rowText = cells.join(' ').toLowerCase();
    if (rowText.includes('liquefied natural gas') || rowText.includes('lng')) {
      // Find quantity column (usually 3rd or 4th column, in 10,000 tonnes)
      for (const cell of cells) {
        const n = parseFloat(cell.replace(/,/g, ''));
        if (!isNaN(n) && n > 10 && n < 20000) {
          result.lng_mt = n; // in 10,000 tonnes
          break;
        }
      }
    }
    if (rowText.includes('natural gas') && (rowText.includes('pipeline') || rowText.includes('gaseous'))) {
      for (const cell of cells) {
        const n = parseFloat(cell.replace(/,/g, ''));
        if (!isNaN(n) && n > 1 && n < 20000) {
          result.pipe_mt = n;
          break;
        }
      }
    }
  }

  return result;
}

// Convert million tonnes → BCM (LNG: 1 MT ≈ 1.36 BCM; pipeline: 1 MT ≈ 1.33 BCM)
function lngMtToBCM(mt_ten_thousands) {
  if (mt_ten_thousands == null) return null;
  const mt = mt_ten_thousands / 100; // 10,000 tonnes → MT
  return +(mt * 1.36).toFixed(3);
}
function pipeMtToBCM(mt_ten_thousands) {
  if (mt_ten_thousands == null) return null;
  const mt = mt_ten_thousands / 100;
  return +(mt * 1.33).toFixed(3);
}

// ── Fallback: hardcoded recent data ──────────────────────────────────────────
const FALLBACK_DATA = [
  {m:'2024-01',prod:20.5,lng:10.61,pipe:6.27},{m:'2024-02',prod:18.2,lng:9.38,pipe:5.58},
  {m:'2024-03',prod:19.8,lng:9.79,pipe:5.85},{m:'2024-04',prod:19.5,lng:9.25,pipe:5.44},
  {m:'2024-05',prod:20.2,lng:8.84,pipe:5.69},{m:'2024-06',prod:20.8,lng:9.38,pipe:5.98},
  {m:'2024-07',prod:21.3,lng:9.65,pipe:6.12},{m:'2024-08',prod:21.9,lng:9.93,pipe:6.25},
  {m:'2024-09',prod:20.7,lng:9.25,pipe:5.85},{m:'2024-10',prod:21.4,lng:9.52,pipe:5.98},
  {m:'2024-11',prod:21.1,lng:10.61,pipe:6.25},{m:'2024-12',prod:22.8,lng:12.51,pipe:6.93},
  {m:'2025-01',prod:22.1,lng:12.92,pipe:7.07},{m:'2025-02',prod:19.5,lng:10.61,pipe:6.12},
  {m:'2025-03',prod:21.2,prod:21.2,lng:11.02,pipe:6.39},{m:'2025-04',prod:21.8,lng:10.75,pipe:6.25},
  {m:'2025-05',prod:22.0,lng:11.29,pipe:6.52},{m:'2025-06',prod:22.5,lng:11.70,pipe:6.66},
  {m:'2025-07',prod:22.9,lng:11.43,pipe:6.52},{m:'2025-08',prod:23.1,lng:11.16,pipe:6.39},
  {m:'2025-09',prod:22.0,lng:11.56,pipe:6.66},{m:'2025-10',prod:22.4,lng:12.24,pipe:6.80},
  {m:'2025-11',prod:22.2,lng:13.33,pipe:7.07},{m:'2025-12',prod:23.8,lng:15.23,pipe:7.89},
];

// ── Vercel handler ────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=86400');

  const errors = {};
  let prodData = {}, gaccRows = [];

  // Fetch NBS production
  try {
    prodData = await fetchNBSProduction();
  } catch(e) {
    errors.nbs = e.message;
  }

  // Fetch GACC imports
  try {
    const indexHtml = await fetchGACCIndex();
    const links = parseGACCLinks(indexHtml);
    const tableResults = await Promise.allSettled(
      links.slice(0, 12).map(link => fetchGACCTable(link))
    );
    for (const r of tableResults) {
      if (r.status === 'fulfilled' && r.value?.period) {
        gaccRows.push(r.value);
      }
    }
  } catch(e) {
    errors.gacc = e.message;
  }

  // Merge production + imports into monthly rows
  const allMonths = new Set([...Object.keys(prodData), ...gaccRows.map(r => r.period).filter(Boolean)]);
  const rows = [];

  for (const m of [...allMonths].sort()) {
    const prod = prodData[m] || null;
    const gacc = gaccRows.find(r => r.period === m);
    const lng  = gacc ? lngMtToBCM(gacc.lng_mt) : null;
    const pipe = gacc ? pipeMtToBCM(gacc.pipe_mt) : null;

    if (prod || lng || pipe) {
      rows.push({ m, prod: prod || 0, lng: lng || 0, pipe: pipe || 0 });
    }
  }

  // If live data is too sparse, supplement with fallback
  const liveRows = rows.filter(r => r.prod > 0);
  if (liveRows.length < 6) {
    return res.status(200).json({
      ok: true,
      rows: FALLBACK_DATA,
      source: 'ESTIMATED (NBS/GACC unavailable)',
      errors,
    });
  }

  return res.status(200).json({
    ok: true,
    rows: rows.filter(r => r.prod > 0 || r.lng > 0),
    source: `NBS + GACC (${liveRows.length} months live)`,
    errors: Object.keys(errors).length ? errors : undefined,
  });
}
