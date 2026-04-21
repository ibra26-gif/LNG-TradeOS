// api/brazil-ons-hydro.js — Vercel Serverless Function
// Fetches daily reservoir stored-energy % per subsystem from ONS open data.
// Source: https://dados.ons.org.br/dataset/ear-diario-por-subsistema
// Dataset CSV (current year): published daily ~12:00 and ~19:00 BRT.
//
// Response shape consumed by the dashboard (samLoadBrazilHydro in lngtradeos.js):
//   { subsystems: { seco:{current,wow}, s:{...}, ne:{...}, n:{...} },
//     updatedAt, source }
// `current` = latest EAR % of max; `wow` = latest % minus % 7 days prior.
//
// Subsystem mapping (ONS uses 4 codes — SE is Sudeste+Centro-Oeste combined):
//   ONS 'SE' → dashboard 'seco'
//   ONS 'S'  → dashboard 's'
//   ONS 'NE' → dashboard 'ne'
//   ONS 'N'  → dashboard 'n'

const CSV_URL_TMPL = 'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/ear_subsistema_di/EAR_DIARIO_SUBSISTEMA_{YEAR}.csv';

const KEY_MAP = { SE: 'seco', S: 's', NE: 'ne', N: 'n' };

async function fetchCSV(year) {
  const url = CSV_URL_TMPL.replace('{YEAR}', year);
  const resp = await fetch(url, {
    headers: { 'User-Agent': 'LNG-TradeOS/1.0' },
    signal: AbortSignal.timeout(10000),
  });
  if (!resp.ok) throw new Error(`ONS HTTP ${resp.status} for ${year}`);
  return await resp.text();
}

function parseCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return [];
  const headers = lines[0].split(';').map(h => h.trim());
  const idx = {
    sub: headers.indexOf('id_subsistema'),
    date: headers.indexOf('ear_data'),
    pct: headers.indexOf('ear_verif_subsistema_percentual'),
  };
  if (idx.sub < 0 || idx.date < 0 || idx.pct < 0) {
    throw new Error(`Unexpected columns: ${headers.join(',')}`);
  }
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(';');
    const sub = parts[idx.sub];
    const date = parts[idx.date];
    const pct = parseFloat(parts[idx.pct]);
    if (!sub || !date || isNaN(pct)) continue;
    rows.push({ sub, date, pct });
  }
  return rows;
}

// Pick the latest value per subsystem and the value from 7 calendar days
// before it. If no exact match 7 days back (e.g. for early-year edge cases),
// fall back to the nearest earlier date within a 10-day lookback window.
function buildSubsystems(rows) {
  const bySub = {};
  for (const r of rows) {
    if (!bySub[r.sub]) bySub[r.sub] = [];
    bySub[r.sub].push(r);
  }
  const out = {};
  let latestDate = '';

  for (const [onsKey, dashKey] of Object.entries(KEY_MAP)) {
    const series = (bySub[onsKey] || []).sort((a, b) => a.date.localeCompare(b.date));
    if (!series.length) continue;
    const last = series[series.length - 1];
    if (last.date > latestDate) latestDate = last.date;

    // Find ~7-day prior value — exact match first, else nearest within [-10, -5] days
    const targetDate = new Date(last.date);
    targetDate.setUTCDate(targetDate.getUTCDate() - 7);
    const targetStr = targetDate.toISOString().slice(0, 10);

    let prior = series.find(r => r.date === targetStr);
    if (!prior) {
      // fallback: closest date in window [last-10, last-5]
      const win = series.filter(r => {
        const d = new Date(r.date);
        const diff = (new Date(last.date) - d) / 86400000;
        return diff >= 5 && diff <= 10;
      });
      prior = win.length ? win[win.length - 1] : null;
    }

    out[dashKey] = {
      current: +last.pct.toFixed(2),
      wow: prior ? +(last.pct - prior.pct).toFixed(2) : 0,
      asOf: last.date,
    };
  }

  return { subsystems: out, latestDate };
}

export default async function handler(req, res) {
  // Cache at the edge for 30 min — ONS updates twice a day
  res.setHeader('Cache-Control', 's-maxage=1800, stale-while-revalidate=3600');
  res.setHeader('Access-Control-Allow-Origin', '*');

  const year = new Date().getUTCFullYear();
  try {
    let text = await fetchCSV(year);
    let rows = parseCSV(text);

    // If the file for the current year is empty (very early Jan) or has
    // < 7 days of data, also pull the prior year so W/W doesn't land blank.
    if (rows.length < 32) {
      try {
        const prior = await fetchCSV(year - 1);
        rows = rows.concat(parseCSV(prior));
      } catch (_) { /* ignore */ }
    }

    const { subsystems, latestDate } = buildSubsystems(rows);
    if (!Object.keys(subsystems).length) {
      return res.status(502).json({ ok: false, error: 'No subsystem data parsed' });
    }

    return res.status(200).json({
      ok: true,
      subsystems,
      updatedAt: latestDate,
      source: `ONS EAR Diário por Subsistema (${year})`,
    });
  } catch (err) {
    return res.status(502).json({ ok: false, error: err.message });
  }
}
