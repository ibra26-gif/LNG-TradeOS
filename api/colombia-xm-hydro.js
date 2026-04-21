// api/colombia-xm-hydro.js — Vercel Serverless Function
// Fetches daily reservoir useful-volume % for the Colombian SIN from SIMEM (XM open data).
// Source: SIMEM dataset c51127 — Volumen Útil Diario / Capacidad Útil (energy GWh-equiv per embalse)
//   https://www.simem.co/backend-files/api/PublicData?datasetId=c51127
// Publishes daily ~06:00 UTC.
//
// Why this dataset and not 843497:
//   Dataset 843497 exposes `VolumenUtilPorcentaje` directly, but its `AGREGADO` row is
//   a mislabeled internal aggregate that does NOT match XM's publicly-reported SIN %
//   (e.g. 843497 said 55.14% on 2026-04-16; XM publicly reported 61.63% same date).
//   XM's "Embalse Agregado" is energy-weighted: sum(VolumenUtilDiarioEnergia) /
//   sum(CapacidadUtilEnergia) across all embalses. Computing that from c51127
//   reproduces XM's public numbers exactly. For individual embalses we also use
//   VU_energy / CapUtil_energy (consistent units with the aggregate).
//
// Response shape consumed by the dashboard (samLoadColombiaHydro in lngtradeos.js):
//   { reservoirs: { agregado:{current,wow,asOf}, penol:{...}, guavio:{...}, punchina:{...} },
//     updatedAt, source }

const SIMEM_URL = 'https://www.simem.co/backend-files/api/PublicData';
const DATASET_ID = 'c51127';

// Per-embalse codes we surface. `agregado` is computed across *all* embalses, not a row.
const EMBALSE_MAP = {
  PENOL:    'penol',     // Peñol-Guatapé · EPM
  GUAVIO:   'guavio',    // Guavio · ENEL Emgesa
  PUNCHINA: 'punchina',  // Punchiná · ISAGEN
};

function fmtDate(d) {
  return d.toISOString().slice(0, 10);
}

async function fetchSIMEM(startDate, endDate) {
  const url = `${SIMEM_URL}?startDate=${startDate}&endDate=${endDate}&datasetId=${DATASET_ID}`;
  const resp = await fetch(url, {
    headers: { 'User-Agent': 'LNG-TradeOS/1.0', 'Accept': 'application/json' },
    signal: AbortSignal.timeout(15000),
  });
  if (!resp.ok) throw new Error(`SIMEM HTTP ${resp.status}`);
  const body = await resp.json();
  if (!body || body.success === false) {
    throw new Error(`SIMEM error: ${body?.message || 'unknown'}`);
  }
  const records = body?.result?.records;
  if (!Array.isArray(records)) throw new Error('SIMEM: missing result.records');
  return records;
}

// Build two time series:
//   1. Per-date SIN aggregate: sum(VU_energy) / sum(CapUtil_energy) across all
//      embalses EXCEPT the mislabeled AGREGADO row.
//   2. Per-date per-embalse % for the named codes (VU / CapUtil).
function buildSeries(records) {
  const aggBuckets = {}; // fecha → { vu, cap }
  const embSeries  = {}; // code  → [{fecha, pct}, ...]

  for (const r of records) {
    const code = r.CodigoEmbalse;
    const fecha = r.Fecha;
    const vu = r.VolumenUtilDiarioEnergia;
    const cap = r.CapacidadUtilEnergia;
    if (vu == null || cap == null || !cap) continue;

    if (code !== 'AGREGADO') {
      if (!aggBuckets[fecha]) aggBuckets[fecha] = { vu: 0, cap: 0 };
      aggBuckets[fecha].vu  += vu;
      aggBuckets[fecha].cap += cap;

      if (EMBALSE_MAP[code]) {
        if (!embSeries[code]) embSeries[code] = [];
        embSeries[code].push({ date: fecha, pct: (vu / cap) * 100 });
      }
    }
  }

  // Aggregate series as {date, pct}
  const aggSeries = Object.entries(aggBuckets)
    .map(([date, v]) => ({ date, pct: (v.vu / v.cap) * 100 }))
    .sort((a, b) => a.date.localeCompare(b.date));

  for (const k of Object.keys(embSeries)) {
    embSeries[k].sort((a, b) => a.date.localeCompare(b.date));
  }

  return { aggSeries, embSeries };
}

// From a time series sorted by date, produce { current, wow, asOf }.
function latestWithWow(series) {
  if (!series.length) return null;
  const last = series[series.length - 1];
  const target = new Date(last.date);
  target.setUTCDate(target.getUTCDate() - 7);
  const targetStr = target.toISOString().slice(0, 10);

  let prior = series.find(r => r.date === targetStr);
  if (!prior) {
    const win = series.filter(r => {
      const diff = (new Date(last.date) - new Date(r.date)) / 86400000;
      return diff >= 5 && diff <= 10;
    });
    prior = win.length ? win[win.length - 1] : null;
  }
  return {
    current: +last.pct.toFixed(2),
    wow: prior ? +(last.pct - prior.pct).toFixed(2) : 0,
    asOf: last.date,
  };
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 's-maxage=1800, stale-while-revalidate=3600');
  res.setHeader('Access-Control-Allow-Origin', '*');

  const end = new Date();
  const start = new Date(end.getTime() - 14 * 86400000);

  try {
    const records = await fetchSIMEM(fmtDate(start), fmtDate(end));
    const { aggSeries, embSeries } = buildSeries(records);

    const reservoirs = {};
    const agg = latestWithWow(aggSeries);
    if (agg) reservoirs.agregado = agg;
    for (const [code, key] of Object.entries(EMBALSE_MAP)) {
      const s = latestWithWow(embSeries[code] || []);
      if (s) reservoirs[key] = s;
    }

    if (!Object.keys(reservoirs).length) {
      return res.status(502).json({ ok: false, error: 'No reservoir data parsed' });
    }

    return res.status(200).json({
      ok: true,
      reservoirs,
      updatedAt: agg?.asOf || aggSeries[aggSeries.length - 1]?.date || null,
      source: `XM SIMEM energy-weighted aggregate (dataset ${DATASET_ID})`,
    });
  } catch (err) {
    return res.status(502).json({ ok: false, error: err.message });
  }
}
