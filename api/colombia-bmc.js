// api/colombia-bmc.js — Vercel Serverless Function
// Serves Colombia gas balance from data/colombia_gas_balance.csv (long format,
// scraped monthly by scripts/colombia_bmc_fetch.py from BMC monthly reports).
//
// CSV schema:
//   month,kind,id,gbtud
//   2026-02,supply,piedemonte_llanero,414
//   2026-02,supply,lng_cartagena,203
//   2026-02,demand,industrial,173
//   2026-02,supply_total,national,889
//   ...
//
// Maps to the platform's CO_D row format:
//   { rows: [ {id:'prod_llan', lbl, s:'sup', v:[12 monthly values in MCM/D]}, ... ] }
//
// 1 GBTUD ≈ 0.0283 MCM/D (1 GBTU/day at standard heating value).

import { readFileSync } from 'fs';
import { join } from 'path';

const CSV_PATH = join(process.cwd(), 'data', 'colombia_gas_balance.csv');

const GBTUD_TO_MCMD = 0.0283;

// Map platform CO_D row id → list of BMC field ids that aggregate into it.
// Supply:
const SUPPLY_MAP = {
  prod_guaj: { lbl: 'La Guajira offshore (Chuchupa + Ballena)', ids: ['ballena_chuchupa'] },
  prod_llan: { lbl: 'Cusiana / Cupiagua / Piedemonte (Ecopetrol)', ids: ['piedemonte_llanero'] },
  prod_oth:  { lbl: 'Other domestic fields (Costa + Interior)',
               ids: ['gibraltar','istanbul','otros_interior','arrecife','bloque_vim',
                     'bonga_mamey','bullerengue','magico','otros_costa'] },
  lng:       { lbl: 'LNG imports (SPEC Cartagena FSRU)', ids: ['lng_cartagena'] },
};
// Demand:
const DEMAND_MAP = {
  res: { lbl: 'Residential',                          ids: ['residencial'] },
  com: { lbl: 'Commercial',                           ids: ['comercial'] },
  ind: { lbl: 'Industrial',                           ids: ['industrial'] },
  pow: { lbl: 'Thermal power generation',             ids: ['termoelectrica'] },
  gnv: { lbl: 'Automotive (GNV / CNG)',               ids: ['gnv'] },
  ref: { lbl: 'Refineries (Barrancabermeja + Cartagena)', ids: ['refineria'] },
  oth: { lbl: 'Other (petroleum / petrochem / compressors)',
         ids: ['petrolero','petroquimica','compresoras'] },
};

function parseCSV(text) {
  // Returns { months: Set, byKey: Map<"month|kind|id", number> }
  const lines = text.trim().split(/\r?\n/);
  const months = new Set();
  const byKey = new Map();
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    if (parts.length < 4) continue;
    const [m, k, id, vRaw] = parts;
    const v = parseFloat(vRaw);
    if (!isFinite(v)) continue;
    months.add(m);
    byKey.set(`${m}|${k}|${id}`, v);
  }
  return { months, byKey };
}

function lastNMonths(months, n) {
  const sorted = [...months].sort();
  return sorted.slice(-n);
}

function aggGbtud(byKey, month, kind, ids) {
  let sum = 0;
  let any = false;
  for (const id of ids) {
    const v = byKey.get(`${month}|${kind}|${id}`);
    if (v != null) { sum += v; any = true; }
  }
  return any ? sum : null;
}

export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=86400');

  try {
    const text = readFileSync(CSV_PATH, 'utf-8');
    const { months, byKey } = parseCSV(text);

    // Pick the latest 12 months that exist in the CSV (some rows are
    // supply_total only, but that still constitutes a month). The frontend
    // table is fixed-length 12; missing values render as 0 / "—".
    const last12 = lastNMonths(months, 12);

    const rows = [];
    // Supply rows — null for months with no breakdown (frontend renders as —)
    for (const [coId, def] of Object.entries(SUPPLY_MAP)) {
      const v = last12.map(m => {
        const g = aggGbtud(byKey, m, 'supply', def.ids);
        return g == null ? null : +(g * GBTUD_TO_MCMD).toFixed(2);
      });
      rows.push({ id: coId, lbl: def.lbl, s: 'sup', v });
    }
    // Demand rows
    for (const [coId, def] of Object.entries(DEMAND_MAP)) {
      const v = last12.map(m => {
        const g = aggGbtud(byKey, m, 'demand', def.ids);
        return g == null ? null : +(g * GBTUD_TO_MCMD).toFixed(2);
      });
      rows.push({ id: coId, lbl: def.lbl, s: 'dem', v });
    }

    return res.status(200).json({
      ok: true,
      rows,
      months: last12,
      meta: {
        source: 'BMC monthly reports (Bolsa Mercantil de Colombia)',
        rowCount: rows.length,
        coverageMonths: last12.length,
        latest: last12[last12.length - 1] || null,
      },
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
}
