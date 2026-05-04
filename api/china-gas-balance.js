// api/china-gas-balance.js — Vercel Serverless Function
// Serves monthly China gas balance from the committed CSV at
// data/china_gas_balance.csv. The CSV is maintained manually: on the 16th
// of each month, run china_gas_balance_fetch.py after updating VERIFIED
// with fresh NBS (~16th) and GACC (~10th) releases, then commit the CSV.
//
// CSV columns: month,prod_bcm,lng_mt,pipe_mt,lng_bcm,pipe_bcm,total_bcm,apparent_bcm
// Response rows preserve blanks as null. The frontend must not infer missing
// China demand or storage from seasonal estimates.

import { readFileSync } from 'fs';
import { join } from 'path';

const CSV_PATH = join(process.cwd(), 'data', 'china_gas_balance.csv');

function parseCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  const hdr = lines[0].split(',').map(h => h.trim());
  const ix = Object.fromEntries(hdr.map((h, i) => [h, i]));
  const val = (cells, key) => {
    const pos = ix[key];
    if (pos == null) return null;
    const raw = String(cells[pos] ?? '').trim();
    if (!raw) return null;
    const parsed = parseFloat(raw.replace(/,/g, ''));
    return Number.isFinite(parsed) ? parsed : null;
  };
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(',');
    if (cells.length < hdr.length) continue;
    const month = String(cells[ix.month] ?? '').trim();
    if (!/^\d{4}-\d{2}$/.test(month)) continue;
    const prod = val(cells, 'prod_bcm');
    const lng = val(cells, 'lng_bcm');
    const pipe = val(cells, 'pipe_bcm');
    const total = val(cells, 'total_bcm');
    const apparent = val(cells, 'apparent_bcm');
    if ([prod, lng, pipe, total, apparent].every(v => v == null)) continue;
    rows.push({
      m: month,
      month,
      prod,
      lng,
      pipe,
      prod_bcm: prod,
      lng_bcm: lng,
      pipe_bcm: pipe,
      lng_mt: val(cells, 'lng_mt'),
      pipe_mt: val(cells, 'pipe_mt'),
      total_bcm: total,
      apparent_bcm: apparent,
    });
  }
  return rows;
}

export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=86400');

  try {
    const text = readFileSync(CSV_PATH, 'utf-8');
    const rows = parseCSV(text);
    return res.status(200).json({
      ok: true,
      rows,
      source: `Committed CSV · ${rows.length} months (${rows[0]?.m} → ${rows[rows.length-1]?.m})`,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
}
