// api/china-gas-balance.js — Vercel Serverless Function
// Serves monthly China gas balance from the committed CSV at
// data/china_gas_balance.csv. The CSV is maintained manually: on the 16th
// of each month, run china_gas_balance_fetch.py after updating VERIFIED
// with fresh NBS (~16th) and GACC (~10th) releases, then commit the CSV.
//
// CSV columns: month,prod_bcm,lng_mt,pipe_mt,lng_bcm,pipe_bcm,total_bcm,apparent_bcm
// Response rows: { m, prod, lng, pipe } — all in BCM, matching frontend expectations.

import { readFileSync } from 'fs';
import { join } from 'path';

const CSV_PATH = join(process.cwd(), 'data', 'china_gas_balance.csv');

function parseCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  const hdr = lines[0].split(',').map(h => h.trim());
  const ix = Object.fromEntries(hdr.map((h, i) => [h, i]));
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split(',');
    if (cells.length < hdr.length) continue;
    const prod = parseFloat(cells[ix.prod_bcm]);
    const lng  = parseFloat(cells[ix.lng_bcm]);
    const pipe = parseFloat(cells[ix.pipe_bcm]);
    if (isNaN(prod) && isNaN(lng) && isNaN(pipe)) continue;
    rows.push({
      m: cells[ix.month],
      prod: isNaN(prod) ? 0 : prod,
      lng:  isNaN(lng)  ? 0 : lng,
      pipe: isNaN(pipe) ? 0 : pipe,
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
