// api/acer.js — Vercel Serverless Function
// Serves the ACER TERMINAL physical price assessment CSV from the committed
// data/acer_historical.csv. Vercel serverless cannot reach
// aegis.acer.europa.eu reliably (502 / fetch failed) so we ship a static
// snapshot that is refreshed daily by scripts/acer_fetch.py running on the
// laptop (launchd at 19:00 CET, after ACER publishes EOD ~18:30 CET).
//
// CSV format (preserved from upstream):
//   "DATE","NORTH-WEST EUROPE PRICE (EUR/MWh)","SOUTH EUROPE PRICE (EUR/MWh)",
//   "EU PRICE (EUR/MWh)","LNG BENCHMARK (EUR/MWh)"
//
// Response:
//   GET /api/acer?action=csv  →  text/csv (passes through)
//   GET /api/acer             →  same as ?action=csv

import { readFileSync } from 'fs';
import { join } from 'path';

const CSV_PATH = join(process.cwd(), 'data', 'acer_historical.csv');

export default function handler(req, res) {
  const action = (req.query?.action) || 'csv';
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (action !== 'csv') {
    res.status(400).send('Unknown action. Only ?action=csv is supported.');
    return;
  }

  try {
    const text = readFileSync(CSV_PATH, 'utf-8');
    const rows = text.trim().split(/\r?\n/).slice(1);
    const latest = rows[0]?.split(',')?.[0]?.replace(/"/g, '') || '';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    if (latest) res.setHeader('X-LNGTradeOS-Latest-Date', latest);
    res.setHeader('X-LNGTradeOS-Row-Count', String(rows.length));
    res.setHeader('Cache-Control', 'public, s-maxage=1800, stale-while-revalidate=7200');
    res.status(200).send(text);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}
