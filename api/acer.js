/**
 * api/acer.js  —  Vercel serverless proxy for ACER TERMINAL
 *
 * Proxies requests to aegis.acer.europa.eu (CORS-blocked from browser).
 * All endpoints are publicly accessible — no auth required.
 *
 * Usage:
 *   GET /api/acer?action=csv
 *       → Proxies the full historical price CSV (DATE, NWE, SE, EU, BENCHMARK)
 *   GET /api/acer?action=list
 *       → Returns array of available PDF file IDs
 *   GET /api/acer?action=pdf&id=257
 *       → Proxies a specific PDF
 *
 * Source: https://aegis.acer.europa.eu/terminal/price_assessments
 */

// Use the Node.js serverless runtime — the edge runtime was returning 502s
// when proxying ACER (likely TLS/bot-detection differences on edge IPs).
const BASE = 'https://aegis.acer.europa.eu/terminal/price_assessments';
const CSV_URL = `${BASE}/historical_data`;

const UPSTREAM_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Accept': 'text/csv,*/*;q=0.9',
  'Accept-Language': 'en-US,en;q=0.9',
};

export default async function handler(req, res) {
  const action = (req.query?.action) || 'csv';
  res.setHeader('Access-Control-Allow-Origin', '*');

  try {
    if (action === 'csv') {
      const upstream = await fetch(CSV_URL, { headers: UPSTREAM_HEADERS });
      if (!upstream.ok) {
        res.status(upstream.status).send(`ACER CSV fetch failed: ${upstream.status}`);
        return;
      }
      const text = await upstream.text();
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Cache-Control', 'public, s-maxage=1800, stale-while-revalidate=7200');
      res.status(200).send(text);
      return;
    }

    if (action === 'list') {
      const upstream = await fetch(BASE, { headers: UPSTREAM_HEADERS });
      if (!upstream.ok) {
        res.status(upstream.status).send(`ACER index fetch failed: ${upstream.status}`);
        return;
      }
      const html = await upstream.text();
      const matches = [...html.matchAll(/\/terminal\/price_assessments\/file\/(\d+)/g)];
      const ids = [...new Set(matches.map(m => parseInt(m[1], 10)))].sort((a, b) => b - a);
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Cache-Control', 'public, max-age=3600');
      res.status(200).json({ files: ids.map(id => ({ id })), total: ids.length });
      return;
    }

    if (action === 'pdf') {
      const id = req.query?.id;
      if (!id || !/^\d+$/.test(String(id))) {
        res.status(400).send('Invalid id');
        return;
      }
      const upstream = await fetch(`${BASE}/file/${id}`, { headers: UPSTREAM_HEADERS });
      if (!upstream.ok) {
        res.status(upstream.status).send(`ACER PDF fetch failed: ${upstream.status}`);
        return;
      }
      const buf = Buffer.from(await upstream.arrayBuffer());
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.status(200).send(buf);
      return;
    }

    res.status(400).send('Unknown action. Use: csv | list | pdf&id=N');
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
}
