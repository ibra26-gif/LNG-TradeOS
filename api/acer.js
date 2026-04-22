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

export const config = { runtime: 'edge' };

const BASE = 'https://aegis.acer.europa.eu/terminal/price_assessments';
const CSV_URL = `${BASE}/historical_data`;

const UPSTREAM_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (compatible; LNGTradeOS/1.0)',
  'Accept': '*/*',
};

export default async function handler(req) {
  const { searchParams } = new URL(req.url);
  const action = searchParams.get('action') || 'csv';

  try {
    if (action === 'csv') {
      const res = await fetch(CSV_URL, { headers: UPSTREAM_HEADERS });
      if (!res.ok) {
        return new Response(`ACER CSV fetch failed: ${res.status}`, { status: res.status });
      }
      const text = await res.text();
      return new Response(text, {
        headers: {
          'Content-Type': 'text/csv; charset=utf-8',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, s-maxage=1800, stale-while-revalidate=7200',
        },
      });
    }

    if (action === 'list') {
      const res = await fetch(BASE, { headers: UPSTREAM_HEADERS });
      if (!res.ok) {
        return new Response(`ACER index fetch failed: ${res.status}`, { status: res.status });
      }
      const html = await res.text();
      const matches = [...html.matchAll(/\/terminal\/price_assessments\/file\/(\d+)/g)];
      const ids = [...new Set(matches.map(m => parseInt(m[1], 10)))].sort((a, b) => b - a);
      return new Response(JSON.stringify({ files: ids.map(id => ({ id })), total: ids.length }), {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=3600',
        },
      });
    }

    if (action === 'pdf') {
      const id = searchParams.get('id');
      if (!id || !/^\d+$/.test(id)) {
        return new Response('Invalid id', { status: 400 });
      }
      const res = await fetch(`${BASE}/file/${id}`, { headers: UPSTREAM_HEADERS });
      if (!res.ok) {
        return new Response(`ACER PDF fetch failed: ${res.status}`, { status: res.status });
      }
      return new Response(res.body, {
        headers: {
          'Content-Type': 'application/pdf',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=86400',
        },
      });
    }

    return new Response('Unknown action. Use: csv | list | pdf&id=N', { status: 400 });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 502,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  }
}
