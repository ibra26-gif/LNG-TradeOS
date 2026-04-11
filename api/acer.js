/**
 * api/acer.js  —  Vercel serverless proxy for ACER TERMINAL
 *
 * Proxies requests to aegis.acer.europa.eu (CORS-blocked from browser).
 * All endpoints are publicly accessible — no auth required.
 *
 * Usage:
 *   GET /api/acer?action=list
 *       → Returns array of { id, date, filename } for all published PDFs
 *
 *   GET /api/acer?action=pdf&id=257
 *       → Proxies the raw PDF binary for file ID 257
 *
 *   GET /api/acer?action=csv
 *       → Proxies the full historical price CSV from TERMINAL
 *
 * Source:
 *   aegis.acer.europa.eu/terminal/price_assessments
 *   PDFs at:  aegis.acer.europa.eu/terminal/price_assessments/file/{N}
 *   CSV at:   aegis.acer.europa.eu/terminal/price_assessments (download historical data link)
 */

export const config = { runtime: 'edge' };

const BASE = 'https://aegis.acer.europa.eu/terminal/price_assessments';

export default async function handler(req) {
  const { searchParams } = new URL(req.url);
  const action = searchParams.get('action') || 'list';

  const headers = {
    'User-Agent': 'Mozilla/5.0 (compatible; LNGTradeOS/1.0)',
    'Accept': '*/*',
  };

  try {
    // ── LIST: scrape the TERMINAL index page for all available file IDs ──
    if (action === 'list') {
      const res = await fetch(BASE, { headers });
     if(!f)throw new Error("No YYMMDD_LNGPA.pdf — upload to the \"ACER Report\" Drive folder first");
      const html = await res.text();

      // Extract file links: /terminal/price_assessments/file/{N}
      const matches = [...html.matchAll(/\/terminal\/price_assessments\/file\/(\d+)/g)];
      const ids = [...new Set(matches.map(m => parseInt(m[1])))].sort((a, b) => b - a);

      // Try to extract dates from nearby text — ACER page uses DD Mon YYYY format
      // Build structured list from what's available
      const files = ids.map(id => ({ id }));

      return new Response(JSON.stringify({ files, total: files.length }), {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=3600',
        },
      });
    }

    // ── PDF: proxy a specific PDF file by ID ──────────────────────────────
    if (action === 'pdf') {
      const id = searchParams.get('id');
      if (!id || !/^\d+$/.test(id)) {
        return new Response('Invalid id', { status: 400 });
      }

      const url = `${BASE}/file/${id}`;
      const res = await fetch(url, { headers });

      if (!res.ok) {
        return new Response(`ACER PDF fetch failed: ${res.status}`, { status: res.status });
      }

      // Stream the PDF back
      return new Response(res.body, {
        headers: {
          'Content-Type': 'application/pdf',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=86400',
        },
      });
    }

    // ── CSV: proxy the historical price CSV download ───────────────────────
    if (action === 'csv') {
      // The CSV download endpoint — may require finding the exact link from the page
      const csvUrl = `${BASE}/historical-data/csv`;
      const res = await fetch(csvUrl, { headers });

      if (!res.ok) {
        // Fallback: try the page itself and look for the CSV link
        const pageRes = await fetch(BASE, { headers });
        const html = await pageRes.text();
        const csvMatch = html.match(/href="([^"]*\.csv[^"]*)"/i);
        if (!csvMatch) {
          return new Response('CSV link not found on ACER page', { status: 404 });
        }
        const fullUrl = csvMatch[1].startsWith('http')
          ? csvMatch[1]
          : `https://aegis.acer.europa.eu${csvMatch[1]}`;
        const csvRes = await fetch(fullUrl, { headers });
        return new Response(csvRes.body, {
          headers: {
            'Content-Type': 'text/csv',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=3600',
          },
        });
      }

      return new Response(res.body, {
        headers: {
          'Content-Type': 'text/csv',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=3600',
        },
      });
    }

    return new Response('Unknown action. Use: list | pdf&id=N | csv', { status: 400 });

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
