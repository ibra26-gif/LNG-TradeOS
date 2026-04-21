// api/fx-cny.js — Vercel Serverless Function
// Fetches USD/CNY daily FX rate from a free source (open.er-api.com).
// Used by the dashboard to auto-populate the USD/CNY input (previously manual).
//
// Response shape:
//   { ok: true, rate: 6.8314, asOf: "2026-04-21", source: "open.er-api.com" }
//
// Cached at Vercel edge for 12h so we don't hammer the FX provider.

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 's-maxage=43200, stale-while-revalidate=86400');
  res.setHeader('Access-Control-Allow-Origin', '*');

  try {
    const resp = await fetch('https://open.er-api.com/v6/latest/USD', {
      headers: { 'User-Agent': 'LNG-TradeOS/1.0' },
      signal: AbortSignal.timeout(8000),
    });
    if (!resp.ok) throw new Error(`FX HTTP ${resp.status}`);
    const j = await resp.json();
    const cny = j?.rates?.CNY;
    if (typeof cny !== 'number' || cny < 5 || cny > 10) {
      throw new Error(`Invalid CNY rate: ${cny}`);
    }
    // Prefer structured date from the provider if present, else today
    const asOf = (j?.time_last_update_utc || '').slice(0, 16) || new Date().toISOString().slice(0, 10);
    return res.status(200).json({
      ok: true,
      rate: +cny.toFixed(4),
      asOf,
      source: 'open.er-api.com',
    });
  } catch (err) {
    return res.status(502).json({ ok: false, error: err.message });
  }
}
