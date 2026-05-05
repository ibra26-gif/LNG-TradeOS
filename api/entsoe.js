// api/entsoe.js — Vercel serverless proxy for ENTSO-E Transparency Platform
// Save as: /api/entsoe.js in your Vercel project root
// Add ENTSOE_API_KEY or ENTSOE_SECURITY_TOKEN to Vercel environment variables
// (Settings → Environment Variables). Never expose the token to browser JS.
//
// Usage from browser:
//   /api/entsoe?documentType=A75&processType=A16&in_Domain=10YFR-RTE------C&psrType=B14&periodStart=202604010000&periodEnd=202604122300

export default async function handler(req, res) {
  // CORS headers — allow requests from lngtradeos.com
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const apiKey = process.env.ENTSOE_API_KEY || process.env.ENTSOE_SECURITY_TOKEN;
  if (!apiKey) {
    return res.status(500).json({ error: 'ENTSOE_API_KEY / ENTSOE_SECURITY_TOKEN not configured in Vercel environment variables' });
  }

  // Forward all query params from the browser request, plus the security token
  const params = new URLSearchParams(req.query);
  params.delete('securityToken');
  params.set('securityToken', apiKey);

  const url = `https://web-api.tp.entsoe.eu/api?${params.toString()}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);

  try {
    const response = await fetch(url, {
      headers: { 'Accept': 'application/xml, text/xml, */*' },
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (!response.ok) {
      const errText = await response.text();
      res.setHeader('Cache-Control', 'no-store');
      return res.status(response.status).send(errText);
    }

    const xml = await response.text();

    // Return raw XML — the browser-side code parses it with regex
    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    res.setHeader('X-LNGTradeOS-Proxy', 'entsoe-configured');
    // Vercel CDN cache: ENTSO-E actual generation is source-published, not tick data.
    res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
    res.setHeader('CDN-Cache-Control', 'public, max-age=3600');
    res.setHeader('Vercel-CDN-Cache-Control', 'public, max-age=3600, stale-while-revalidate=86400');
    return res.status(200).send(xml);

  } catch (err) {
    clearTimeout(timeout);
    res.setHeader('Cache-Control', 'no-store');
    return res.status(502).json({ error: 'ENTSO-E upstream error: ' + err.message });
  }
}
