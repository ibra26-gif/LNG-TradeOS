// Vercel Serverless Function: ENTSOG Transparency Platform Proxy
// Avoids CORS failures when ENTSOG returns 502/504 without Access-Control headers.
// Deploy at: api/entsog.js in your Vercel project root.
//
// Vercel Hobby plan: 10s max execution. Keep single-attempt, let client retry.

export default async function handler(req, res) {
  // CORS headers on ALL responses (this is the whole point of the proxy)
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Accept, Content-Type');
  res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=7200');

  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const { endpoint, ...params } = req.query;
    const endpointMap = {
      operationaldata: 'operationalDatas',
      connectionpoints: 'connectionPoints',
      aggregateddata:   'aggregatedData',
    };
    const entsogPath = endpointMap[endpoint] || 'operationalDatas';
    // CRITICAL: ENTSOG requires %20 for spaces, not + (URLSearchParams default).
    // "indicator=Physical+Flow" returns no results; "indicator=Physical%20Flow" works.
    const qs = new URLSearchParams(params).toString().replace(/\+/g, '%20');
    const url = `https://transparency.entsog.eu/api/v1/${entsogPath}?${qs}`;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000); // 8s hard limit (leaves 2s buffer for Vercel)

    const entsogRes = await fetch(url, {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (!entsogRes.ok) {
      const errText = await entsogRes.text().catch(() => '');
      return res.status(entsogRes.status).json({
        error: `ENTSOG ${entsogRes.status}`,
        detail: errText.slice(0, 200),
      });
    }

    const data = await entsogRes.json();
    return res.status(200).json(data);
  } catch (e) {
    const status = e.name === 'AbortError' ? 504 : 502;
    return res.status(status).json({
      error: e.name === 'AbortError' ? 'ENTSOG timeout (8s)' : e.message,
    });
  }
}
