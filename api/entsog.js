// api/entsog.js — Vercel serverless proxy for ENTSOG Transparency Platform
// Avoids CORS failures when ENTSOG returns 502/504 without Access-Control headers.
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Accept, Content-Type');
  res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=7200');

  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    // Extract 'endpoint' to build the ENTSOG path, forward everything else as query params
    const { endpoint, ...params } = req.query;
    const endpointMap = {
      operationaldata: 'operationalDatas',
      connectionpoints: 'connectionPoints',
      aggregateddata:   'aggregatedData',
    };
    const entsogPath = endpointMap[endpoint] || 'operationalDatas';
    const qs = new URLSearchParams(params).toString();
    const url = `https://transparency.entsog.eu/api/v1/${entsogPath}?${qs}`;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);

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
