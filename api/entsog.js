// Vercel serverless proxy for ENTSOG Transparency Platform API
// No API key required — public API
// Base URL: https://transparency.entsog.eu/api/v1
// Attribution: "Source: ENTSOG Transparency Platform" on all outputs.
// Docs: https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_Gas_version5.1.1.pdf

const ENTSOG_BASE = 'https://transparency.entsog.eu/api/v1';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  // Build endpoint from query — e.g. ?endpoint=operationaldata&...
  const { endpoint, ...rest } = req.query;
  if (!endpoint) {
    res.status(400).json({ error: 'Missing endpoint param. E.g. ?endpoint=operationaldata' });
    return;
  }

  // Whitelist endpoints for safety
  const ALLOWED = ['operationaldata','operators','points','interconnections','urgentmarketmessages','cmpUnsuccessfulRequests'];
  if (!ALLOWED.includes(endpoint)) {
    res.status(400).json({ error: `Endpoint not allowed: ${endpoint}` });
    return;
  }

  // Always request JSON
  const params = new URLSearchParams({ ...rest, limit: rest.limit || 10000, offset: rest.offset || 0 });
  const url = `${ENTSOG_BASE}/${endpoint}?${params}`;

  try {
    const r = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (!r.ok) {
      const text = await r.text();
      res.status(r.status).json({ error: `ENTSOG ${r.status}`, detail: text.slice(0, 500) });
      return;
    }
    const data = await r.json();
    res.status(200).json(data);
  } catch (err) {
    console.error('ENTSOG proxy error:', err);
    res.status(500).json({ error: err.message });
  }
};
