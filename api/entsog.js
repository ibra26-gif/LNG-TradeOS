// Vercel serverless proxy for ENTSOG Transparency Platform API
// No API key required — public API
// Base URL: https://transparency.entsog.eu/api/v1
// CRITICAL: forward raw query string — do NOT rebuild with URLSearchParams.
// ENTSOG uses literal commas as multi-value separators (e.g. pointLabel=A,B,C).
// URLSearchParams encodes commas as %2C which breaks point matching → 404.
const ENTSOG_BASE = 'https://transparency.entsog.eu/api/v1';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { endpoint } = req.query;
  if (!endpoint) {
    res.status(400).json({ error: 'Missing endpoint param. E.g. ?endpoint=operationaldata' });
    return;
  }
  const ALLOWED = [
    'operationaldata', 'operationaldatas',
    'operators', 'connectionpoints', 'interconnections',
    'urgentmarketmessages', 'cmpUnsuccessfulRequests'
  ];
  if (!ALLOWED.includes(endpoint.toLowerCase())) {
    res.status(400).json({ error: `Endpoint not allowed: ${endpoint}` });
    return;
  }

  // Take the raw query string from req.url, strip the endpoint= param,
  // then forward everything else verbatim so commas stay as commas.
  const rawQuery = (req.url || '').split('?').slice(1).join('?');
  const stripped = rawQuery
    .replace(/(?:^|&)endpoint=[^&]*/g, '')
    .replace(/^&/, '');
  const hasLimit = /\blimit=/.test(stripped);
  const finalQuery = stripped + (hasLimit ? '' : '&limit=10000');
  const url = `${ENTSOG_BASE}/${endpoint}?${finalQuery}`;

  try {
    const r = await fetch(url, { headers: { Accept: 'application/json' } });
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
