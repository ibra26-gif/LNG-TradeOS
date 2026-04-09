const AGSI_KEY = process.env.AGSI_API_KEY;
const AGSI_BASE = 'https://agsi.gie.eu/api';
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (!AGSI_KEY) {
    res.status(500).json({ error: 'AGSI_API_KEY not set in Vercel environment variables.' });
    return;
  }
  const params = new URLSearchParams(req.query).toString();
  const url = `${AGSI_BASE}${params ? '?' + params : ''}`;
  try {
    const r = await fetch(url, {
      headers: { 'x-key': AGSI_KEY, 'Content-Type': 'application/json' }
    });
    const data = await r.json();
    res.status(r.ok ? 200 : r.status).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
