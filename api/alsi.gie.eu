const AGGI_KEY = process.env.AGGI_API_KEY;
const AGGI_BASE = 'https://alsi.gie.eu/api';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (!AGGI_KEY) {
    res.status(500).json({ error: 'AGGI_API_KEY not set in Vercel environment variables.' });
    return;
  }
  const params = new URLSearchParams(req.query).toString();
  const url = `${AGGI_BASE}${params ? '?' + params : ''}`;
  try {
    const r = await fetch(url, {
      headers: { 'x-key': AGGI_KEY, 'Content-Type': 'application/json' }
    });
    const data = await r.json();
    res.status(r.ok ? 200 : r.status).json(data);
  } catch (err) {
    console.error('ALSI proxy error:', err);
    res.status(500).json({ error: err.message });
  }
};
