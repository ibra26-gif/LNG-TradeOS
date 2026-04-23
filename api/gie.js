// Unified GIE proxy: AGSI+ (gas storage) and ALSI+ (LNG terminals / aggi).
// One serverless function handles both to stay under Hobby plan's 12-function cap.
// Vercel rewrites map /api/agsi → /api/gie?src=agsi and /api/aggi → /api/gie?src=aggi,
// so no client code changes are required.

const KEY = process.env.AGGI_API_KEY || process.env.AGSI_API_KEY;
const BASES = {
  agsi: 'https://agsi.gie.eu/api',
  aggi: 'https://alsi.gie.eu/api',
};

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (!KEY) {
    res.status(500).json({ error: 'GIE API key not set (AGGI_API_KEY / AGSI_API_KEY).' });
    return;
  }
  const { src = 'agsi', ...rest } = req.query;
  const base = BASES[src];
  if (!base) { res.status(400).json({ error: `unknown src=${src}` }); return; }
  const params = new URLSearchParams(rest).toString();
  const url = `${base}${params ? '?' + params : ''}`;
  try {
    const r = await fetch(url, {
      headers: { 'x-key': KEY, 'Content-Type': 'application/json' },
    });
    const data = await r.json();
    res.status(r.ok ? 200 : r.status).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
