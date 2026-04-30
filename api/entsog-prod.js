// api/entsog-prod.js — Vercel serverless proxy for ENTSOG domestic-production
// physical-flow rows. ENTSOG's transparency API does NOT send CORS headers,
// so browsers cannot read its responses directly — we round-trip through
// Vercel to add the Access-Control-Allow-Origin header.
//
// Query:  GET /api/entsog-prod?from=YYYY-MM-DD&to=YYYY-MM-DD
// Returns: { rows: [...merged EU + ExtEU aggregated production rows...] }
//
// Filters:
//   pointType=Aggregated production point - TP        (EU reporters)
//   pointType=Aggregated production point - TP ExtEU  (UK post-Brexit, etc.)
// Keeps only the fields the front-end actually consumes: operatorKey,
// pointLabel, periodFrom, value, unit, pointType.

const ENTSOG = 'https://transparency.entsog.eu/api/v1/operationalDatas';
const UA = 'Mozilla/5.0 (Vercel LNG TradeOS) AppleWebKit/537.36';

async function fetchOne(ptype, from, to) {
  const params = new URLSearchParams({
    indicator: 'Physical Flow',
    pointType: ptype,
    from, to,
    periodType: 'day',
    timeZone: 'WET',
    limit: '3000',
  }).toString();
  const r = await fetch(`${ENTSOG}?${params}`, {
    headers: { 'Accept': 'application/json', 'User-Agent': UA },
  });
  if (!r.ok) throw new Error(`ENTSOG ${r.status}`);
  const j = await r.json();
  return j.operationalDatas || j.operationaldatas || j.data || [];
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=86400');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const from = (req.query.from || '').slice(0, 10);
  const to   = (req.query.to   || '').slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
    return res.status(400).json({ error: 'Pass from=YYYY-MM-DD&to=YYYY-MM-DD' });
  }

  try {
    const [a, b] = await Promise.all([
      fetchOne('Aggregated production point - TP',       from, to),
      fetchOne('Aggregated production point - TP ExtEU', from, to),
    ]);
    const seen = new Set();
    const rows = [...a, ...b].map(r => ({
      operatorKey: r.operatorKey,
      pointLabel:  r.pointLabel,
      periodFrom:  r.periodFrom,
      value:       r.value,
      unit:        r.unit,
      pointType:   r.pointType,
    })).filter(r => {
      const key = `${r.operatorKey}|${r.pointLabel}|${r.periodFrom}|${r.pointType}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    res.status(200).json({ rows, meta: { from, to, count: rows.length } });
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
}
