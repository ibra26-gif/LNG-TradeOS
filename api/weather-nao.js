// api/weather-nao.js — NOAA/CPC NAO CSV proxy and parser.
// NOAA CPC publishes clean CSV files, but the FTP host does not expose
// browser CORS headers. Keep the proxy narrow and source-specific.

const NOAA_NAO_OBS = 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.cdas.z500.19500101_current.csv';
const NOAA_NAO_GFS = 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.gfs.z500.120days.csv';
const NOAA_NAO_GEFS = 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.gefs.z500.120days.csv';

function parseCsv(text) {
  const lines = String(text || '').trim().split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const header = lines[0].split(',').map(s => s.trim());
  return lines.slice(1).map(line => {
    const cells = line.split(',').map(s => s.trim());
    const row = {};
    header.forEach((key, i) => { row[key] = cells[i] ?? ''; });
    return row;
  });
}

async function fetchText(url) {
  const response = await fetch(url, { headers: { Accept: 'text/csv,text/plain,*/*' } });
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(`${response.status} ${body.slice(0, 120)}`);
  }
  return response.text();
}

function numberOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function percentile(values, pct) {
  const arr = values.filter(v => Number.isFinite(v)).sort((a, b) => a - b);
  if (!arr.length) return null;
  const pos = (arr.length - 1) * pct;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return arr[lo];
  return arr[lo] + (arr[hi] - arr[lo]) * (pos - lo);
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'GET only' });

  try {
    const [obsText, gfsText, gefsText] = await Promise.all([
      fetchText(NOAA_NAO_OBS),
      fetchText(NOAA_NAO_GFS),
      fetchText(NOAA_NAO_GEFS),
    ]);

    const observed = parseCsv(obsText)
      .map(row => {
        const year = String(row.year || '').padStart(4, '0');
        const month = String(row.month || '').padStart(2, '0');
        const day = String(row.day || '').padStart(2, '0');
        return { date: `${year}-${month}-${day}`, value: numberOrNull(row.nao_index_cdas) };
      })
      .filter(row => /^\d{4}-\d{2}-\d{2}$/.test(row.date) && row.value != null)
      .slice(-370);

    const gfs = parseCsv(gfsText)
      .map(row => ({
        lead: numberOrNull(row.lead),
        date: String(row.valid_time || '').slice(0, 10),
        value: numberOrNull(row.nao_index),
      }))
      .filter(row => row.date && row.value != null);

    const membersByDate = {};
    parseCsv(gefsText).forEach(row => {
      const date = String(row.valid_time || '').slice(0, 10);
      const value = numberOrNull(row.nao_index);
      if (!date || value == null) return;
      if (!membersByDate[date]) membersByDate[date] = [];
      membersByDate[date].push(value);
    });

    const forecast = gfs.slice(0, 45).map(row => {
      const members = membersByDate[row.date] || [];
      return {
        date: row.date,
        lead: row.lead,
        value: row.value,
        p10: percentile(members, 0.10),
        p90: percentile(members, 0.90),
        members: members.length,
      };
    });

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 's-maxage=21600, stale-while-revalidate=43200');
    return res.status(200).json({
      source: 'NOAA/CPC NAO index CSVs',
      sourceUrls: { observed: NOAA_NAO_OBS, gfs: NOAA_NAO_GFS, gefs: NOAA_NAO_GEFS },
      observedLatest: observed[observed.length - 1] || null,
      observed,
      forecast,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    return res.status(502).json({ error: `NOAA/CPC NAO upstream error: ${error.message}` });
  }
}
