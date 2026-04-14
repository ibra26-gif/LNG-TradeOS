// api/drive.js — Vercel serverless function
// Proxies Google Drive API to avoid exposing the API key client-side.
const DEFAULT_FOLDER = process.env.DRIVE_PRICE_FOLDER_ID || '18CJsgeFbLzmW3fV4I5XEz8nGsRHd7WQq';
const API_KEY = process.env.GOOGLE_API_KEY;
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Cache-Control', 'no-store');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!API_KEY) {
    return res.status(500).json({ error: 'GOOGLE_API_KEY not configured in Vercel environment variables.' });
  }
  const { action, id, pageToken, folderId, after } = req.query;
  const folder = folderId || DEFAULT_FOLDER;
  if (action === 'list') {
    const mimeFilter = [
      "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
      "mimeType='application/vnd.ms-excel'",
      "name contains '.xlsx'",
      "name contains '.xls'"
    ].join(' or ');
    let q = `'${folder}' in parents and trashed=false and (${mimeFilter})`;
    if (after) {
      q += ` and modifiedTime > '${after}T12:00:00'`;
    }
    const params = new URLSearchParams({
      q,
      fields: 'nextPageToken,files(id,name,size,modifiedTime)',
      orderBy: 'name asc',
      pageSize: '1000',
      key: API_KEY,
    });
    if (pageToken) params.set('pageToken', pageToken);
    try {
      const r = await fetch(`https://www.googleapis.com/drive/v3/files?${params}`);
      const data = await r.json();
      if (!r.ok) return res.status(r.status).json(data);
      return res.status(200).json(data);
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }
  if (action === 'file' && id) {
    try {
      const r = await fetch(
        `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(id)}?alt=media&key=${API_KEY}`
      );
      if (!r.ok) {
        const err = await r.text();
        return res.status(r.status).json({ error: `Drive download failed: ${err}` });
      }
      const buf = await r.arrayBuffer();
      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('Content-Length', buf.byteLength);
      return res.send(Buffer.from(buf));
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }
  return res.status(400).json({ error: 'Invalid action. Use action=list or action=file&id=FILE_ID' });
}
