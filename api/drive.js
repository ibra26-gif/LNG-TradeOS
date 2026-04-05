
// Vercel serverless proxy for Google Drive API
// API key lives server-side in GOOGLE_API_KEY env variable — never exposed to browser
// CORS handled by your own domain — works on all browsers including Safari

const API_KEY = process.env.GOOGLE_API_KEY;
const FOLDER_ID = '18CJsgeFbLzmW3fV4I5XEz8nGsRHd7WQq';

module.exports = async (req, res) => {
  // CORS headers — allow requests from your domain
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  if (!API_KEY) {
    res.status(500).json({ error: 'GOOGLE_API_KEY environment variable not set' });
    return;
  }

  const { action, id, pageToken } = req.query;

  try {
    // ── LIST FILES in LNG Curves folder ──
    if (action === 'list') {
      const q = encodeURIComponent(
        `'${FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false`
      );
      let url = `https://www.googleapis.com/drive/v3/files?q=${q}&key=${API_KEY}&fields=files(id,name),nextPageToken&pageSize=200&orderBy=name`;
      if (pageToken) url += `&pageToken=${encodeURIComponent(pageToken)}`;

      const r = await fetch(url);
      const data = await r.json();

      if (!r.ok) {
        res.status(r.status).json({ error: data.error?.message || 'Drive list failed' });
        return;
      }

      res.status(200).json(data);

    // ── DOWNLOAD a specific file by ID ──
    } else if (action === 'file' && id) {
      // Validate id is a safe Drive file ID (alphanumeric + dash + underscore)
      if (!/^[a-zA-Z0-9_\-]+$/.test(id)) {
        res.status(400).json({ error: 'Invalid file ID' });
        return;
      }

      const url = `https://www.googleapis.com/drive/v3/files/${id}?alt=media&key=${API_KEY}`;
      const r = await fetch(url);

      if (!r.ok) {
        res.status(r.status).json({ error: `Drive file fetch failed: ${r.status}` });
        return;
      }

      const buffer = await r.arrayBuffer();
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.status(200).send(Buffer.from(buffer));

    } else {
      res.status(400).json({ error: 'Invalid action. Use action=list or action=file&id=FILE_ID' });
    }

  } catch (err) {
    console.error('Drive proxy error:', err);
    res.status(500).json({ error: err.message || 'Internal proxy error' });
  }
