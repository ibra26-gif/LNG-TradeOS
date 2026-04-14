// api/drive.js — Vercel serverless proxy for Google Drive
// Handles: list, file (download), upload
// Save as: api/drive.js in your GitHub repo root

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { action, folderId, filter, id, pageToken, after } = req.query;
  const API_KEY = process.env.GOOGLE_API_KEY;

  // ── LIST files in a folder ────────────────────────────────────────────────
  if (action === 'list') {
    try {
      let q = `'${folderId}' in parents and trashed=false`;
      if (filter === 'xlsx') q += ` and name contains '.xls'`;
      let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}&fields=files(id,name,modifiedTime,size),nextPageToken&orderBy=name&pageSize=100&key=${API_KEY}`;
      if (pageToken) url += `&pageToken=${encodeURIComponent(pageToken)}`;

      let files = [];
      let nextPageToken = null;
      do {
        const r = await fetch(url);
        if (!r.ok) {
          const e = await r.json();
          return res.status(r.status).json({ error: e.error || 'Drive API error' });
        }
        const d = await r.json();
        files = files.concat(d.files || []);
        nextPageToken = d.nextPageToken || null;
        if (nextPageToken) {
          url = url.split('&pageToken=')[0] + `&pageToken=${encodeURIComponent(nextPageToken)}`;
        }
      } while (nextPageToken);

      // Filter by modification date if requested
      if (after) {
        const cutoff = new Date(after);
        files = files.filter(f => !f.modifiedTime || new Date(f.modifiedTime) > cutoff);
      }

      return res.status(200).json({ files, nextPageToken: null });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  // ── DOWNLOAD a file by ID ─────────────────────────────────────────────────
  if (action === 'file') {
    try {
      const url = `https://www.googleapis.com/drive/v3/files/${id}?alt=media&key=${API_KEY}`;
      const r = await fetch(url);
      if (!r.ok) {
        const e = await r.json().catch(() => ({}));
        return res.status(r.status).json({ error: e.error || 'Drive download error' });
      }
      const buf = await r.arrayBuffer();
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Cache-Control', 'no-store');
      return res.status(200).send(Buffer.from(buf));
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  // ── UPLOAD a file (overwrite existing or create new) ─────────────────────
  // Called via POST from eex_v4.py with multipart/form-data
  // Uses a secret upload token to prevent unauthorised writes
  if (action === 'upload') {
    if (req.method !== 'POST') {
      return res.status(405).json({ error: 'POST required' });
    }

    // Simple secret token check — set UPLOAD_SECRET in Vercel env vars
    const uploadSecret = process.env.UPLOAD_SECRET;
    const authHeader = req.headers['x-upload-token'];
    if (!uploadSecret || authHeader !== uploadSecret) {
      return res.status(401).json({ error: 'Unauthorised' });
    }

    try {
      // For uploads we need OAuth, not API key — use service account token
      // Stored in GOOGLE_SERVICE_ACCOUNT_JSON env var (JSON string)
      const saJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
      if (!saJson) {
        return res.status(500).json({ error: 'GOOGLE_SERVICE_ACCOUNT_JSON not configured' });
      }

      // Get access token from service account
      const token = await getServiceAccountToken(JSON.parse(saJson));

      // Find existing file in the folder
      const targetFolder = req.query.folderId || '1YIzYfFyANWtZJcxQsVQxZGX2w-kglZsY';
      const fileName = req.query.name || 'EEX_Gas_Curves.xlsx';
      const listUrl = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(`name='${fileName}' and '${targetFolder}' in parents and trashed=false`)}&fields=files(id,name)`;
      const listR = await fetch(listUrl, { headers: { Authorization: `Bearer ${token}` } });
      const listD = await listR.json();
      const existing = (listD.files || [])[0];

      // Read body buffer
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const fileBuffer = Buffer.concat(chunks);
      const mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';

      if (existing) {
        // Overwrite — PATCH with media
        const uploadUrl = `https://www.googleapis.com/upload/drive/v3/files/${existing.id}?uploadType=media`;
        const upR = await fetch(uploadUrl, {
          method: 'PATCH',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': mimeType },
          body: fileBuffer,
        });
        if (!upR.ok) {
          const e = await upR.json().catch(() => ({}));
          return res.status(upR.status).json({ error: e.error || 'Upload failed' });
        }
        return res.status(200).json({ ok: true, action: 'updated', id: existing.id, name: fileName });
      } else {
        // Create new — multipart upload
        const meta = JSON.stringify({ name: fileName, parents: [targetFolder] });
        const boundary = 'EEX_BOUNDARY_X7Z';
        const body = Buffer.concat([
          Buffer.from(`--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${meta}\r\n--${boundary}\r\nContent-Type: ${mimeType}\r\n\r\n`),
          fileBuffer,
          Buffer.from(`\r\n--${boundary}--`),
        ]);
        const upR = await fetch(`https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart`, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': `multipart/related; boundary=${boundary}`,
          },
          body,
        });
        if (!upR.ok) {
          const e = await upR.json().catch(() => ({}));
          return res.status(upR.status).json({ error: e.error || 'Upload failed' });
        }
        const created = await upR.json();
        return res.status(200).json({ ok: true, action: 'created', id: created.id, name: fileName });
      }
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  return res.status(400).json({ error: 'Unknown action' });
}

// ── Service account JWT → access token ───────────────────────────────────────
async function getServiceAccountToken(sa) {
  const now = Math.floor(Date.now() / 1000);
  const header = btoa(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  const payload = btoa(JSON.stringify({
    iss: sa.client_email,
    scope: 'https://www.googleapis.com/auth/drive',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600,
  })).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');

  const unsigned = `${header}.${payload}`;

  // Sign with RSA-SHA256 using Web Crypto
  const pemKey = sa.private_key.replace(/\\n/g, '\n');
  const pemBody = pemKey.replace(/-----BEGIN PRIVATE KEY-----/, '').replace(/-----END PRIVATE KEY-----/, '').replace(/\s/g, '');
  const binaryDer = Uint8Array.from(atob(pemBody), c => c.charCodeAt(0));
  const cryptoKey = await crypto.subtle.importKey(
    'pkcs8', binaryDer.buffer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false, ['sign']
  );
  const sig = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', cryptoKey, new TextEncoder().encode(unsigned));
  const sigB64 = btoa(String.fromCharCode(...new Uint8Array(sig))).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  const jwt = `${unsigned}.${sigB64}`;

  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`,
  });
  const d = await r.json();
  if (!d.access_token) throw new Error(`Token error: ${JSON.stringify(d)}`);
  return d.access_token;
}
