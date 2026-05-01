// api/drive.js — Vercel serverless function
// Proxies Google Drive API to avoid exposing the API key client-side.
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const DEFAULT_FOLDER = process.env.DRIVE_PRICE_FOLDER_ID || '18CJsgeFbLzmW3fV4I5XEz8nGsRHd7WQq';
const API_KEY = process.env.GOOGLE_API_KEY;
const COOKIE_NAME = 'lngtradeos_session';
const MAX_AGE_SECONDS = 60 * 60 * 12;
const ASSETS = {
  'lngtradeos.css': {
    contentType: 'text/css; charset=utf-8',
    fileName: 'platform-css.txt',
  },
  'lngtradeos.js': {
    contentType: 'application/javascript; charset=utf-8',
    fileName: 'platform-js.txt',
  },
};

function getPassword() {
  return process.env.LNGTRADEOS_ACCESS_PASSWORD || '';
}

function getSecret() {
  return process.env.LNGTRADEOS_SESSION_SECRET || process.env.LNGTRADEOS_ACCESS_PASSWORD || '';
}

function sign(value) {
  return crypto.createHmac('sha256', getSecret()).update(value).digest('hex');
}

function safeEqual(a, b) {
  const left = Buffer.from(String(a));
  const right = Buffer.from(String(b));
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function isValidPassword(password) {
  const configuredPassword = getPassword();
  return Boolean(configuredPassword) && safeEqual(password || '', configuredPassword);
}

function createSessionToken() {
  const issuedAt = String(Date.now());
  return `${issuedAt}.${sign(issuedAt)}`;
}

function isValidSession(req) {
  if (!getSecret()) return false;

  const cookie = req.headers.cookie || '';
  const match = cookie.match(new RegExp(`(?:^|; )${COOKIE_NAME}=([^;]+)`));
  if (!match) return false;

  const [issuedAt, signature] = decodeURIComponent(match[1]).split('.');
  if (!issuedAt || !signature || !safeEqual(signature, sign(issuedAt))) return false;

  const ageMs = Date.now() - Number(issuedAt);
  return Number.isFinite(ageMs) && ageMs >= 0 && ageMs <= MAX_AGE_SECONDS * 1000;
}

function setSessionCookie(res) {
  res.setHeader(
    'Set-Cookie',
    `${COOKIE_NAME}=${encodeURIComponent(createSessionToken())}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=${MAX_AGE_SECONDS}`,
  );
}

function clearSessionCookie(res) {
  res.setHeader(
    'Set-Cookie',
    `${COOKIE_NAME}=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0`,
  );
}

function withPassword(content) {
  return content.replaceAll('__LNGTRADEOS_ACCESS_PASSWORD__', getPassword());
}

function sendRedirect(res, location) {
  res.statusCode = 302;
  res.setHeader('Location', location);
  res.end();
}

function handleLogin(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ ok: false });
  }

  let body = req.body || {};
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch {
      body = {};
    }
  }

  if (!isValidPassword(body.password)) return res.status(401).json({ ok: false });

  setSessionCookie(res);
  return res.status(200).json({ ok: true });
}

function handleApp(req, res) {
  if (!isValidSession(req)) return sendRedirect(res, '/');

  const appPath = path.join(process.cwd(), 'api/private/platform-app.txt');
  const html = withPassword(fs.readFileSync(appPath, 'utf8'));
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'private, no-store');
  return res.status(200).send(html);
}

function handleAsset(req, res) {
  if (!isValidSession(req)) return res.status(401).send('Unauthorized');

  const asset = ASSETS[String(req.query.name || '')];
  if (!asset) return res.status(404).send('Not found');

  const assetPath = path.join(process.cwd(), 'api/private', asset.fileName);
  const content = withPassword(fs.readFileSync(assetPath, 'utf8'));
  res.setHeader('Content-Type', asset.contentType);
  res.setHeader('Cache-Control', 'private, no-store');
  return res.status(200).send(content);
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Cache-Control', 'no-store');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const { action, id, pageToken, folderId, after } = req.query;
  if (action === 'login') return handleLogin(req, res);
  if (action === 'app') return handleApp(req, res);
  if (action === 'asset') return handleAsset(req, res);
  if (action === 'logout') {
    clearSessionCookie(res);
    return sendRedirect(res, '/');
  }
  if (!API_KEY) {
    return res.status(500).json({ error: 'GOOGLE_API_KEY not configured in Vercel environment variables.' });
  }
  const folder = folderId || DEFAULT_FOLDER;
  if (action === 'list') {
    const filter = String(req.query.filter || 'xlsx').toLowerCase();
    const filters = {
      xlsx: [
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        "mimeType='application/vnd.ms-excel'",
        "name contains '.xlsx'",
        "name contains '.xls'"
      ],
      json: [
        "mimeType='application/json'",
        "name contains '.json'"
      ],
      image: [
        "mimeType contains 'image/'",
        "name contains '.png'",
        "name contains '.jpg'",
        "name contains '.jpeg'"
      ],
      all: [
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        "mimeType='application/vnd.ms-excel'",
        "mimeType='application/json'",
        "name contains '.xlsx'",
        "name contains '.xls'",
        "name contains '.json'"
      ],
    };
    const mimeFilter = (filters[filter] || filters.xlsx).join(' or ');
    let q = `'${folder}' in parents and trashed=false and (${mimeFilter})`;
    if (after) {
      q += ` and modifiedTime > '${after}T12:00:00'`;
    }
    const params = new URLSearchParams({
      q,
      fields: 'nextPageToken,files(id,name,size,mimeType,modifiedTime)',
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
