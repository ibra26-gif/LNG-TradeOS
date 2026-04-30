const crypto = require('crypto');

const COOKIE_NAME = 'lngtradeos_session';
const MAX_AGE_SECONDS = 60 * 60 * 12;

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

function isValidPassword(password) {
  const configuredPassword = getPassword();
  return Boolean(configuredPassword) && safeEqual(password || '', configuredPassword);
}

module.exports = {
  clearSessionCookie,
  isValidPassword,
  isValidSession,
  setSessionCookie,
};
