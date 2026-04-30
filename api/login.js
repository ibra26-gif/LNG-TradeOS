const { isValidPassword, setSessionCookie } = require('./_auth');

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    res.status(405).json({ ok: false });
    return;
  }

  let body = req.body || {};
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch {
      body = {};
    }
  }

  if (!isValidPassword(body.password)) {
    res.status(401).json({ ok: false });
    return;
  }

  setSessionCookie(res);
  res.status(200).json({ ok: true });
};
