const { clearSessionCookie } = require('./_auth');

module.exports = function handler(req, res) {
  clearSessionCookie(res);
  res.statusCode = 302;
  res.setHeader('Location', '/');
  res.end();
};
