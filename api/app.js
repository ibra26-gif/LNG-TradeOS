const fs = require('fs');
const path = require('path');
const { isValidSession } = require('./_auth');

module.exports = function handler(req, res) {
  if (!isValidSession(req)) {
    res.statusCode = 302;
    res.setHeader('Location', '/');
    res.end();
    return;
  }

  const appPath = path.join(process.cwd(), 'api/private/platform-app.txt');
  const html = fs
    .readFileSync(appPath, 'utf8')
    .replaceAll('__LNGTRADEOS_ACCESS_PASSWORD__', process.env.LNGTRADEOS_ACCESS_PASSWORD || '');
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'private, no-store');
  res.status(200).send(html);
};
