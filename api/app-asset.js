const fs = require('fs');
const path = require('path');
const { isValidSession } = require('./_auth');

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

module.exports = function handler(req, res) {
  if (!isValidSession(req)) {
    res.status(401).send('Unauthorized');
    return;
  }

  const name = String(req.query.name || '');
  const asset = ASSETS[name];
  if (!asset) {
    res.status(404).send('Not found');
    return;
  }

  const assetPath = path.join(process.cwd(), 'api/private', asset.fileName);
  const content = fs
    .readFileSync(assetPath, 'utf8')
    .replaceAll('__LNGTRADEOS_ACCESS_PASSWORD__', process.env.LNGTRADEOS_ACCESS_PASSWORD || '');
  res.setHeader('Content-Type', asset.contentType);
  res.setHeader('Cache-Control', 'private, no-store');
  res.status(200).send(content);
};
