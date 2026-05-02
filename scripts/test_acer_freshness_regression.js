const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const workflow = fs.readFileSync(path.join(root, '.github/workflows/data-heartbeat.yml'), 'utf8');
const plist = fs.readFileSync(path.join(root, 'scripts/com.lngtradeos.acer.plist'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const api = fs.readFileSync(path.join(root, 'api/acer.js'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  workflow.includes('check_acer_csv "data/acer_historical.csv"') &&
    workflow.includes('latest_date=$(awk') &&
    workflow.includes('latest ACER DATE') &&
    !workflow.includes('check_file "data/acer_historical.csv"'),
  'heartbeat must validate ACER by latest DATE inside the CSV, not git commit timestamp'
);

assert(
  workflow.includes('"ACER curves"     48 110'),
  'ACER heartbeat thresholds must tolerate normal weekday and Monday publication lag'
);

assert(
  plist.includes('__LNGTRADEOS_REPO__') &&
    plist.includes('/bin/bash') &&
    plist.includes('scripts/acer_fetch.py') &&
    !plist.includes('/Users/iboumar/Desktop/lngtradeos-fix'),
  'ACER launchd plist must be repo-path templated and not point at the old clone'
);

assert(
  app.includes('Loading live ACER CSV') &&
    /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app) &&
    !app.includes('name=lngtradeos.js&v=20260501-hpa-notifications'),
  'ACER UI must show live-load state and private script cache-bust must move past the old bundle'
);

assert(
  js.includes('function acer_freshness') &&
    js.includes('latest ACER DATE') &&
    js.includes('Live ACER CSV failed · using embedded fallback') &&
    js.includes('acer_freshness(latest)'),
  'ACER UI must surface fresh/stale status after live CSV load and on fallback'
);

assert(
  api.includes('X-LNGTradeOS-Latest-Date') &&
    api.includes('X-LNGTradeOS-Row-Count'),
  'ACER API must expose latest-date and row-count headers for diagnostics'
);

if (!process.exitCode) console.log('ACER freshness regression checks passed');
