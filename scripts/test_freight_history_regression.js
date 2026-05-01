const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  js.includes('③ HISTORICAL FREIGHT CURVES') && js.includes('renderHistoricalStandalone'),
  'freight history must remain a top-level Freight tab'
);

assert(
  /async function parseHistExcel\(el\)[\s\S]*?results\.push\(fHistParseWorkbook\(buf,file\.name/.test(js),
  'manual Excel upload must use the shared robust freight parser'
);

assert(
  js.includes('if(refreshEditor&&typeof _fRefreshFreightCurveEditor') && !js.includes('if(refreshEditor&&typeof fMatTab===\'function\')fMatTab(0)'),
  'Drive sync refresh must not jump to hidden tab index 0'
);

assert(
  app.includes('20260501-freight-history-restore'),
  'cache-bust must be bumped for freight history restore'
);

if (!process.exitCode) console.log('freight history regression checks passed');
