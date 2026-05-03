const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  app.includes('id="gas-snapshot-status"') &&
    app.includes('id="eex-status"'),
  'dashboard chart headers must expose separate LNG EOD and EEX status badges'
);

assert(
  js.includes('LNG EOD ${src.length} dates · last ${fmtD(latestPlotted)}') &&
    js.includes('pointRadius:ctx=>ctx.dataIndex===src.length-1?2.4:0'),
  'global gas snapshot must label and mark the latest plotted LNG EOD point'
);

assert(
  js.includes('EEX gap: last ${fmtD(latestEex)} · LNG EOD ${fmtD(latestLng)}') &&
    js.includes('latestEex<latestLng') &&
    js.includes('not filling a missing day'),
  'EU hub chart must flag EEX source gaps instead of silently implying missing data was plotted'
);

assert(
  js.includes('EEX hub rows latest:') &&
    js.includes('gap vs LNG EOD'),
  'financial data foundation status must show EEX hub freshness separately from LNG EOD freshness'
);

if (!process.exitCode) console.log('Financial curve freshness regression checks passed');
