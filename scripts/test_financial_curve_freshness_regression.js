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
    app.includes('id="eex-status"') &&
    app.includes('id="h-chart-title"'),
  'dashboard chart headers must expose separate LNG EOD and EEX status badges'
);

assert(
  js.includes('LNG EOD ${src.length} dates · last ${fmtD(latestPlotted)}') &&
    js.includes('function finForcedLatestTickSpec(labels,maxTicks=12)') &&
    js.includes('idx===0||idx===n-1||idx%step===0') &&
    js.includes('autoSkip:false,maxTicksLimit:Math.max(n,1)') &&
    js.includes('function finLatestPointRadius(ctx,lastIndex,size=3.6)') &&
    js.includes('latest gaps: ${latestGaps.join') &&
    js.includes('pointRadius:ctx=>finLatestPointRadius(ctx,src.length-1,3.6)'),
  'global gas snapshot must label, tick, and mark the latest plotted LNG EOD point'
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

assert(
  js.includes('function rowInstValue') &&
    js.includes('Derived spread fallback from sourced legs only') &&
    js.includes('function gAgg(inst,t,rows)') &&
    js.includes('const v=rowInstValue(row,inst)') &&
    js.includes('function gSR(inst,t)') &&
    js.includes('function gSF(inst,pk)'),
  'historical/forward series must derive spread values from sourced legs when the explicit spread cell is missing'
);

assert(
  js.includes('latest plotted ${fmtD(latestSeries)}') &&
    js.includes('has no ${INST[i1]?.label||i1} ${tvL(t1)} value') &&
    js.includes('EEX latest ${fmtD(eexLatest)} vs LNG EOD ${fmtD(latestLoaded)}'),
  'historical price chart must disclose when the selected series stops before the latest loaded EOD date'
);

if (!process.exitCode) console.log('Financial curve freshness regression checks passed');
