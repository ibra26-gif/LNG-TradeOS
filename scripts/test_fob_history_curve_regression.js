const fs = require('fs');
const path = require('path');
const vm = require('vm');

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
  js.includes('FOB HISTORY WORKBENCH · route / index / loading point') &&
    js.includes('LOADING POINT') &&
    js.includes('INDEXATION') &&
    js.includes('fob-hist-workbench-curve') &&
    js.includes('fob-hist-workbench-historical') &&
    js.includes('OBSERVATION DATE') &&
    js.includes('cp_fob_hist_obs_date') &&
    js.includes('cp_fob_hist_origin') &&
    js.includes('cp_fob_hist_view') &&
    js.includes('Liquefaction fee excluded from FOB netback history'),
  'FOB history must expose a route/index/loading-point workbench for curve and historical views'
);

assert(
  js.includes('pks, months,') &&
    js.includes('function _fobSnapshotMonthLabels') &&
    js.includes('function _fobSnapshotCurveRows') &&
    js.includes('function _fobSnapshotFobValue'),
  'FOB snapshots must keep delivery labels and reusable saved-snapshot curve helpers'
);

const start = js.indexOf('function _fobRouteFreight');
const end = js.indexOf('function _fobHistTimeSeries', start);
assert(start >= 0 && end > start, 'FOB snapshot helper block must be extractable');

if (start >= 0 && end > start) {
  const ctx = {
    CP: { hhSlope: 1.15 },
    ML: ['May-26', 'Jun-26'],
    PC_NM_RATIO: 0.5,
    PC_CANAL_FEE_PER_MMBTU_LOCAL: 0.1,
    pkL: (pk) => ({ '2026-05': 'May-26', '2026-06': 'Jun-26' }[pk] || pk),
    rPK: (d, t) => {
      const r = new Date(d.getFullYear(), d.getMonth() + parseInt(t, 10), 1);
      return `${r.getFullYear()}-${String(r.getMonth() + 1).padStart(2, '0')}`;
    },
  };
  vm.runInNewContext(
    js.slice(start, end) +
      '\nthis.__fobTest={FOB_ROUTES,_fobSnapshotMonthLabels,_fobSnapshotFobValue,_fobSnapshotCurveRows};',
    ctx
  );

  const snap = {
    eod_date: '2026-04-30',
    gas: { HH: [2], TTF: [10], JKM: [20, 21] },
    freight: {
      sabine_rotterdam: [1],
      sabine_tokyo: [3],
      angola_dahej: [2],
      angola_tokyo: [4],
    },
    phys: { nwe: [-0.4], jktc: [0.05, 0.1] },
    physOv: {},
  };
  const routes = ctx.__fobTest.FOB_ROUTES.usgc;
  assert(ctx.__fobTest._fobSnapshotMonthLabels(snap)[0] === 'May-26', 'snapshot month labels must derive from observation date');
  assert(ctx.__fobTest._fobSnapshotFobValue(snap, routes[0], 'FIXED', 0) === 8.6, 'NWE FOB fixed curve must use saved TTF, phys, and freight');
  assert(ctx.__fobTest._fobSnapshotFobValue(snap, routes[5], 'FIXED', 0) === 18.1, 'USGC COGH curve must price from saved next-month JKM');
  assert(ctx.__fobTest._fobSnapshotFobValue(snap, routes[6], 'FIXED', 0) === 18.45, 'USGC Panama curve must price from saved same-month JKM and PC freight');
}

assert(
  /name=lngtradeos\.js&v=20260501-/.test(app),
  'private platform cache-bust must stay bumped for the current bundle'
);

if (!process.exitCode) console.log('FOB history curve regression checks passed');
