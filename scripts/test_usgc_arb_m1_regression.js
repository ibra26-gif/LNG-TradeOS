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
  js.includes("COGH uses next JKM") &&
    js.includes("Panama uses JKM ${m1Label}") &&
    js.includes('Observation chips') &&
    js.includes('select several to see how the arb evolved') &&
    !js.includes('COGH ROUTE DIRECTION') &&
    !js.includes('const m1Cogh = arbData.today?.arbCogh?.[0]'),
  'USGC COGH arb must expose next-JKM timing and multi-observation chart controls without the removed KPI boxes'
);

assert(
  js.includes('function cpAutoSyncPricesFromLatestEod') &&
    js.includes("cpSet('cp_fp_meta'") &&
    js.includes('cpBuildFpFromEod(cpLatestEodKey())') &&
    !js.includes('sDates.forEach(date=>{const r=aD[date]'),
  'USGC/global arb price curves must sync from the latest EOD only, not roll older values into missing tenors'
);

assert(
  js.includes('const usgcJktcCoghDes = ML.map') &&
    js.includes('const jkmCogh = gas?.JKM?.[i+1]') &&
    js.includes("label:'JKTC COGH'") &&
    js.includes("label:'JKTC PC'"),
  'USGC PC/COGH DES pricing must keep separate same-month and next-month JKM rules'
);

assert(
  js.includes('function _usgcIndicatorsTable(d)') &&
    js.includes('USGC ARBITRAGE INDICATORS') &&
    js.includes('Profit to Europe ($/MMBtu)') &&
    js.includes('Profit to Asia PC ($/MMBtu)') &&
    js.includes('Profit to Asia COGH ($/MMBtu)') &&
    js.includes('JKM/TTF Spread COGH') &&
    js.includes("physCurve('nwe')") &&
    js.includes("physCurve('jktc')") &&
    js.includes('const priceMeta=cpPriceMeta()') &&
    js.includes('Curve EOD: ${eod} · Phys Diff: PHYS DIFFERENTIALS tab') &&
    js.includes('Not hardcoded: rows are calculated from latest Financial Trading EOD curves') &&
    js.includes('${_usgcIndicatorsTable(d)}'),
  'USGC Arb tab must show the requested indicator table, sourced from the Physical Differentials tab, before the existing charts'
);

assert(
  js.includes('id="usgc-arb-cogh"') &&
    js.includes('id="usgc-arb-pc"') &&
  js.includes('function _usgcArbRenderCharts()') &&
    js.includes('function _usgcArbSelectedCurves') &&
    js.includes('function _usgcAlignedSnapshotInputs') &&
    js.includes('cpHorizonPk(i)') &&
    js.includes('cp_usgc_arb_obs_dates') &&
    js.includes("button class=\"f-btn sm\" onclick=\"_usgcArbSetObsPreset('last3')"),
  'USGC Arb charts must remain present, align snapshots by delivery month, and support multiple observation overlays'
);

const fnStart = js.indexOf('function _usgcArbComputeFromInputs');
const fnEnd = js.indexOf('function _usgcArbCurves', fnStart);
assert(fnStart >= 0 && fnEnd > fnStart, 'USGC arb compute function must be extractable');
if (fnStart >= 0 && fnEnd > fnStart) {
  const ctx = {
    CP: { hhSlope: 1.15 },
    PC_NM_RATIO: 0.5,
    PC_CANAL_FEE_PER_MMBTU_LOCAL: 0.1,
  };
  vm.runInNewContext(js.slice(fnStart, fnEnd), ctx);
  const out = ctx._usgcArbComputeFromInputs(
    { HH: [2], TTF: [10], JKM: [20, 30] },
    { sabine_rotterdam: [1], sabine_tokyo: [3] },
    { nwe: [0], jktc: [0, 0] },
    []
  );
  assert(out.arbCogh[0] === 18, 'COGH arb must use next JKM contract');
  assert(out.arbPc[0] === 9.4, 'Panama arb must use same-month JKM contract');
}

assert(
  /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private platform script must carry a dated cache-bust'
);

if (!process.exitCode) console.log('USGC route DES shift regression checks passed');
