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
  js.includes("const m1Cogh = arbData.today?.arbCogh?.[0]") &&
    js.includes("COGH ARB · ${m1Label} LOAD · JKM ${coghDesLabel}") &&
    js.includes("COGH uses next JKM"),
  'USGC COGH arb must expose the next-JKM route timing'
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
    js.includes('${_usgcIndicatorsTable(d)}'),
  'USGC Arb tab must show the requested indicator table before the existing charts'
);

assert(
  js.includes('id="usgc-arb-cogh"') &&
    js.includes('id="usgc-arb-pc"') &&
    js.includes('function _usgcArbRenderCharts()'),
  'USGC Arb charts must remain present on the tab'
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
