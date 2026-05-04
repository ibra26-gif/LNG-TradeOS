const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const data = JSON.parse(fs.readFileSync(path.join(root, 'data/taiwan_gas_balance.json'), 'utf8'));
const scraper = fs.readFileSync(path.join(root, 'scripts/fetch_taiwan_gas_balance.py'), 'utf8');
const workflow = fs.readFileSync(path.join(root, '.github/workflows/taiwan-daily.yml'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error('FAIL:', msg);
    process.exitCode = 1;
  }
}

assert(app.includes('id="twn-container"'), 'Taiwan tab must render a real container, not a placeholder');
assert(js.includes("country==='taiwan'") && js.includes('twnInit'), 'Asia Taiwan tab must initialize twnInit');
assert(
  js.includes('/data/taiwan_gas_balance.json') &&
    js.includes('TAIWAN MODULE — Gas Balance') &&
    js.includes('MONTHLY TAIWAN GAS BALANCE') &&
    js.includes('TAIWAN SEASONAL BALANCE') &&
    js.includes('TAIWAN SOURCE STACK'),
  'Taiwan module must fetch and render the gas balance dashboard'
);
assert(
    js.includes('Gassco UMM is not used here') &&
    js.includes('NG1 LNG adjustment + NG2 marketable gas') &&
    js.includes('Physical LNG imports') &&
    js.includes('Missing source row = gap, not modelled value') &&
    js.includes("bcm * 0.735") &&
    js.includes("bcm/month x 0.735"),
  'Taiwan module must disclose source logic, physical LNG cross-check, and no invented data'
);
assert(data.schema === 'taiwan_gas_balance_v1', 'Taiwan data schema missing');
assert(data.status === 'parsed', 'Taiwan gas balance should be parsed from official MOEA data');
assert(Array.isArray(data.rows) && data.rows.length >= 200, 'Taiwan gas balance should include multi-year monthly rows');
assert(data.latest_month === data.rows.at(-1).month, 'Taiwan latest month must match the final row');
const latest = data.rows.at(-1);
assert(latest.totalSupplyBcm > 0 && latest.totalDemandBcm > 0, 'Taiwan latest row needs positive supply and demand');
assert(latest.ng1?.domesticProductionBcm > 0, 'Taiwan indigenous production must be counted inside NG1');
assert(latest.ng2?.marketableSupplyBcm > 0 && latest.physicalLngImportsBcm > 0, 'Taiwan NG2/LNG values must be parsed');
assert(latest.coverage?.boeTable91Ng2 === true && latest.coverage?.boeTable93Ng1 === true, 'Taiwan row must mark both BOE table coverages');
assert(
  Math.abs((latest.ng1.supplyBcm + latest.ng2.marketableSupplyBcm) - latest.totalSupplyBcm) < 0.00001,
  'Taiwan total supply must equal NG1 marketable supply plus NG2 marketable supply'
);
assert(
  data.sourceMap.some(s => s.source?.includes('Energy Administration') && s.status === 'wired' && s.pull?.includes('Table 9-1')) &&
    data.sourceMap.some(s => s.source === 'Gassco UMM' && s.status === 'not_used'),
  'Taiwan source map must preserve MOEA BOE source and explicitly exclude Gassco UMM'
);
assert(
  scraper.includes('set_id=135') &&
    scraper.includes('Table 9-1') &&
    scraper.includes('Table 9-3') &&
    scraper.includes('ng1_supply') &&
    scraper.includes('ng2_supply') &&
    scraper.includes('total_supply = add(ng1_supply, ng2_supply)') &&
    scraper.includes('No fallback rows are estimated'),
  'Taiwan scraper must parse the MOEA monthly open-data CSV and add NG1 plus NG2 by component'
);
assert(
  workflow.includes('scripts/fetch_taiwan_gas_balance.py') &&
    workflow.includes('data/taiwan_gas_balance.json'),
  'Taiwan workflow must refresh and commit the gas balance data'
);

if (!process.exitCode) console.log(`Taiwan gas balance regression ok: ${data.rows.length} rows, latest ${data.latest_month}`);
