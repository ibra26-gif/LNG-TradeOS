#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const data = JSON.parse(fs.readFileSync(path.join(root, 'data/thailand_gas_balance.json'), 'utf8'));

function assert(cond, msg){
  if(!cond){
    console.error('FAIL:', msg);
    process.exit(1);
  }
}

assert(app.includes('id="thai-container"'), 'Thailand tab must render a real container, not a placeholder');
assert(js.includes("country==='thailand'") && js.includes('thaiInit'), 'Asia Thailand tab must initialize thaiInit');
assert(js.includes("const THAI_PREF_KEY = 'thai_gb_prefs_v1'"), 'Thailand balance prefs must use a namespaced localStorage key');
assert(js.includes('SEASONAL CHART') && js.includes('thaiToggleYear'), 'Thailand view must include a seasonal chart with multi-year toggles');
assert(js.includes('MONTHLY GAS BALANCE') && js.includes('Residual (supply - demand)'), 'Thailand view must include the monthly balance table and residual');
assert(js.includes('Distribution of NGL is ignored'), 'Thailand view must surface the NGL exclusion');
assert(app.includes('20260502-thailand-gas-balance'), 'Cache-bust must be bumped for Thailand gas balance');

assert(data.schema === 'thailand_gas_balance_v1', 'Thailand data schema missing');
assert(data.ngl_distribution_ignored === true, 'Thailand data must explicitly ignore NGL distribution');
assert(Array.isArray(data.rows) && data.rows.length > 400, 'Thailand data should include full historical monthly rows');
assert(data.sources?.production_import?.current?.includes('T03_01_01.xls'), 'Production/import source must be EPPO T03_01_01');
assert(data.sources?.sector_demand?.current?.includes('T03_02_02.xls'), 'Sector demand source must be EPPO T03_02_02');

const latest = data.rows[data.rows.length - 1];
assert(latest.supply?.domestic_production_mmscfd != null, 'Latest row needs domestic production');
assert(latest.supply?.pipeline_imports_mmscfd != null, 'Latest row needs pipeline imports');
assert(latest.supply?.lng_imports_mmscfd != null, 'Latest row needs LNG imports');
assert(latest.demand?.total_sector_demand_mmscfd != null, 'Latest row needs total sector demand');
assert(latest.residual_mmscfd != null, 'Latest row needs computed residual');

console.log(`Thailand gas balance regression ok: ${data.rows.length} rows, latest ${data.latest_month}`);
