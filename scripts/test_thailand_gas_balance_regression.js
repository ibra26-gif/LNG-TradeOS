#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const data = JSON.parse(fs.readFileSync(path.join(root, 'data/thailand_gas_balance.json'), 'utf8'));
const weather = JSON.parse(fs.readFileSync(path.join(root, 'data/thailand_weather_cdd.json'), 'utf8'));
const weatherScript = fs.readFileSync(path.join(root, 'scripts/fetch_thailand_weather.py'), 'utf8');
const workflow = fs.readFileSync(path.join(root, '.github/workflows/thailand-daily.yml'), 'utf8');

function assert(cond, msg){
  if(!cond){
    console.error('FAIL:', msg);
    process.exit(1);
  }
}

assert(app.includes('id="thai-container"'), 'Thailand tab must render a real container, not a placeholder');
assert(js.includes("country==='thailand'") && js.includes('thaiInit'), 'Asia Thailand tab must initialize thaiInit');
assert(js.includes("const THAI_PREF_KEY = 'thai_gb_prefs_v1'"), 'Thailand balance prefs must use a namespaced localStorage key');
assert(js.includes('function thaiShowTab') && js.includes('WEATHER MONITOR'), 'Thailand must expose Gas Balance and Weather Monitor tabs');
assert(js.includes('SEASONAL CHART') && js.includes('thaiToggleYear'), 'Thailand view must include a seasonal chart with multi-year toggles');
assert(js.includes('MONTHLY GAS BALANCE') && js.includes('Residual (supply - demand)'), 'Thailand view must include the monthly balance table and residual');
assert(js.includes('THAILAND CDD INDEX') && js.includes('CDD vs POWER GAS BURN vs LNG IMPORTS'), 'Weather monitor must show CDD and power/LNG sensitivity charts');
assert(js.includes('Distribution of NGL is ignored'), 'Thailand view must surface the NGL exclusion');
assert(app.includes('20260502-thailand-weather-monitor'), 'Cache-bust must be bumped for Thailand weather monitor');

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

assert(weather.schema === 'thailand_weather_cdd_v1', 'Thailand weather CDD schema missing');
assert(weather.base_temp_c === 24, 'Thailand CDD base temperature should be 24C');
assert(weather.sources?.weather === 'Open-Meteo Historical Weather API', 'Thailand CDD must use Open-Meteo source');
assert(weather.forecast_included === false, 'Thailand CDD must not pretend to include forecast');
assert(Array.isArray(weather.city_weights) && weather.city_weights.length === 5, 'Thailand CDD needs a five-city weighted basket');
assert(Array.isArray(weather.monthly) && weather.monthly.length >= 60, 'Thailand CDD needs multi-year monthly history');
assert(weather.monthly[weather.monthly.length - 1].cdd != null, 'Latest Thailand CDD month needs CDD value');
assert(weather.monthly[weather.monthly.length - 1].normal_cdd_to_date != null, 'Latest Thailand CDD month needs prorated normal');
assert(weatherScript.includes('CDD = max(mean_temp_c - base_temp_c, 0)') && weatherScript.includes('Open-Meteo'), 'Weather scraper must document CDD method and source');
assert(workflow.includes('scripts/fetch_thailand_weather.py') && workflow.includes('data/thailand_weather_cdd.json'), 'Thailand workflow must refresh and commit weather CDD data');

console.log(`Thailand gas balance regression ok: ${data.rows.length} rows, latest ${data.latest_month}`);
