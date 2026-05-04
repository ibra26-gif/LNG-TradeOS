const fs = require('fs');
const path = require('path');
const assert = require('assert');

const root = path.resolve(__dirname, '..');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const data = JSON.parse(fs.readFileSync(path.join(root, 'data/turkey_gas_balance.json'), 'utf8'));
const scraper = fs.readFileSync(path.join(root, 'scripts/fetch_turkey_gas_balance.py'), 'utf8');

function approx(a, b, eps = 1e-6) {
  return Math.abs(a - b) <= eps;
}

assert(app.includes('id="lngbal-btn-sam"') && app.includes('id="lngbal-btn-turkey"'), 'Turkey country button must sit after South America');
assert(app.indexOf('id="lngbal-btn-sam"') < app.indexOf('id="lngbal-btn-turkey"'), 'Turkey button must sit after South America');
assert(app.includes('id="tur-container"'), 'Turkey tab must render a real container');
assert(/lngtradeos\.js&v=20260504-/.test(app), 'cache bust must stay bumped after Turkey app JS change');

assert(js.includes('/data/turkey_gas_balance.json'), 'Turkey UI must load the Turkey JSON data file');
assert(js.includes('TURKEY MODULE — Gas Balance + TEIAS Power Source Mix'), 'Turkey module marker missing');
assert(js.includes("TURKEY GAS BALANCE"), 'Turkey UI must show the requested Turkey gas-balance page');
assert(!js.includes("turSetTab") && !js.includes(">HYDRO<"), 'Turkey UI should be one gas-balance page, not a separate Hydro tab');
assert(js.includes('LNG imports + domestic production + pipeline imports'), 'Turkey UI must show the requested balance equation');
assert(!js.includes('IHS') && !js.includes('Kpler'), 'Turkey UI must not reference IHS/Kpler');
assert(js.includes('supplyDemandBalanceBcm'), 'Turkey UI must show supply minus sectorial demand');
for (const key of ['hydroGenerationGWh','coalGenerationGWh','windGenerationGWh','solarGenerationGWh','geothermalWindSolarGenerationGWh','alternativeFuelsGenerationGWh']) {
  assert(js.includes(key), `Turkey UI must display TEIAS ${key}`);
}

assert(data.schema === 'turkey_gas_balance_v1', 'Turkey data schema missing');
assert(data.status === 'wired' || data.status === 'partial', 'Turkey data should be wired or partial, not fabricated');
assert(data.balanceEquation === 'LNG imports + domestic production + pipeline imports = sectorial gas demand', 'Turkey balance equation must match user guidance');
assert(Array.isArray(data.rows) && data.rows.length >= 14, 'Turkey data should include parsed 2025+ EPDK rows');
assert(data.rows.some(r => /^2025-/.test(r.month)), 'Turkey gas balance must include 2025 EPDK rows');
assert(data.latest_month === data.rows[data.rows.length - 1].month, 'latest month must match latest parsed row');
for (const row of data.rows) {
  for (const key of ['lngImportsBcm','domesticProductionBcm','pipelineImportsBcm','totalSupplyBcm','totalDemandBcm','powerGenerationDemandBcm','industryDemandBcm','residentialKonutDemandBcm']) {
    assert(Number.isFinite(row[key]), `${row.month} ${key} must be sourced numeric data`);
  }
  assert(approx(row.totalSupplyBcm, row.lngImportsBcm + row.domesticProductionBcm + row.pipelineImportsBcm), `${row.month} supply equation must add up`);
  assert(approx(row.supplyDemandBalanceBcm, row.totalSupplyBcm - row.totalDemandBcm), `${row.month} balance must be supply - sectorial demand`);
  assert(Array.isArray(row.importsByCountry) && row.importsByCountry.length > 0, `${row.month} must include EPDK import-country rows`);
}
assert(data.sourceMap.some(s => /EPDK monthly natural gas sector report PDF/.test(s.source) && /Table 2\.1\.2/.test(s.pull)), 'EPDK PDF Table 2.1.2 source missing');
assert(data.sourceMap.some(s => /Excel appendix/.test(s.source) && /Table 7/.test(s.pull) && /Konut/.test(s.pull)), 'EPDK sector demand/Konut source missing');
assert(data.sourceMap.some(s => /TEİAŞ/.test(s.source) && /Coal = Hard Coal \+ Imported Coal plus Lignite/.test(s.pull)), 'TEIAS power source mix source missing');

assert(data.powerGeneration && ['wired','partial'].includes(data.powerGeneration.status), 'TEIAS power generation should be wired or partial');
assert(Array.isArray(data.powerGeneration.rows) && data.powerGeneration.rows.length >= 51, 'TEIAS power generation should include 2022 onward plus current-year rows');
assert(data.powerGeneration.rows[0].month === '2022-01', 'TEIAS power generation should start at January 2022');
assert(data.powerGeneration.rows.some(r => /^2022-/.test(r.month)), 'TEIAS power generation must include 2022 rows');
assert(data.powerGeneration.rows.some(r => /^2023-/.test(r.month)), 'TEIAS power generation must include 2023 rows');
assert(data.powerGeneration.rows.some(r => /^2024-/.test(r.month)), 'TEIAS power generation must include 2024 rows');
assert(data.powerGeneration.rows.some(r => /^2025-/.test(r.month)), 'TEIAS power generation must include 2025 rows');
assert(data.powerGeneration.latestMonth === data.powerGeneration.rows[data.powerGeneration.rows.length - 1].month, 'TEIAS latest month must match latest parsed row');
assert(data.powerGeneration.fixedHistory?.startMonth === '2022-01', 'TEIAS fixed history should start at 2022-01');
assert(data.powerGeneration.fixedHistory?.endMonth === '2025-12', 'TEIAS fixed history should end at 2025-12');
assert(data.powerGeneration.fixedHistory?.rows === 48, 'TEIAS fixed history should pin 48 monthly rows for 2022-2025');
assert(data.powerGeneration.fixedHistory?.status === 'pinned_static', 'TEIAS fixed history must be marked as static/pinned');
assert(/2022-2025 monthly power-generation rows are fixed history/.test(data.powerGeneration.refreshPolicy || ''), 'TEIAS fixed refresh policy must be explicit');
for (const row of data.powerGeneration.rows) {
  for (const key of ['hydroGenerationGWh','hardCoalImportedAsphaltiteGenerationGWh','ligniteGenerationGWh','coalGenerationGWh','alternativeFuelsGenerationGWh']) {
    assert(Number.isFinite(row[key]), `${row.month} ${key} must be sourced numeric TEIAS data`);
  }
  if (Number(row.month.slice(0, 4)) <= 2024) {
    assert(Number.isFinite(row.geothermalWindSolarGenerationGWh), `${row.month} must include TEIAS combined geothermal/wind/solar row`);
    assert(row.windGenerationGWh == null && row.solarGenerationGWh == null, `${row.month} must not invent separate wind/solar when TEIAS only publishes combined row`);
    assert(/GEOTHERMAL.*WIND.*SOLAR/i.test(row.sourceRows.geothermalWindSolarGenerationGWh), `${row.month} combined source row must map to TEIAS GEOTHERMAL + WIND + SOLAR`);
  } else {
    assert(Number.isFinite(row.windGenerationGWh), `${row.month} windGenerationGWh must be sourced numeric TEIAS data`);
    assert(Number.isFinite(row.solarGenerationGWh), `${row.month} solarGenerationGWh must be sourced numeric TEIAS data`);
  }
  assert(approx(row.coalGenerationGWh, row.hardCoalImportedAsphaltiteGenerationGWh + row.ligniteGenerationGWh, 1e-3), `${row.month} coal must equal hard/imported coal plus lignite`);
  assert(/Renew and Wastes|Yenilenebilir/.test(row.sourceRows.alternativeFuelsGenerationGWh), `${row.month} Alternative Fuels must map to TEIAS Renew and Wastes row`);
}

assert(scraper.includes('Table 2.1.2') && scraper.includes('Tablo 7') && scraper.includes('Konut'), 'scraper must document EPDK tables used');
assert(scraper.includes('totalSupplyBcm') && scraper.includes('supplyDemandBalanceBcm'), 'scraper must compute supply equation and balance');
assert(scraper.includes('No fallback rows are estimated'), 'scraper must explicitly avoid fallback estimates');
assert(!scraper.includes('Kpler') && !scraper.includes('IHS'), 'Turkey scraper must not use IHS/Kpler');
assert(scraper.includes('TEIAS_GALLERY_API') && scraper.includes('Renew and Wastes'), 'Turkey scraper must parse TEIAS source mix workbooks');
assert(scraper.includes('GEOTHERMAL + WIND + SOLAR'), 'Turkey scraper must preserve TEIAS combined pre-2025 renewable row');
assert(scraper.includes('TEIAS_FIXED_HISTORY_END_YEAR = 2025') && scraper.includes('fixed_history_complete'), 'Turkey scraper must preserve fixed TEIAS 2022-2025 history');

console.log(`Turkey gas balance regression ok: ${data.rows.length} EPDK rows, latest ${data.latest_month}, TEIAS power ${data.powerGeneration.rows.length} rows latest ${data.powerGeneration.latestMonth}`);
