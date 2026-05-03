const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const proxy = fs.readFileSync(path.join(root, 'api/entsoe.js'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  app.includes('id="gmtab-euren"') &&
    app.includes('EUROPEAN RENEWABLE') &&
    app.includes('id="ga-tab-euren"') &&
    app.includes("shellNav('gasanalytics','euren')") &&
    /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'Gas & LNG Analytics must expose European Renewable as a main tab and home module with cache-bust'
);

assert(
  app.indexOf('id="gmtab-lngvc"') > -1 &&
    app.indexOf('id="gmtab-euren"') > app.indexOf('id="gmtab-lngvc"') &&
    app.indexOf('id="ga-tab-euren"') > app.indexOf('id="ga-tab-lngvc"'),
  'European Renewable must sit after LNG Value Chain in the tab order'
);

assert(
  js.includes("if(id==='euren'){renderEuroRenewables();return;}") &&
    js.includes("id==='ga-tab-euren'") &&
    js.includes('function renderEuroRenewables()') &&
    js.includes('euren_prefs_v1'),
  'European Renewable tab must be routed, renderable, and persist preferences'
);

assert(
  js.includes("hydro: {label:'Hydro'") &&
    js.includes("'B10','B11','B12'") &&
    js.includes("wind:  {label:'Wind'") &&
    js.includes("'B18','B19'") &&
    js.includes("solar: {label:'Solar'") &&
    js.includes("'B16'"),
  'European Renewable must define hydro, wind, and solar ENTSO-E PSR mappings'
);

assert(
  js.includes('/api/entsoe?') &&
    !js.includes('securityToken=') &&
    js.includes("documentType:'A75'") &&
    js.includes("processType:'A16'") &&
    js.includes("documentType:'A68'") &&
    js.includes("processType:'A33'") &&
    js.includes('ENTSO-E Transparency Platform Actual Generation per Type'),
  'European Renewable must use the existing ENTSO-E proxy and cite generation plus installed capacity'
);

assert(
  js.includes('psrType:psr') &&
    js.includes('euren_${countryId}_${tech}_${year}') &&
    js.includes('euren_cap_${countryId}_${tech}_${year}') &&
    js.includes('erMergeEntsoeBundle'),
  'European Renewable ENTSO-E calls must fetch selected PSR codes and merge them, not request whole-country all-tech years'
);

assert(
  proxy.includes('process.env.ENTSOE_API_KEY || process.env.ENTSOE_SECURITY_TOKEN') &&
    proxy.includes("params.delete('securityToken')") &&
    proxy.includes("params.set('securityToken', apiKey)"),
  'ENTSO-E proxy must keep the security token server-side and ignore browser-supplied tokens'
);

assert(
    js.includes('function erFetchCapacityCountryYear') &&
    js.includes('function erParseCapacityXml') &&
    js.includes('function erCapacityGW') &&
    js.includes('function erFormatEntsoeUpdate') &&
    js.includes('Last ENTSO-E update:') &&
    js.includes('INSTALLED CAPACITY') &&
    js.includes('CAPACITY YoY') &&
    js.includes('er-capacity-chart') &&
    js.includes('Capacity GW') &&
    js.includes('YoY add GW'),
  'European Renewable must show installed capacity and YoY capacity increase'
);

assert(
  js.includes('Missing API/key/data stays blank') &&
    js.includes('ENTSO-E proxy is available on deployed / localhost app, not file preview') &&
    !js.slice(js.indexOf('const ER_PREF_KEY'), js.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// LNG VALUE CHAIN')).includes('Math.random()'),
  'European Renewable must not fabricate fallback renewable data'
);

if (!process.exitCode) console.log('European Renewable regression checks passed');
