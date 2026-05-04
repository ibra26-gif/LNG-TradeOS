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
    js.includes('euren_prefs_v1') &&
    js.includes('function erCountriesForTech') &&
    js.includes("const ER_EU_ID = 'europe'") &&
    js.includes('function erDashboardArea') &&
    js.includes('function erFetchAreaYear') &&
    js.includes('function erLoadDashboard') &&
    js.includes('Europe Aggregate') &&
    js.includes('onchange="erSetArea(this.value)"') &&
    js.includes('window.erSetArea'),
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
    js.includes('ENTSO-E Transparency Platform Actual Generation per Type'),
  'European Renewable must use the existing ENTSO-E proxy and cite actual generation'
);

assert(
  js.includes('psrType:psr') &&
    js.includes('euren_${countryId}_${tech}_${year}') &&
    js.includes('erMergeEntsoeBundle') &&
    js.includes('missingPsr') &&
    js.includes('erFetchEntsoeXml'),
  'European Renewable ENTSO-E calls must fetch selected generation PSR codes and merge them, not request whole-country all-tech years'
);

assert(
  js.includes('function erFetchEuropeYear') &&
    js.includes("euren_${ER_EU_ID}_${tech}_${year}_tracked_v1") &&
    js.includes('memberOk') &&
    js.includes('memberMissing') &&
    js.includes('Coverage: ${cov}') &&
    js.includes('Europe Aggregate sums mapped ENTSO-E areas separately for hydro, wind and solar') &&
    js.includes('not treated as zero'),
  'European Renewable must support a Europe aggregate seasonal YoY dashboard with coverage disclosure'
);

assert(
  proxy.includes('process.env.ENTSOE_API_KEY || process.env.ENTSOE_SECURITY_TOKEN') &&
    proxy.includes("params.delete('securityToken')") &&
    proxy.includes("params.set('securityToken', apiKey)"),
  'ENTSO-E proxy must keep the security token server-side and ignore browser-supplied tokens'
);

assert(
  js.includes('Object.entries(ER_TECH).map(([tech,cfg])') &&
    js.includes('canvas id="er-seasonal-${tech}"') &&
    js.includes('gaMakeChart(`er-seasonal-${tech}`') &&
    js.includes('function erDrawTechSeasonal') &&
    js.includes('function erRenderDashboardKpis') &&
    js.includes('function erRenderDashboardTable') &&
    js.includes('MONTHLY TABLE — HYDRO / WIND / SOLAR') &&
    js.includes('ER_COUNTRIES.map(c=>`<option value="${c.id}"'),
  'European Renewable must render hydro, wind, and solar seasonal charts together with dashboard KPIs and table'
);

assert(
  js.includes('Missing API/key/data stays blank') &&
    js.includes('ENTSO-E proxy is available on deployed / localhost app, not file preview') &&
    js.includes("techs:['wind','solar']") &&
    js.includes('function erAreaHasTech') &&
    js.includes('function erMissingTechBundle') &&
    js.includes('missing countries or PSR codes are flagged and are not treated as zero') &&
    !js.slice(js.indexOf('const ER_PREF_KEY'), js.indexOf('// ══════════════════════════════════════════════════════════════════════════\n// LNG VALUE CHAIN')).includes('Math.random()'),
  'European Renewable must show all mapped countries while flagging unavailable technologies and not fabricate fallback renewable data'
);

if (!process.exitCode) console.log('European Renewable regression checks passed');
