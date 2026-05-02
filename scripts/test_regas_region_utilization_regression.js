const fs = require('fs');
const path = require('path');

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
  app.includes("regasTab('regions',this)") &&
    app.includes('REGIONAL UTILISATION') &&
    /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'LNG Regas must expose a regional utilisation tab and keep cache-bust dated'
);

assert(
  js.includes("const REGAS_REGION_PREF_KEY = 'regas_region_prefs_v1'") &&
    js.includes('const REGAS_REGION_DEFS = [') &&
    js.includes("label:'NWE'") &&
    js.includes("label:'Med'") &&
    js.includes("label:'Baltic'") &&
    js.includes("label:'Iberia'"),
  'regional utilisation must define persisted NWE/Med/Baltic/Iberia buckets'
);

assert(
  js.includes('function regasTerminalRegion') &&
    js.includes("ctry==='fr' && n.includes('fos')") &&
    js.includes("ctry==='de' && (n.includes('lubmin')||n.includes('mukran'))") &&
    js.includes("ctry==='es'||ctry==='pt'"),
  'terminal region classification must handle French Med, German Baltic, and Iberia explicitly'
);

assert(
  js.includes("else if(sub==='regions')    renderRegasRegionalUtil(pane)") &&
    js.includes("txt.includes('regional') ? 'regions'") &&
    js.includes("'regions':2"),
  'regas routing must preserve the regional tab through refresh/rerender'
);

assert(
  js.includes('async function renderRegasRegionalUtil') &&
    js.includes('async function regasLoadRegionTerminals') &&
    js.includes('function regasRegionAgg') &&
    js.includes('function regasDrawRegionalUtil') &&
    js.includes('weighted utilisation = sum(sendOut) / sum(dtrs)'),
  'regional renderer must load terminal rows and compute weighted utilisation'
);

assert(
  js.includes('company=${encodeURIComponent(t.company)}&facility=${encodeURIComponent(t.facility)}') &&
    js.includes('Source: GIE ALSI terminal sendOut and dtrs') &&
    js.includes('converted GWh/d to mcm/d via /10.55'),
  'regional ALSI queries must use country+company+facility and cite sendOut/dtrs conversion'
);

assert(
  js.includes('AVAILABLE REGAS CAPACITY — SEASONAL') &&
    js.includes('regas-cap-seas') &&
    js.includes('const capDs = seasYrs.map') &&
    js.includes('r.dtrsGWh ? r.dtrsGWh*GWH_MCM : null') &&
    js.includes('denominator behind utilisation') &&
    js.includes('sendOut converted from GWh/d to mcm/d via /10.55'),
  'regas seasonal view must show DTRS capacity denominator and correct sendout unit caption'
);

if (!process.exitCode) console.log('regas regional utilisation regression checks passed');
