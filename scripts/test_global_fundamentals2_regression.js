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
  app.includes("id=\"gmtab-dashboard2\"") &&
    app.includes('FUNDAMENTALS DASHBOARD 2.0') &&
    app.includes('id="ga-tab-dashboard2"') &&
    /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'Gas Analytics must expose a separate Fundamentals dashboard 2.0 tab with dated cache-bust'
);

assert(
  js.includes("if(id==='dashboard2'){renderGaDashboard2();return;}") &&
    js.includes("id==='ga-tab-dashboard2'") &&
    js.includes('async function renderGaDashboard2()'),
  'dashboard2 routing must render and refresh its own view'
);

assert(
  js.includes('1. EUROPEAN GAS FUNDAMENTALS') &&
    js.includes('PIPELINE IMPORT WATCH') &&
    js.includes('NORWEGIAN OUTAGE WATCH') &&
    js.includes('FRENCH NUCLEAR') &&
    js.includes('No domestic production here by design') &&
    js.includes("k:'algeria_italy'") &&
    js.includes("k:'algeria_spain'") &&
    js.includes("k:'tap'") &&
    js.includes("k:'russia'") &&
    js.includes("k:'uk_interconn'"),
  'European half must focus on LNG, Norway, Russia, Algeria IT/ES, TAP, tracked interconnectors, and French nuclear below outage watch'
);

assert(
  js.includes('2. LNG FUNDAMENTALS SIGNPOSTS') &&
    js.includes('KOREAN NUCLEAR') &&
    js.includes('JAPANESE NUCLEAR') &&
    js.includes('BRAZILIAN HYDRO') &&
    js.includes('JKM FLAT vs CHINA LNG TRUCK') &&
    js.includes('No live Japan nuclear feed wired yet'),
  'LNG half must include Korean/Japanese nuclear, Brazilian hydro, and JKM vs China truck'
);

assert(
  js.indexOf('NORWEGIAN OUTAGE WATCH') > -1 &&
    js.indexOf('gf2-fr-nuclear') > js.indexOf('NORWEGIAN OUTAGE WATCH') &&
    js.indexOf('gf2-fr-nuclear') < js.indexOf('2. LNG FUNDAMENTALS SIGNPOSTS') &&
    !js.includes('>EU LNG SENDOUT</div>'),
  'dashboard2 must place French nuclear below Norwegian outage watch and remove the LNG-half EU sendout card'
);

assert(
  js.includes("_loadFrNuclear('gf2-fr-nuclear')") &&
    js.includes("_loadBrzHydroCard('gf2')") &&
    js.includes('_loadGf2ChinaTruckCard()') &&
    js.includes("['gf','gf2'].forEach(prefix=>"),
  'dashboard2 must reuse existing French nuclear, Brazil hydro, China truck, and Korea nuclear loaders'
);

assert(
  !js.includes("row('Power demand") &&
    !js.includes("row('Industry demand") &&
    !js.includes("row('Residential / heating demand"),
  'new fundamentals work must not reintroduce unsourced sector demand placeholders'
);

if (!process.exitCode) console.log('Global Fundamentals 2.0 regression checks passed');
