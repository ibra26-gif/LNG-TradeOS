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
  js.includes("const PHYS_DIFF_SNAP_KEY = 'phys_diff_snapshots'") &&
    js.includes('function _physDiffLoadSnapshots') &&
    js.includes('function _physDiffCaptureSnapshot') &&
    js.includes('observation_date → destination → delivery_month → physical differential') &&
    js.includes('source:\'LNG Global Netback PHYS DIFFERENTIALS · CP.phys + cpDerived\''),
  'physical differential history must persist observation-date forward-curve snapshots from CP.phys/cpDerived'
);

assert(
  js.includes('function _physDiffRows') &&
    js.includes("label:'DES MEI'") &&
    js.includes("label:'DES Thailand'") &&
    js.includes("label:'DES NWE'") &&
    js.includes("label:'DES JKTC'") &&
    js.includes("data:d.phyMei") &&
    js.includes("data:d.phyThailand"),
  'physical differential rows must include existing netback destinations and derived MEI/Thailand curves'
);

assert(
  js.includes('PHYSICAL DIFFERENTIAL HISTORY · forward curve / historical tenor') &&
    js.includes('phys-diff-hist-curve') &&
    js.includes('phys-diff-hist-series') &&
    js.includes('cp_phys_hist_obs_date') &&
    js.includes('cp_phys_hist_dest') &&
    js.includes('cp_phys_hist_tenor') &&
    js.includes('cp_phys_hist_window') &&
    js.includes('This is not ACER'),
  'physical differential tab must expose forward-curve and historical-tenor views with persisted controls'
);

assert(
  js.includes("if(CP.tab===0){b.innerHTML=cpPhysDiff(d); setTimeout(()=>{ try{_physDiffRenderHistoryCharts(d);}") &&
    js.includes('function cpPhysTouch') &&
    js.includes("_physDiffCaptureSnapshot({replace:true})") &&
    js.includes("if (kind === 'phys')    localStorage.setItem('phys_last_ts'"),
  'phys-diff edits must refresh timestamps, snapshot curves, and render charts after DOM mount'
);

assert(
  app.includes('20260502-phys-fob-history') &&
    /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private platform script must be cache-busted for phys/FOB history work'
);

if (!process.exitCode) console.log('physical differential history regression checks passed');
