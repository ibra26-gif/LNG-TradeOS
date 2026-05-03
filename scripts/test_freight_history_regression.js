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
  js.includes('③ HISTORICAL FREIGHT CURVES') && js.includes('renderHistoricalStandalone'),
  'freight history must remain a top-level Freight tab'
);

assert(
  /async function parseHistExcel\(el\)[\s\S]*?results\.push\(fHistParseWorkbook\(buf,file\.name/.test(js),
  'manual Excel upload must use the shared robust freight parser'
);

assert(
  /function parseBLNGxls\(el\)[\s\S]*?fHistParseWorkbook\(e\.target\.result,f\.name/.test(js),
  'active Freight Excel upload must use the shared robust freight parser'
);

assert(
  js.includes('function fBlngHasBadValues') &&
    js.includes('n<5000||n>500000') &&
    js.includes('repairActiveBlng') &&
    js.includes('Cannot save snapshot: BLNG curve has invalid $/day values'),
  'freight curves must reject cached Date-column garbage before saving or loading'
);

assert(
  js.includes('cPeriod') &&
    js.includes('quarterly/calendar strips with M=1') &&
    js.includes('/\\+\\d+_?M\\b/.test(label)') &&
    js.includes("label.includes('CAL')"),
  'shared freight parser must exclude quarterly/calendar rows before applying M index'
);

assert(
  /function fHistMergeCurves\(results\)[\s\S]*?localUpload[\s\S]*?oldBad[\s\S]*?newerDrive/.test(js),
  'historical freight merge must replace same-date corrupt or locally re-uploaded curves'
);

assert(
  /const toProcess=valid\.filter\(f=>\{[\s\S]*?if\(!old\|\|!fBlngIsValid\(old\.blng\)\)return true;[\s\S]*?if\(seen\[f\.id\]&&seen\[f\.id\]===f\.modifiedTime\)return false;/.test(js),
  'Drive sync must reload missing/cleaned dates even when the old file id was marked seen'
);

assert(
  /async function parseHistExcel\(el\)[\s\S]*?const m=fHistMergeCurves\(results\)/.test(js),
  'manual historical Excel upload must use validated merge semantics'
);

assert(
  js.includes('if(refreshEditor&&typeof _fRefreshFreightCurveEditor') && !js.includes('if(refreshEditor&&typeof fMatTab===\'function\')fMatTab(0)'),
  'Drive sync refresh must not jump to hidden tab index 0'
);

assert(
  js.includes("F.histVbBlng=fg('f_hist_vb_blng',{BLNG1:true,BLNG2:true,BLNG3:true})") &&
    js.includes('HISTORICAL FREIGHT · ${ML[tenor]}') &&
    js.includes('JKM/TTF SPREAD · ${ML[tenor]}') &&
    js.includes('Both use the same selected delivery month, not rolling M+1') &&
    js.includes('③ BASIS vs FREIGHT'),
  'basis vs freight must be two stacked historical charts for the selected contract month'
);

assert(
  /function histSpreadView\(\)[\s\S]*?function mkChart\(series[\s\S]*?const chart = mkChart\(lineData/.test(js),
  'freight spread history view must own its chart renderer instead of calling a helper scoped to another view'
);

assert(
  js.includes("F.histStatus=fg('f_hist_status_v1',null)") &&
    js.includes('function fHistHealth()') &&
    js.includes('function fHistHealthPanel()') &&
    js.includes('FREIGHT HISTORY HEALTH') &&
    js.includes('REPAIR + DRIVE RELOAD'),
  'freight history must expose persisted health, latest source, and repair controls'
);

assert(
  js.includes('function fHistRepairHistory(reloadDrive=false)') &&
    js.includes("fs('f_hist_curves',F.histCurves)") &&
    js.includes('filter(s=>s&&s.date&&fBlngIsValid(s.blng))') &&
    js.includes('syncFreightHistFromDrive(false,true)'),
  'freight repair must purge only invalid cached curves and optionally reload Drive'
);

assert(
  js.includes('function fHistSafeView(name,renderFn)') &&
    /return fHistSafeView\(v,\(\)=>\{[\s\S]*?if\(v==='spread'\)[\s\S]*?return histSpreadView/.test(js) &&
    js.includes('FREIGHT HISTORY VIEW FAILED SAFELY') &&
    js.includes("F.histView='historical'"),
  'freight history subviews must be wrapped so one broken chart cannot blank the whole tab'
);

assert(
  /async function syncFreightHistFromDrive\(silent=false,refreshEditor=false\)[\s\S]*?fHistSetStatus\(\{kind:'drive'[\s\S]*?Drive already synced[\s\S]*?fHistSetStatus\(\{kind:'drive'[\s\S]*?Drive sync failed/.test(js),
  'Drive sync must persist visible status for running, already-synced, success, and failure states'
);

assert(
  /async function parseHistExcel\(el\)[\s\S]*?fHistSetStatus\(\{kind:'excel'[\s\S]*?const m=fHistMergeCurves\(results\)[\s\S]*?Excel import/.test(js) &&
    /async function parseHistImages\(el\)[\s\S]*?fHistSetStatus\(\{kind:'image'[\s\S]*?const m=fHistMergeCurves\(results\)[\s\S]*?Image import/.test(js),
  'manual Excel and image imports must use validated merge semantics and update health status'
);

assert(
  /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private platform script must carry a dated cache-bust'
);

if (!process.exitCode) console.log('freight history regression checks passed');
