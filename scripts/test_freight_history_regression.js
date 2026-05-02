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
  /function fHistMergeCurves\(results\)[\s\S]*?localUpload[\s\S]*?oldBad[\s\S]*?newerDrive/.test(js),
  'historical freight merge must replace same-date corrupt or locally re-uploaded curves'
);

assert(
  js.includes('if(refreshEditor&&typeof _fRefreshFreightCurveEditor') && !js.includes('if(refreshEditor&&typeof fMatTab===\'function\')fMatTab(0)'),
  'Drive sync refresh must not jump to hidden tab index 0'
);

assert(
  js.includes("F.histVbMode=fg('f_hist_vb_mode_v1','spread')") &&
    js.includes('DUAL AXIS · FREIGHT SPREAD vs JKM−TTF BASIS') &&
    js.includes('BLNG1 - BLNG2') &&
    js.includes('③ BASIS vs FREIGHT'),
  'freight history must include default freight-spread vs basis analysis'
);

assert(
  /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private platform script must carry a dated cache-bust'
);

if (!process.exitCode) console.log('freight history regression checks passed');
