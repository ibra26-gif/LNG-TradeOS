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
  app.includes('id="ga-tab-lngvc"') &&
    /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'LNG Value Chain tab must exist and cache-bust must be dated'
);

assert(
  js.includes("['Storage Injection ('+uLbl()+')'") &&
    js.includes("['Withdrawal ('+uLbl()+')'") &&
    js.includes("['Storage Variation ('+uLbl()+')'"),
  'storage injection, withdrawal, and variation table labels must use selected LNG VC unit'
);

assert(
  js.includes("const flowDp=u==='mtpm'?4:u==='gwh'?1:2") &&
    js.includes("const flowUnit=' '+uLbl()") &&
    js.includes('fmtD(latInj,wwInj,flowUnit,flowDp)') &&
    js.includes('fmtD(latWdr,wwWdr,flowUnit,flowDp)') &&
    js.includes('fmtD(latSV,wwSV,flowUnit,flowDp)'),
  'storage deltas must display the selected unit and precision'
);

assert(
  js.includes('const latSV=getNet(code,latMo), lySV=getNet(code,lyMo), wwSV=getNet(code,wwMo)') &&
    !js.includes('Storage Injection (TWh/d)') &&
    !js.includes('Withdrawal (TWh/d)') &&
    !js.includes('Storage Variation (TWh/d)'),
  'storage variation must reuse the selected-unit net getter and no hardcoded TWh/d labels may remain'
);

if (!process.exitCode) console.log('LNG Value Chain unit regression checks passed');
