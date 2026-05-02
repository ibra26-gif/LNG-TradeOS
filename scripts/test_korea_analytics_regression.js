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
  js.includes("let _korTab = 'snd'") &&
    js.includes("tabBtn('snd',     'S&amp;D'") &&
    js.includes("tabBtn('nuclear', 'NUCLEAR'") &&
    js.includes("tabBtn('tariffs', 'TARIFFS'") &&
    !js.includes("tabBtn('balance', 'GAS BALANCE'") &&
    !js.includes("tabBtn('prices',  'PRICES'"),
  'Korea analytics must expose exactly S&D, Nuclear, and Tariffs tab routing'
);

assert(
  js.includes('KESIS / KEEI official monthly gas balance') &&
    js.includes('The page will not invent missing balance rows') &&
    js.includes('KOGAS monthly supply signal') &&
    js.includes('data.go.kr · KOGAS monthly supply by use') &&
    !js.includes('Forward contracted volumes · kt') &&
    !js.includes('LNG procurement origins'),
  'S&D tab must center KESIS/KEEI official balance and keep secondary KOGAS supply signal only'
);

assert(
  js.includes('Seasonal · monthly historical nuclear utilization') &&
    js.includes('kor-seasonal-monthly') &&
    js.includes('Ember TWh ÷ fleet nameplate'),
  'Nuclear tab must keep a monthly historical utilization seasonal chart'
);

assert(
  js.includes('Seasonal · KOGAS tariff · JKM · Brent 11-14% (3,0,1)') &&
    js.includes('kor-tariff-seasonal') &&
    js.includes('avgByMonth') &&
    js.includes('KOGAS power tariff') &&
    js.includes('11% Brent 3-0-1') &&
    js.includes('14% Brent 3-0-1'),
  'Tariffs tab must compare KOGAS tariffs, JKM, and Brent slope range seasonally'
);

assert(
  /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private bundle must keep a dated cache-bust on lngtradeos.js'
);

if (!process.exitCode) console.log('Korea analytics regression checks passed');
