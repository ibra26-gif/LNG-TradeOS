const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const scraper = fs.readFileSync(path.join(root, 'scripts/fetch_korea.py'), 'utf8');
const workflow = fs.readFileSync(path.join(root, '.github/workflows/korea-daily.yml'), 'utf8');
const korea = JSON.parse(fs.readFileSync(path.join(root, 'data/korea.json'), 'utf8'));

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
  js.includes('2026 · KOGAS tariff · JKM · Brent 11-14% (3,0,1)') &&
    js.includes('kor-tariff-2026') &&
    !js.includes('kor-prices-chart') &&
    !js.includes('kor-tariff-history') &&
    js.includes("_korLive?.kogasTariff") &&
    js.includes('KOGAS direct power') &&
    js.includes('Latest official KOGAS direct') &&
    js.includes('monthly values only · no seasonal averaging') &&
    js.includes('blanks are missing official data, not estimates') &&
    js.includes('const kogas2026') &&
    js.includes('JKM vs KOGAS direct') &&
    js.includes('KOGAS direct power tariff') &&
    js.includes('11% Brent 3-0-1') &&
    js.includes('14% Brent 3-0-1'),
  'Tariffs tab must compare 2026 official KOGAS tariffs, JKM, and Brent slope range without seasonal averaging'
);

assert(
  scraper.includes('KOGAS_POWER_TARIFF_URL') &&
    scraper.includes('def fetch_kogas_current_power_tariff') &&
    !scraper.includes('age_h < 12') &&
    scraper.includes("out['kogasTariff'] = kt") &&
    scraper.includes('kogas_current_power_tariff.json'),
  'Korea scraper must fetch/cache the official KOGAS current power tariff'
);

assert(
  workflow.includes('cron: "17 * * * *"') &&
    workflow.includes('cancel-in-progress: false') &&
    workflow.includes('not acquired by Runner'),
  'Korea hourly workflow must stay off :00 and must not cancel in-progress scrapes'
);

assert(
  korea.kogasTariff &&
    korea.kogasTariff.sourceUrl === 'https://www.kogas.or.kr/site/koGas/1040402000000' &&
    korea.kogasTariff.directPower &&
    korea.kogasTariff.generalPowerChp &&
    korea.kogasTariff.generalPowerChp.supply_krw_gj === 2317.04 &&
    korea.kogasTariff.generalPowerChp.total_krw_gj === 16641.05 &&
    korea.kogasTariff.unit === 'KRW/GJ',
  'korea.json must include the latest cached official KOGAS power tariff'
);

assert(
  /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private bundle must keep a dated cache-bust on lngtradeos.js'
);

if (!process.exitCode) console.log('Korea analytics regression checks passed');
