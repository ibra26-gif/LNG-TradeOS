const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const data = JSON.parse(fs.readFileSync(path.join(root, 'data/japan.json'), 'utf8'));
const scraper = fs.readFileSync(path.join(root, 'scripts/fetch_japan.py'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error('FAIL:', msg);
    process.exitCode = 1;
  }
}

assert(app.includes('id="jpn-container"'), 'Japan tab must render a real container, not a placeholder');
assert(js.includes("country==='japan'") && js.includes('jpnInit'), 'Asia Japan tab must initialize jpnInit');
assert(
  js.includes("['gas','GAS BALANCE']") &&
    js.includes("['power','JAPAN POWER']") &&
    js.includes("['weather','WEATHER FORECAST']"),
  'Japan analytics must expose Gas Balance, Japan Power, and Weather Forecast tabs'
);
assert(
  js.includes('JAPAN GAS BALANCE SOURCE STACK') &&
    js.includes('JAPAN LNG IMPORT SUPPLY') &&
    js.includes('MONTHLY JAPAN GAS BALANCE') &&
    js.includes('Japan Customs HS 271111000') &&
    js.includes('METI Gas Business') &&
    js.includes('METI Electric Power') &&
    js.includes('JOGMEC') &&
    js.includes('Missing source row = gap, not modelled value') &&
    js.includes('partial imports chart wired; demand/storage pending'),
  'Japan Gas Balance must render sourced Customs imports while keeping missing demand/storage as explicit gaps'
);
assert(
  js.includes('JAPAN NUCLEAR UTILIZATION') &&
    js.includes('Ember Monthly Electricity Data') &&
    js.includes('jpn-nuclear-seasonal') &&
    js.includes('text:\'% utilization\'') &&
    js.includes('jpnUpdateGlobalFundamentalsCard'),
  'Japan Power must chart sourced monthly nuclear utilization derived from Ember generation'
);
assert(
  js.includes('gf2-jpn-nuclear-val') &&
    js.includes('gf2-jpn-nuclear-share') &&
    js.includes('jpnNuclearUtilPct') &&
    js.includes('EMBER'),
  'Global Fundamentals 2.0 must show the Japan nuclear level/utilization from Japan analytics data'
);
assert(
  js.includes('JMA long-range forecast') &&
    js.includes('Open-Meteo numeric forecast') &&
    js.includes('jpn-weather-chart'),
  'Japan Weather Forecast must show JMA source context and numeric forecast status'
);
assert(data.schema === 'japan_analytics_v1', 'Japan data schema missing');
assert(data.gasBalance?.status === 'partial_balance', 'Japan gas balance must declare partial_balance once Customs imports are parsed');
assert(Array.isArray(data.gasBalance.monthlyRows) && data.gasBalance.monthlyRows.length >= 12, 'Japan monthly balance rows must include parsed Customs LNG imports');
assert(data.gasBalance.weeklyLngStorage === null, 'Japan weekly LNG storage must stay null until sourced');
const latestGas = data.gasBalance.monthlyRows.at(-1);
assert(latestGas?.month === data.gasBalance.lngImports?.latestMonth, 'Japan gas balance latest row must match Customs latest month');
assert(latestGas?.lngImportsMillionTonnes > 0, 'Japan Customs LNG import row must include positive Mt/month');
assert(latestGas?.lngImportsValueJpyBillion > 0 && latestGas.lngImportsValueJpyBillion < 1000, 'Japan Customs LNG import value must be expressed in JPY bn, not raw JPY000');
assert(latestGas?.coverage?.customsLngImports === true, 'Japan latest row must mark Customs import coverage');
assert(latestGas?.coverage?.metiGasBusiness === false && latestGas?.cityGasDemand === null, 'Japan city gas must remain an explicit gap until METI Gas Business is parsed');
assert(latestGas?.coverage?.metiPowerFuel === false && latestGas?.powerLngBurn === null, 'Japan power LNG burn must remain an explicit gap until METI power fuel is parsed');
assert(
  Array.isArray(data.gasBalance.sourceMap) &&
    data.gasBalance.sourceMap.some(s => s.source?.includes('Japan Customs') && s.status === 'wired' && s.pull?.includes('HS 271111000')) &&
    data.gasBalance.sourceMap.some(s => s.source?.includes('Gas Business Production Dynamic Statistics')) &&
    data.gasBalance.sourceMap.some(s => s.source?.includes('Electric Power Survey Statistics')) &&
    data.gasBalance.sourceMap.some(s => s.source?.includes('JOGMEC')),
  'Japan gas balance source map must preserve Customs, METI gas, METI power, and JOGMEC sources'
);
assert(Array.isArray(data.nuclear?.series) && data.nuclear.series.length > 60, 'Japan nuclear series should include multi-year monthly rows');
assert(data.nuclear.series.at(-1).twh != null, 'Japan latest nuclear row needs TWh');
assert(data.nuclear.source === 'Ember Monthly Electricity Data', 'Japan nuclear source must be Ember');
assert(data.nuclear.capacityReference?.operableGw > 0, 'Japan nuclear utilization needs a sourced operable capacity reference');
assert(data.nuclear.capacityReference?.restartAdjustedGw > 0, 'Japan nuclear utilization must use a restart-adjusted denominator');
assert(
  data.nuclear.capacityReference?.note?.includes('restart review') &&
    data.nuclear.capacityReference?.note?.includes('maintenance'),
  'Japan nuclear capacity note must explain restart-review exclusions and maintenance/offline treatment'
);
assert(Array.isArray(data.weather?.national) && data.weather.national.length >= 7, 'Japan weather needs at least seven forecast days');
assert(data.weather.officialJmaLongRangeUrl?.includes('jma.go.jp'), 'Japan weather must preserve the official JMA long-range source link');
assert(
    scraper.includes('Area") != "Japan"') &&
    scraper.includes('Variable") != "Nuclear"') &&
    scraper.includes('capacityReference') &&
    scraper.includes('"restartAdjustedGw": 14.609') &&
    scraper.includes('JAPAN_GAS_BALANCE_SOURCE_MAP') &&
    scraper.includes('load_customs_lng_imports') &&
    scraper.includes('CUSTOMS_HS_LNG = "271111000"') &&
    scraper.includes('monthlyRows') &&
    scraper.includes('OPEN_METEO_URL') &&
    scraper.includes('"weeklyLngStorage": None'),
  'Japan scraper must fetch real nuclear/weather data, parse Customs LNG imports, and keep unparsed Japan balance legs as gaps'
);

assert(
  js.includes('const capGw = capRef.restartAdjustedGw || capRef.operableGw') &&
    js.includes('GW restart-adjusted fleet') &&
    js.includes('Restart-review reactors are excluded until proper commercial restart'),
  'Japan nuclear UI must calculate and label utilization with the restart-adjusted fleet denominator'
);

if (!process.exitCode) console.log('Japan analytics regression checks passed');
