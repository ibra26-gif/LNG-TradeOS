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
  js.includes('weekly LNG storage') &&
    js.includes('SOURCE GAP') &&
    js.includes('no estimated tank stock') &&
    js.includes('OCCTO navigation'),
  'Japan Gas Balance must surface source gaps rather than invented storage or gas-for-power data'
);
assert(
  js.includes('JAPAN NUCLEAR GENERATION') &&
    js.includes('Ember Monthly Electricity Data') &&
    js.includes('jpn-nuclear-seasonal') &&
    js.includes('jpnUpdateGlobalFundamentalsCard'),
  'Japan Power must chart real monthly nuclear generation from Ember'
);
assert(
  js.includes('gf2-jpn-nuclear-val') &&
    js.includes('gf2-jpn-nuclear-share') &&
    js.includes('EMBER'),
  'Global Fundamentals 2.0 must show the Japan nuclear level from Japan analytics data'
);
assert(
  js.includes('JMA long-range forecast') &&
    js.includes('Open-Meteo numeric forecast') &&
    js.includes('jpn-weather-chart'),
  'Japan Weather Forecast must show JMA source context and numeric forecast status'
);
assert(data.schema === 'japan_analytics_v1', 'Japan data schema missing');
assert(data.gasBalance?.status === 'source_gap', 'Japan gas balance must declare source_gap');
assert(data.gasBalance.weeklyLngStorage === null, 'Japan weekly LNG storage must stay null until sourced');
assert(Array.isArray(data.nuclear?.series) && data.nuclear.series.length > 60, 'Japan nuclear series should include multi-year monthly rows');
assert(data.nuclear.series.at(-1).twh != null, 'Japan latest nuclear row needs TWh');
assert(data.nuclear.source === 'Ember Monthly Electricity Data', 'Japan nuclear source must be Ember');
assert(Array.isArray(data.weather?.national) && data.weather.national.length >= 7, 'Japan weather needs at least seven forecast days');
assert(data.weather.officialJmaLongRangeUrl?.includes('jma.go.jp'), 'Japan weather must preserve the official JMA long-range source link');
assert(
  scraper.includes('Area") != "Japan"') &&
    scraper.includes('Variable") != "Nuclear"') &&
    scraper.includes('OPEN_METEO_URL') &&
    scraper.includes('"weeklyLngStorage": None'),
  'Japan scraper must fetch real nuclear/weather data and keep gas storage as an explicit gap'
);

if (!process.exitCode) console.log('Japan analytics regression checks passed');
