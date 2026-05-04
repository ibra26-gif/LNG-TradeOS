const fs = require('fs');
const path = require('path');
const assert = require('assert');

const root = path.resolve(__dirname, '..');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const nao = fs.readFileSync(path.join(root, 'api/weather-nao.js'), 'utf8');

assert(app.includes('Weather Tracking'), 'home card must expose Weather Tracking');
assert(app.includes('id="gmtab-weather"'), 'Gas & LNG main tab must include Weather Tracking');
assert(app.includes('id="ga-tab-weather"') && app.includes('id="ga-weather-pane"'), 'Weather Tracking pane missing');
assert(/lngtradeos\.js&v=20260504-/.test(app), 'cache bust must stay bumped after Weather Tracking app JS change');

assert(js.includes('WEATHER TRACKING — Open-Meteo HDD/CDD, wind and NAO regime'), 'Weather module marker missing');
assert(js.includes("const WX_GROUPS"), 'Weather market groups missing');
assert(js.includes("id:'europe'") && js.includes("id:'asia'") && js.includes("id:'other-lng'"), 'Weather market groups must cover Europe, Asia, and other LNG demand centers');
assert(js.includes("id:'iberia'") && js.includes("id:'baltic'") && js.includes("id:'nwe'") && js.includes("id:'med'"), 'Europe region drilldowns missing');
assert(js.includes('archive-api.open-meteo.com/v1/archive'), 'Weather actuals must use Open-Meteo Historical Weather API');
assert(js.includes('seasonal-api.open-meteo.com/v1/seasonal'), 'Weather forecast must use Open-Meteo Seasonal API');
assert(js.includes('temperature_2m_mean') && js.includes('wind_speed_100m_mean'), 'Weather variables must include temperature and wind');
assert(js.includes('wx-seasonal-chart') && js.includes('wx-yoy-chart') && js.includes('wx-forecast-chart') && js.includes('wx-regime-chart'), 'Weather tab must keep the four requested charts');
assert(js.includes('No demand weights are inferred') && js.includes('equal-city weather basket'), 'Weather dashboard must label basket logic and avoid invented demand weights');
assert(js.includes('Missing month = blank, not zero'), 'Weather monthly chart must not treat gaps as zero');
assert(js.includes('/api/weather-nao'), 'Weather NAO panel must use the local NOAA/CPC proxy');
assert(js.includes('https://github.com/open-meteo/open-meteo'), 'Weather source table must link Open-Meteo GitHub');

assert(nao.includes('NOAA/CPC NAO CSV proxy'), 'NOAA/CPC NAO proxy marker missing');
assert(nao.includes('norm.daily.nao.cdas.z500.19500101_current.csv'), 'NAO observed CSV source missing');
assert(nao.includes('norm.daily.nao.gfs.z500.120days.csv'), 'NAO GFS forecast CSV source missing');
assert(nao.includes('norm.daily.nao.gefs.z500.120days.csv'), 'NAO GEFS member CSV source missing');
assert(!/API_KEY|SECURITY_TOKEN|process\.env\.[A-Z_]*KEY/.test(nao), 'NAO proxy must not require a secret key');

console.log('Weather Tracking regression ok: Open-Meteo actuals/seasonal forecast, four charts, NAO proxy, no demand-weight invention');
