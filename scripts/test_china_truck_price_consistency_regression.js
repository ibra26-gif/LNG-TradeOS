const fs = require('fs');
const path = require('path');
const assert = require('assert');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');

assert(/lngtradeos\.js&v=20260504-/.test(app), 'cache bust must stay bumped after China truck display logic change');
assert(js.includes('function chinaNorthTruckSeries()'), 'China truck helper missing');
assert(js.includes("return { north:['华北'], east:['华东'], south:['华南'] }[key] || [];"), 'China truck helper must prefer SHPGX aggregate regions');
assert(js.includes('return [];'), 'China truck helper must return a gap when no real SHPGX truck data is loaded');
assert(js.includes('function truckToUSD(cny_kg, usdcny) { return +(cny_kg*1000/(52.5*usdcny)).toFixed(3); }'), 'Gas Price Chain truck conversion must use 52.5 MMBtu/t');
assert(!js.includes('truckToUSD(cny_kg, usdcny) { return +(cny_kg*1000/(52*usdcny))'), 'Old 52.0 truck conversion must not remain');
assert(js.includes('const truckRaw=chinaNorthTruckSeries();'), 'China Gas Prices must use same North China truck helper');
assert(js.includes('const truckData = chinaTruckSeriesForRegion(region);'), 'Gas Price Chain must use selected-region SHPGX truck data');
assert(js.includes('No simulated price is shown.'), 'Gas Price Chain must blank missing SHPGX instead of displaying simulated values');
assert(js.includes('truckCny*1000/fx/52.5'), 'Global Fundamentals 2.0 truck card must use same 52.5 conversion');

console.log('China truck price consistency regression ok: North China SHPGX series and 52.5 conversion are aligned');
