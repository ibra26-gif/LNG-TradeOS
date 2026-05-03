const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  js.includes("'TTF DIFFERENTIAL'") &&
    js.includes('function rgDiff()') &&
    js.includes('else if(RG.tab===2)b.innerHTML=rgDiff()'),
  'EU Regas Model must route the TTF Differential tab to rgDiff()'
);

assert(
  js.includes("const isNA=t.id==='stade' && typeof _stadeValid==='function' && !_stadeValid(i)") &&
    js.includes('const noVal=isNA||des==null') &&
    !js.includes("const isNA=t.id==='stade'&&i<2"),
  'TTF Differential must use Stade validity rules and avoid calling toFixed() on null DES values'
);

if (!process.exitCode) console.log('EU Regas TTF Differential regression checks passed');
