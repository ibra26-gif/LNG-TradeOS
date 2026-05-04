const fs = require('fs');
const path = require('path');
const assert = require('assert');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const api = fs.readFileSync(path.join(root, 'api/china-gas-balance.js'), 'utf8');
const scraper = fs.readFileSync(path.join(root, 'scripts/fetch_china_shpgx.py'), 'utf8');
const csvHead = fs.readFileSync(path.join(root, 'data/china_gas_balance.csv'), 'utf8').split(/\r?\n/)[0];

const start = js.indexOf('CHINA SIGNPOSTS MODULE');
const end = js.indexOf('/* ─── Block 2: main application script');
assert(start > -1 && end > start, 'China module slice not found');
const china = js.slice(start, end);

assert(!china.includes('Math.random('), 'China module must not generate random/synthetic series');
assert(!china.includes('genShpgxSeries'), 'China SHPGX synthetic generator must not exist');
assert(!china.includes('genPipelineMonthly'), 'China pipeline synthetic generator must not exist');
assert(!china.includes('CN_STOR_ADJ'), 'China gas balance must not infer demand from seasonal storage adjustments');
assert(china.includes('const GB_HISTORICAL = [];'), 'China gas balance must not ship embedded fallback numbers');
assert(china.includes('const latApparent=latR.apparent;'), 'China apparent demand must come from sourced apparent_bcm only');
assert(!china.includes('Supply + storage'), 'China UI must not describe inferred storage-adjusted demand');
assert(china.includes('SOURCE GAP — committed China gas balance CSV unavailable'), 'China gas balance must expose source gaps');

assert(api.includes('return null;'), 'China gas balance API must preserve blanks as null');
assert(!api.includes('isNaN(prod) ? 0 : prod'), 'China gas balance API must not convert missing production to zero');
assert(api.includes('total_bcm: total'), 'China gas balance API must return total_bcm');
assert(api.includes('apparent_bcm: apparent'), 'China gas balance API must return apparent_bcm');
assert(csvHead.includes('total_bcm') && csvHead.includes('apparent_bcm'), 'China balance CSV must keep total/apparent columns');

assert(scraper.includes('def merge_rows(existing_rows, new_rows):'), 'SHPGX scraper must merge into existing history');
assert(scraper.includes('date/region/grade'), 'SHPGX scraper must document date/region/grade merge key');
assert(!scraper.includes('KEEP_DAYS'), 'SHPGX scraper must not trim stored history by days');
assert(!scraper.includes('dedupe_and_trim'), 'SHPGX scraper must not use the old trim function');

console.log('China analytics integrity regression checks passed');
