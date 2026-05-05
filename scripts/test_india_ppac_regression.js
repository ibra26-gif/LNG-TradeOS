const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const scraper = fs.readFileSync(path.join(root, 'scripts/fetch_india_ppac.py'), 'utf8');
const workflow = fs.readFileSync(path.join(root, '.github/workflows/india-weekly.yml'), 'utf8');
const balance = JSON.parse(fs.readFileSync(path.join(root, 'data/india_gas_balance.json'), 'utf8'));
const prices = JSON.parse(fs.readFileSync(path.join(root, 'data/india_gas_prices.json'), 'utf8'));

function assert(cond, msg) {
  if (!cond) {
    console.error('FAIL:', msg);
    process.exitCode = 1;
  }
}

assert(app.includes('20260504-india-ppac'), 'India PPAC change must bump the app JS cache bust');
assert(balance.schema === 'india_gas_balance_v1', 'India gas balance schema missing');
assert(balance.status === 'parsed', 'India gas balance must parse PPAC files');
assert(Array.isArray(balance.rows) && balance.rows.length >= 150, 'India balance should include multi-year PPAC monthly rows');
assert(balance.latest_month === balance.rows.at(-1).month, 'India latest month must match final row');
assert(balance.rows.some(r => r.month === '2026-03'), 'India balance should include PPAC March 2026 row when sourced');
const feb = balance.rows.find(r => r.month === '2026-02');
assert(feb && feb.prod > 0 && feb.rlng > 0 && feb.total_demand > 0, 'February 2026 row needs sourced production, LNG and demand');
assert(Math.abs((feb.prod + feb.rlng) - feb.total_supply) < 0.01, 'India total supply must equal sourced production plus RLNG');
assert(balance.notes.some(n => n.includes('No missing PPAC month is estimated')), 'India balance must disclose no-estimate rule');

assert(prices.schema === 'india_gas_prices_v1', 'India gas prices schema missing');
assert(prices.records.some(r => r.kind === 'apm' && r.valid_from === '2026-05' && r.price_usd_mmbtu === 7), 'May 2026 APM price must be sourced from PPAC');
assert(prices.records.some(r => r.kind === 'ceiling' && r.valid_from === '2026-04' && r.valid_to === '2026-09' && r.price_usd_mmbtu === 8.9), 'Apr-Sep 2026 ceiling must keep full validity window');
assert(!prices.records.some(r => r.kind === 'ceiling' && /Domestic/i.test(r.source_file || '')), 'Domestic gas price PDFs must not be classified as ceiling');

assert(scraper.includes('No row is estimated') && scraper.includes('existing committed history is preserved'), 'India scraper must preserve no-invention source rules');
assert(scraper.includes('tesseract') && scraper.includes('pdftoppm'), 'India scraper must support OCR for scanned PPAC PDFs');
assert(scraper.includes('first_nonblank') && !scraper.includes('row.get("domesticProductionMmscm") or row.get("prod")'), 'India scraper must not drop sourced zero values during field merge');
assert(workflow.includes('0 9 * * 0') && workflow.includes('scripts/fetch_india_ppac.py'), 'India workflow must refresh weekly on Sunday 10:00 London during BST');

assert(js.includes('/data/india_gas_balance.json') && js.includes('/data/india_gas_prices.json'), 'India UI must load committed PPAC JSON files');
assert(js.includes('Missing PPAC row = blank, not modelled'), 'India UI must disclose missing-row behavior');
assert(!js.includes('Object.values(IND_APM).pop'), 'India APM must not roll stale values forward');
assert(!js.includes('const IND_APM'), 'India APM hardcoded table should be removed');
assert(!js.includes('const IND_DEEPWATER'), 'India deepwater hardcoded table should be removed');
assert(js.includes('supply=indNum(d.total_supply)') && !js.includes('(d.prod||0)+(d.rlng||0)-(d.total_demand||0)'), 'India gap chart must not zero-fill missing inputs');
assert(!js.includes("'lng_shpgx_v1','lng_china_gb_v1','lng_india_gb_v1'"), 'Public state should not resync stale browser-only India balance');

if (!process.exitCode) {
  console.log(`India PPAC regression ok: ${balance.rows.length} balance rows, latest ${balance.latest_month}; ${prices.records.length} price records`);
}
