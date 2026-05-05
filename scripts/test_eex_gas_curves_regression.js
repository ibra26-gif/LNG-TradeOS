#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const root = path.resolve(__dirname, '..');
const scraper = fs.readFileSync(path.join(root, 'scripts/fetch_eex_gas_curves.py'), 'utf8');
const workflow = fs.readFileSync(path.join(root, '.github/workflows/eex-daily.yml'), 'utf8');

function assert(cond, msg){
  if(!cond){
    console.error('FAIL:', msg);
    process.exit(1);
  }
}

assert(scraper.includes('https://api.eex-group.com/pub/market-data'), 'EEX refresh must use the current public JSON market-data endpoint');
assert(scraper.includes('https://api.eex-group.com/pub/customise-widget'), 'EEX refresh must discover contracts through the widget filter endpoint');
assert(scraper.includes('"Origin": "https://www.eex.com"') && scraper.includes('"Referer": "https://www.eex.com/"'), 'EEX API calls need the widget headers that the public endpoint expects');
assert(scraper.includes('"ZTP": "Belgian ZTP Natural Gas Futures"'), 'EEX refresh must include ZTP, not only THE/PSV/PEG/PVB');
assert(scraper.includes('load_existing()') && scraper.includes('merge_rows(data, scraped)'), 'EEX refresh must merge into existing workbook instead of replacing history');
assert(!scraper.includes('playwright install') && !workflow.includes('playwright install'), 'EEX refresh should not depend on brittle browser automation');
assert(workflow.includes('scripts/fetch_eex_gas_curves.py') && workflow.includes('data/EEX_Gas_Curves.xlsx'), 'EEX workflow must refresh and commit the workbook');
assert(
  workflow.includes("          LATEST=$(python3 - <<'PY'\n          from scripts.fetch_eex_gas_curves import load_existing\n          d=load_existing()\n") &&
    workflow.includes('\n          PY\n          )\n'),
  'EEX workflow inline Python heredoc must stay inside the YAML run block'
);

console.log('EEX gas curves regression ok');
