const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');
const todo = fs.readFileSync(path.join(root, 'TODO.md'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  js.includes("const m1Cogh = arbData.today?.arbCogh?.[0]") &&
    js.includes("COGH ARB · M+1") &&
    js.includes("The current/assessment month is not used"),
  'USGC COGH arb must expose an explicit M+1 readout'
);

assert(
  todo.includes('Spread closing') &&
    todo.includes('Correlation changing') &&
    todo.includes('Outright increasing/decreasing'),
  'historical price-analysis notifications must be tracked in TODO.md'
);

assert(
  app.includes('20260501-usgc-m1-arb'),
  'cache-bust must be bumped for USGC M+1 arb change'
);

if (!process.exitCode) console.log('USGC arb M+1 regression checks passed');
