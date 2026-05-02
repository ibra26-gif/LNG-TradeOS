const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.resolve(__dirname, '..');
const appPath = path.join(root, 'api/private/platform-app.txt');
const portfolioPath = path.join(root, 'portfolio/morning_book.html');

const app = fs.readFileSync(appPath, 'utf8');
const portfolio = fs.readFileSync(portfolioPath, 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  app.includes('src="portfolio/morning_book.html?v=20260502-contract-mtm"'),
  'private platform must iframe the restored Portfolio Valuation page with cache-bust'
);

assert(
  portfolio.length > 500000 &&
    portfolio.includes('Morning Book') &&
    portfolio.includes('Contract MtM') &&
    portfolio.includes('function renderMorningBook()') &&
    portfolio.includes('function renderContractMtm()') &&
    portfolio.includes('function getContractMtmRows') &&
    portfolio.includes('lng-tradeos.state.v1'),
  'portfolio/morning_book.html must remain the full standalone valuation app'
);

const inlineScripts = [...portfolio.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)];
assert(inlineScripts.length > 0, 'portfolio page must contain executable inline scripts');

inlineScripts.forEach((match, index) => {
  try {
    new vm.Script(match[1], { filename: `portfolio-inline-${index}.js` });
  } catch (err) {
    assert(false, `portfolio inline script ${index} must parse: ${err.message}`);
  }
});

if (!process.exitCode) console.log('portfolio restore regression checks passed');
