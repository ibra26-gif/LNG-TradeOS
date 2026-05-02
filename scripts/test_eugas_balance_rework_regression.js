const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const js = fs.readFileSync(path.join(root, 'api/private/platform-js.txt'), 'utf8');
const app = fs.readFileSync(path.join(root, 'api/private/platform-app.txt'), 'utf8');

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
  }
}

assert(
  app.includes('20260502-eugas-balance-rework') &&
    /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private bundle cache-bust must be bumped for the EU gas balance rework'
);

assert(
  js.includes('Historical monthly S&amp;D') &&
    js.includes('current month is MTD where available') &&
    js.includes('Data coverage') &&
    js.includes('Residual / unexplained'),
  'balance table must expose MTD/current-month context, coverage, and residual row'
);

assert(
  js.includes('function eubStrictSum') &&
    js.includes('const pipeTotal=eubStrictSum([norway,russian,central,african])') &&
    js.includes('const supply=eubStrictSum([production,pipeTotal,lng])') &&
    js.includes('EUB_PIPE_ROUTES.every(r => gdGetPipeMCM(r,mo)!=null)'),
  'balance aggregates must require all mapped components instead of summing partial data'
);

assert(
  js.includes('function eubDailyPipeTotal') &&
    js.includes('return eubStrictSum(routes.map(r=>eubDailyPipe(r,date)))') &&
    js.includes('return eubStrictSum([eubDailyPipeTotal(date), eubDailyLng(date), eubDailyDomTotal(date)])') &&
    js.includes('latest complete gas day'),
  'daily S&D chart must use complete gas days only'
);

assert(
  js.includes('Sector demand is not sourced yet') &&
    js.includes('With implied consumption the residual would be circular') &&
    !js.includes("row('Power demand") &&
    !js.includes("row('Industry demand") &&
    !js.includes("row('Residential / heating demand"),
  'sector demand placeholders must stay out until sourced, with residual explained'
);

assert(
  js.includes('_eubHistCacheLoading') &&
    js.includes('gdLoadPipelineHistoryFromCache()') &&
    js.includes('return {loading:true, years:[]}'),
  'historical pipeline cache must load before falling back to live year fetches'
);

if (!process.exitCode) console.log('EU gas balance rework regression checks passed');
