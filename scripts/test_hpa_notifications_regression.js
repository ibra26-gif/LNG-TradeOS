const fs = require('fs');
const path = require('path');
const vm = require('vm');

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
  app.includes("showSec('notifications',this)") &&
    app.includes('id="sec-notifications"') &&
    app.includes('id="hpa-notif-body"'),
  'Financial Trading must expose a Notifications section'
);

assert(
  /name=lngtradeos\.js&v=\d{8}-[a-z0-9-]+/.test(app),
  'private platform script must carry a dated cache-bust'
);

assert(
  js.includes("const HPA_NOTIF_KEY='hpa_notif_prefs_v1'") &&
    js.includes('const HPA_NOTIF_RULES=') &&
    js.includes('function hpaRenderNotifications') &&
    js.includes("if(id==='notifications')hpaRenderNotifications()"),
  'notification engine must have persisted prefs, rules, renderer, and tab hook'
);

assert(
  js.includes("label:'JKM-TTF M+1'") &&
    js.includes("label:'TTF-HH M+1'") &&
    js.includes("label:'JKM-HH M+1'") &&
    js.includes("label:'NBP-TTF M+1'") &&
    !js.includes("label:'ZTP-TTF M+1'"),
  'default spread monitors must include agreed spreads and keep ZTP/TTF custom-only'
);

assert(
  js.includes("label:'Slope M+1'") &&
    js.includes("label:'USGC Asia-Europe FOB M+1'") &&
    js.includes("label:'Oman Asia-Europe FOB M+1'") &&
    js.includes('Foundation Freight Curves · saved BLNG observations'),
  'notifications must cover slope, freight, and FOB snapshot monitors'
);

assert(
  js.includes('function hpaTrendAlert') &&
    js.includes('function hpaPersistenceFor') &&
    js.includes('function hpaContextFor') &&
    js.includes('function hpaQuestionFor') &&
    js.includes('trendMinShare') &&
    js.includes('Spread widening') &&
    js.includes('Spread closing') &&
    js.includes('Outright increasing') &&
    js.includes('Outright decreasing'),
  'notifications must include directional trend alerts for widening/closing and increasing/decreasing'
);

assert(
  js.includes('Market Watch · Relationship Shifts') &&
    js.includes('NEW TODAY') &&
    js.includes('Persistence') &&
    js.includes('Context') &&
    js.includes('Question') &&
    js.includes('window.hpaSetFilter') &&
    js.includes("'5D','20D','30D','60D','90D','1Y','ALL'"),
  'notification inbox must expose persistence, range context, questions, and filters'
);

const start = js.indexOf('function hpaDefaultPrefs');
const end = js.indexOf('function showSec', start);
assert(start >= 0 && end > start, 'notification helper block must be extractable');
if (start >= 0 && end > start) {
  const ctx = {
    HPA_NOTIF_RULES: [
      { id: 'spread-jkm-ttf', kind: 'spread', label: 'JKM-TTF M+1', unit: '$/MMBtu' },
    ],
    localStorage: { getItem: () => null, setItem: () => {} },
    window: {},
    Date,
    Number,
    Math,
    console,
  };
  vm.runInNewContext(
    js.slice(start, end) +
      '\nthis.__hpaTest={hpaDefaultPrefs,hpaStatsFor,hpaTrendAlert,hpaBuildAlert};',
    ctx
  );
  const rule = { id: 'spread-jkm-ttf', kind: 'spread', label: 'JKM-TTF M+1', unit: '$/MMBtu', changeThreshold: 0.10 };
  const base = new Date('2026-04-01T12:00:00');
  const series = Array.from({ length: 11 }, (_, i) => ({
    date: new Date(base.getTime() + i * 86400000),
    value: 0.5 + i * 0.04,
  }));
  const prefs = ctx.__hpaTest.hpaDefaultPrefs();
  const stats = ctx.__hpaTest.hpaStatsFor(rule, series, 'ALL');
  const trend = ctx.__hpaTest.hpaTrendAlert(rule, stats, prefs);
  const alert = ctx.__hpaTest.hpaBuildAlert(rule, stats, prefs);
  assert(trend && trend.type === 'Spread widening', 'trend helper must classify persistent spread widening');
  assert(alert && alert.title.includes('Spread widening'), 'trend breach must produce a widening alert');
  assert(alert && alert.persistence && alert.persistence.count >= 5, 'alert must report persistence across observations');
  assert(alert && alert.context && alert.context.short, 'alert must include normal-range context');
  assert(alert && /Asia premium/.test(alert.question), 'alert must include the implied market question');
}

if (!process.exitCode) console.log('HPA notifications regression checks passed');
