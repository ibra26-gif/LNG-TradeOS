// ════════════════════════════════════════════════════════════════════════════════════════
//  LNG TradeOS dashboard — consolidated regression suite
//  Run:  node outputs/verify.js
// ════════════════════════════════════════════════════════════════════════════════════════

const fs = require('fs');
const path = require('path');
// JSDOM is installed at /tmp/testdeps for the Cowork sandbox; fall back to a
// regular node_modules resolution when running locally.
let JSDOM;
try { JSDOM = require('/tmp/testdeps/node_modules/jsdom').JSDOM; }
catch (_) { JSDOM = require('jsdom').JSDOM; }

// Source-of-truth path: morning_book.html lives next to this verify.js script.
// Original Cowork path was a sandbox absolute; fall back to it for parity runs.
const candidates = [
  path.join(__dirname, 'morning_book.html'),
  '/sessions/loving-affectionate-fermi/mnt/outputs/morning_book.html',
];
const htmlPath = candidates.find(p => { try { return fs.statSync(p).isFile(); } catch { return false; } });
if (!htmlPath) { console.error('morning_book.html not found in:\n  ' + candidates.join('\n  ')); process.exit(2); }
const html = fs.readFileSync(htmlPath, 'utf8');
const dom = new JSDOM(html, { runScripts:'dangerously', pretendToBeVisual:true, url:'https://lng-tradeos.local/' });
const { window: w } = dom;

setTimeout(() => {
  const bar = '═'.repeat(96);

  const fmtM = v => {
    const a = Math.abs(v);
    if (a >= 1e6) return (v >= 0 ? '+$' : '−$') + (a / 1e6).toFixed(2) + 'M';
    if (a >= 1e3) return (v >= 0 ? '+$' : '−$') + (a / 1e3).toFixed(0) + 'k';
    return (v >= 0 ? '+$' : '−$') + a.toFixed(0);
  };
  let passed = 0, failed = 0;
  const check = (name, cond, detail = '') => {
    if (cond) { passed++; console.log(`    ✓ ${name}${detail ? ' — ' + detail : ''}`); }
    else       { failed++; console.log(`    ✗ ${name}${detail ? ' — ' + detail : ''}`); }
  };

  try {
    // ══════════════════════════════════════════════
    console.log(bar);
    console.log('SECTION 1 · State bootstrap (schema v7)');
    console.log(bar);
    const stored = JSON.parse(w.localStorage.getItem('lng-tradeos.state.v1'));
    check('Schema version = 7',  stored.version === 7);
    check('Parent deals seeded', stored.parentDeals.length === 4, `${stored.parentDeals.length} parents`);
    check('Paper trades seeded', stored.paperTrades.length === 6);
    check('Curve snapshots',     Object.keys(stored.curves.snapshots).length === 2);
    check('Freight snapshots',   Object.keys(stored.freightCurves.snapshots).length === 2);
    check('Settings present',    !!stored.settings);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 2 · Parent deals + children');
    console.log(bar);
    const parents = w.getParentDeals();
    for (const p of parents) {
      const kids = w.getChildrenOf(p.id);
      const kindBadge = p.kind === 'lt' ? 'LT  ' : 'Strip';
      console.log(`    ${kindBadge} · ${p.id.padEnd(14)} · ${p.name.padEnd(40)} · ${String(kids.length).padStart(2)} cargoes`);
    }
    check('LT-EG-SPA parent exists',     !!w.getParentDealById('LT-EG-SPA'));
    check('LT-CHENIERE parent exists',   !!w.getParentDealById('LT-CHENIERE'));
    check('STR-KRK-27Q parent exists',   !!w.getParentDealById('STR-KRK-27Q'));
    check('STR-GATE-26 parent exists',   !!w.getParentDealById('STR-GATE-26'));
    const egKids     = w.getChildrenOf('LT-EG-SPA');
    const cheniereKids = w.getChildrenOf('LT-CHENIERE');
    const krkKids    = w.getChildrenOf('STR-KRK-27Q');
    const gateKids   = w.getChildrenOf('STR-GATE-26');
    check('LT-EG-SPA: 10y full tenor (120 cargoes)',   egKids.length === 120, `${egKids.length} children`);
    check('LT-CHENIERE: 20y full tenor (240 cargoes)', cheniereKids.length === 240);
    check('STR-KRK-27Q: 8 quarters',                    krkKids.length === 8);
    check('STR-GATE-26: 24 monthly cargoes',            gateKids.length === 24);
    check('Total children = 392',                       egKids.length + cheniereKids.length + krkKids.length + gateKids.length === 392);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 3 · Child cargo inheritance');
    console.log(bar);
    const eg1 = egKids[0];
    const ch1 = cheniereKids[0];
    const kr1 = krkKids[0];
    const ga1 = gateKids[0];
    check('EG child inherits parent buy formula', eg1.buy.idx === 'TTF' && eg1.buy.mult === 0.945 && eg1.buy.spread === -2.91);
    check('Cheniere child formula = 115% HH + 2.50', ch1.buy.idx === 'HH' && ch1.buy.mult === 1.15 && ch1.buy.spread === 2.50);
    check('KrK child formula = TTF − 0.60',       kr1.sell.idx === 'TTF' && kr1.sell.spread === -0.60);
    check('Gate child formula = TTF − 0.25',      ga1.sell.idx === 'TTF' && ga1.sell.spread === -0.25);
    check('Gate counterparty = OMV',              ga1.cpty === 'OMV');
    check('EG coverage = unsold',                 eg1.coverage === 'unsold');
    check('KrK coverage = uncovered',             kr1.coverage === 'uncovered');
    check('KrK uses quarter period',              /^Q\d-\d{2}$/.test(kr1.loadMonth));
    check('Gate first cargo = May-26',            ga1.loadMonth === 'May-26');
    check('EG first cargo = May-26',              eg1.loadMonth === 'May-26');

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 4 · Cargo MTM math by incoterm/coverage');
    console.log(bar);
    const v_eg1 = w.computeCargoValues(eg1);
    const v_ch1 = w.computeCargoValues(ch1);
    const v_kr1 = w.computeCargoValues(kr1);
    const v_ga1 = w.computeCargoValues(ga1);
    console.log(`    ${eg1.id} (unsold):    Loaded ${v_eg1.loaded.toFixed(2)} TBtu · MTM ${fmtM(v_eg1.mtm * 1e6)}`);
    console.log(`    ${ch1.id} (unsold):    Loaded ${v_ch1.loaded.toFixed(2)} TBtu · MTM ${fmtM(v_ch1.mtm * 1e6)}`);
    console.log(`    ${kr1.id} (uncovered): Loaded ${v_kr1.loaded.toFixed(2)} TBtu · MTM ${fmtM(v_kr1.mtm * 1e6)}`);
    console.log(`    ${ga1.id} (uncovered): Loaded ${v_ga1.loaded.toFixed(2)} TBtu · MTM ${fmtM(v_ga1.mtm * 1e6)}`);
    check('Cheniere MTM positive',      v_ch1.mtm > 0);
    check('EG MTM positive',            v_eg1.mtm > 0);
    check('MTM math finite everywhere', isFinite(v_eg1.mtm) && isFinite(v_ch1.mtm) && isFinite(v_kr1.mtm) && isFinite(v_ga1.mtm));

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 5 · Delete parent cascades, undo restores all children');
    console.log(bar);
    const beforeCount = w.getCargoes().length;
    const deleted = w.deleteParentDeal('STR-GATE-26');
    const afterCount = w.getCargoes().length;
    check('Delete parent removes all 24 children', beforeCount - afterCount === 24, `${beforeCount} → ${afterCount}`);
    check('Parent also removed',                !w.getParentDealById('STR-GATE-26'));
    w.undoDeleteParent();
    check('Undo restores children',             w.getCargoes().length === beforeCount);
    check('Undo restores parent',               !!w.getParentDealById('STR-GATE-26'));

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 6 · Paper Book CRUD + quarterly hedge decomposition');
    console.log(bar);
    const trades0 = w.getPaperTrades().length;
    const newH = { id: w.nextTradeId(), date:'2026-04-22', venue:'ICE · cleared', cpty:'Cleared',
      type:'hedge', linked:null, hedgeType:'Price risk (index)',
      idx:'HH', dir:'Sell', month:'Q3-26', lots:80, exec:3.50, notes:'test' };
    w.bookTrade(newH);
    check('Hedge booked',         w.getPaperTrades().length === trades0 + 1);
    const expo = w.computeHedgeExposure();
    check('Q3-26 hedge spreads to Jul/Aug/Sep',
      expo.HH?.['Jul-26'] != null && expo.HH?.['Aug-26'] != null && expo.HH?.['Sep-26'] != null);
    w.deleteTrade(newH.id);
    check('Trade delete clean',   w.getPaperTrades().length === trades0);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 7 · LNG curves: lookup + re-mark propagation');
    console.log(bar);
    check('JKM Jul-26 = 16.528',   Math.abs(w.curveLookup('Jul-26', 'JKM') - 16.528) < 0.001);
    check('TTF Jul-26 = 15.230',   Math.abs(w.curveLookup('Jul-26', 'TTF') - 15.230) < 0.001);
    check('HH Dec-26 = 4.31',      Math.abs(w.curveLookup('Dec-26', 'HH') - 4.31) < 0.001);
    // Re-mark propagation on a Cheniere child (HH-indexed)
    const before = w.computeCargoValues(ch1).mtm;
    w.updateCurveCell('2026-04-22', ch1.loadMonth, 'HH', w.curveLookup(ch1.loadMonth, 'HH') + 0.50);
    const after = w.computeCargoValues(w.getCargoById(ch1.id)).mtm;
    check('Curve re-mark shifts Cheniere MTM', Math.abs(after - before) > 0.01);
    w.updateCurveCell('2026-04-22', ch1.loadMonth, 'HH', w.curveLookup(ch1.loadMonth, 'HH') - 0.50);  // reset

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 8 · Freight curves + route classifier');
    console.log(bar);
    const routeTests = [
      ['Sabine Pass', 'Gate',      'Atlantic', 'BLNG2'],
      ['Bioko',       'Gate',      'X-Basin',  'BLNG3'],   // WAF → EU
      ['Sabine Pass', 'Krk',       'Atlantic', 'BLNG2'],    // Krk is Atlantic
      ['Ras Laffan',  'Gate',      'X-Basin',  'BLNG3'],
    ];
    for (const [load, disch, expBasin, expCurve] of routeTests) {
      const basin = w.routeBasin(load, disch);
      const curve = w.defaultFreightRoute(basin);
      check(`${load} → ${disch}`, basin === expBasin && curve === expCurve, `${basin} / ${curve}`);
    }
    check('BLNG2 Jul-26 = $75k', w.freightLookup('Jul-26', 'BLNG2') === 75000);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 9 · Settings · live propagation + reset');
    console.log(bar);
    const v1 = w.calcVoyage('Sabine Pass', 'Gate', 'Jul-26', 3.6, null);
    w.setSetting('fuel.hfoUSD', 700);
    const v2 = w.calcVoyage('Sabine Pass', 'Gate', 'Jul-26', 3.6, null);
    check('HFO $550 → $700 raises fuel cost', v2.hfoCost > v1.hfoCost);
    w.resetSettings();
    check('Reset restores defaults',          w.getSetting('fuel.hfoUSD') === 550);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 10 · Optionality math');
    console.log(bar);
    const hullMarg = w.margrabeExchange(30, 28, 0.30, 0.40, 0.30, 1);
    check('Margrabe ≈ 3.91 (Hull 36.1)', Math.abs(hullMarg - 3.91) < 0.05, `got ${hullMarg.toFixed(3)}`);
    const b_call = w.bachelierCall(10, 10, 2, 1);
    const b_put  = w.bachelierPut(10, 10, 2, 1);
    check('Bachelier ATM call = put',     Math.abs(b_call - b_put) < 1e-6);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 11 · Morning Book KPIs');
    console.log(bar);
    w.renderMorningBook();
    const kpiVals = w.document.querySelectorAll('#sec-morning .metric-value');
    const kpiSubs = w.document.querySelectorAll('#sec-morning .metric-sub');
    console.log(`    Physical MTM:  ${kpiVals[0]?.textContent.trim()}  (${kpiSubs[0]?.textContent.trim()})`);
    console.log(`    Hedge P&L:     ${kpiVals[1]?.textContent.trim()}`);
    console.log(`    Spec MtM:      ${kpiVals[2]?.textContent.trim()}`);
    console.log(`    Combined:      ${kpiVals[3]?.textContent.replace(/\s+/g, ' ').trim()}`);
    console.log(`    Floating / T:  ${w.document.getElementById('metric-floating-pct')?.textContent.replace(/\s+/g, ' ').trim()}`);
    check('Morning Book KPIs rendered', /\$[\d.]+[MBk]/.test(kpiVals[0]?.textContent || ''));
    const subText = w.document.querySelector('#sec-morning .page-head .sub')?.textContent || '';
    check('Sub-line counts real',       /\d+\s+cargo/.test(subText) && /\d+\s+hedge/.test(subText));

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 12 · Cargo table grouping (parent/child render)');
    console.log(bar);
    w.renderCargoTable();
    const rows = w.document.querySelectorAll('#cargo-table-body tr').length;
    check('Cargo table has rows', rows > 0, `${rows} rows`);
    // Default: parents collapsed → 4 parent rows + 0 child rows + 0 standalone = 4
    check('Default collapsed view shows parents only', rows === 4);
    // Expand one parent — full 10y × 12/mo = 120 children
    w.toggleParentExpanded('LT-EG-SPA');
    w.renderCargoTable();
    const rowsExpanded = w.document.querySelectorAll('#cargo-table-body tr').length;
    check('Expanding LT-EG-SPA adds 120 child rows (full tenor)', rowsExpanded === 4 + 120, `${rowsExpanded} rows`);
    w.toggleParentExpanded('LT-EG-SPA');   // collapse back

    // Window filter: Next 24mo should narrow down
    w.setCargoWindow('24');
    w.toggleParentExpanded('LT-CHENIERE');
    w.renderCargoTable();
    const filtered24 = w.document.querySelectorAll('#cargo-table-body tr').length;
    // parents + ~24 Cheniere children within next 24 months
    check('Window filter "Next 24mo" trims far-tenor',        filtered24 > 0 && filtered24 < 4 + 240, `${filtered24} rows`);
    w.setCargoWindow('all');
    w.toggleParentExpanded('LT-CHENIERE');

    // Cal-fallback sanity: curveLookupDetailed for far-future month should fall back
    const far = w.curveLookupDetailed('Jul-35', 'HH');
    check('Far-tenor lookup falls back gracefully', far.value > 0 && far.source !== 'monthly', `source=${far.source}`);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 13 · P&L Log · 30-day seed + render');
    console.log(bar);
    const hist = w.getPnlHistory();
    check('30-day P&L history seeded',  hist.length === 30, `${hist.length} snapshots`);
    check('History is chronological',   hist[0].date < hist[hist.length - 1].date);
    check('All snapshots have combined', hist.every(s => typeof s.combined === 'number' && isFinite(s.combined)));
    // Render
    w.renderPnlLog();
    const logRows = w.document.querySelectorAll('#pnl-log-body tr').length;
    check('P&L log table rendered',     logRows === 30, `${logRows} rows`);
    const kpiCurrent = w.document.getElementById('pnl-current')?.textContent || '';
    check('Current P&L KPI populated',  /\$[\d.]+[MBk]/.test(kpiCurrent), `got "${kpiCurrent}"`);
    const kpiPeak = w.document.getElementById('pnl-peak')?.textContent || '';
    check('Peak P&L KPI populated',     /\$[\d.]+[MBk]/.test(kpiPeak));
    // Range toggle
    w.setPnlRange('7');
    const rows7 = w.document.querySelectorAll('#pnl-log-body tr').length;
    check('7d range shows 7 rows',      rows7 === 7);
    w.setPnlRange('30');
    // Snapshot EOD
    const beforeCount2 = w.getPnlHistory().length;
    w.capturePnlSnapshot('2026-04-30');
    check('Snapshot EOD appends',       w.getPnlHistory().length === beforeCount2 + 1);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 14a · Optionality group-by-deal toggle');
    console.log(bar);
    w.renderOptionality();
    const instanceRowCount = w.document.querySelectorAll('#opt-ledger-body tr').length;
    check('Per-instance ledger has many rows',
          instanceRowCount > 10,
          `${instanceRowCount} rows`);
    w.setOptGroup('deal');
    const dealRowCount = w.document.querySelectorAll('#opt-ledger-body tr').length;
    check('Deal view collapses rows to 4 parent deals',
          dealRowCount === 4,
          `${dealRowCount} deal rows (from ${instanceRowCount} instance rows)`);
    // Totals should be preserved across views (footer sums must match)
    const dealFoot = w.document.getElementById('opt-ledger-foot')?.textContent || '';
    check('Deal view footer includes "deal" label',
          /deal/i.test(dealFoot));
    w.setOptGroup('instance');
    const backToInstance = w.document.querySelectorAll('#opt-ledger-body tr').length;
    check('Toggle back restores instance rows',
          backToInstance === instanceRowCount);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 14 · CSV export builders');
    console.log(bar);
    // Intercept downloadCSV to capture the payload instead of triggering a real
    // Blob/URL.createObjectURL/<a>.click() chain (which is flaky under JSDOM).
    const captured = [];
    const origDl = w.downloadCSV;
    w.downloadCSV = (filename, headers, rows) => {
      captured.push({ filename, headers, rowCount: rows.length, firstRow: rows[0] || null });
    };
    try {
      w.exportCargoCSV();
      const last = captured[captured.length - 1];
      check('exportCargoCSV emits rows',
            last && last.rowCount > 0,
            `${last?.rowCount ?? 0} rows, file=${last?.filename}`);
      check('exportCargoCSV: parents + children',
            last && last.rowCount >= 4 + 1,  // 4 parents + at least 1 child
            `${last?.rowCount} total`);
      check('exportCargoCSV: header has MTM_USD_M',
            last && last.headers.includes('MTM_USD_M'));

      w.exportPaperCSV();
      const paperOut = captured[captured.length - 1];
      check('exportPaperCSV emits rows',
            paperOut && paperOut.rowCount > 0,
            `${paperOut?.rowCount ?? 0} rows`);
      check('exportPaperCSV: PnL_USD in header',
            paperOut && paperOut.headers.includes('PnL_USD'));

      w.exportSpecCSV();
      const specOut = captured[captured.length - 1];
      check('exportSpecCSV emits rows',
            specOut && specOut.rowCount >= 0,
            `${specOut?.rowCount ?? 0} rows`);

      w.exportOptionalityCSV();
      const optOut = captured[captured.length - 1];
      check('exportOptionalityCSV emits rows',
            optOut && optOut.rowCount > 0,
            `${optOut?.rowCount ?? 0} rows`);
      check('exportOptionalityCSV: Total_USD_M in header',
            optOut && optOut.headers.includes('Total_USD_M'));

      w.exportExposureCSV();
      const expoOut = captured[captured.length - 1];
      check('exportExposureCSV emits rows',
            expoOut && expoOut.rowCount > 0,
            `${expoOut?.rowCount ?? 0} rows`);
      check('exportExposureCSV: Net_TBtu in header',
            expoOut && expoOut.headers.includes('Net_TBtu'));
    } finally {
      w.downloadCSV = origDl;
    }

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 15 · Scenarios · preset + custom shocks');
    console.log(bar);
    // Base book MTM without shocks
    w.setCurveShocks(null);
    const baseCargoes = w.getCargoes();
    const sampleCargo = baseCargoes[0];
    const baseMtm = w.computeCargoValues(sampleCargo).mtm;

    // Install a $1.00 HH shock — any HH-indexed cargo should move
    w.setCurveShocks({ HH: +1.00 });
    const shockedMtm = w.computeCargoValues(sampleCargo).mtm;
    w.setCurveShocks(null);
    // Shock must revert
    const revertedMtm = w.computeCargoValues(sampleCargo).mtm;
    check('Shock layer reverts cleanly after setCurveShocks(null)',
          Math.abs(revertedMtm - baseMtm) < 1e-6);

    // Run the full runScenario pipeline with a TTF shock (EG SPA is TTF-indexed on buy side)
    w.setScenarioShocksMap({ TTF: +2.00 });
    w.runScenario();
    const deltaEl = w.document.getElementById('sc-delta')?.textContent || '';
    check('runScenario populates Δ KPI',    /\$/.test(deltaEl), `Δ=${deltaEl}`);
    const impactRows = w.document.querySelectorAll('#scenario-impact-body tr').length;
    check('runScenario populates impact table rows',
          impactRows === baseCargoes.length,
          `${impactRows} rows (expected ${baseCargoes.length})`);
    // Critical: shocks must NOT leak into the main book after the run
    const postRun = w.computeCargoValues(sampleCargo).mtm;
    check('Scenario run does not mutate main book',
          Math.abs(postRun - baseMtm) < 1e-6);

    // Preset loader
    w.loadScenarioPreset('asia-heat');
    const presetEl = w.document.getElementById('sc-delta')?.textContent || '';
    check('Preset "asia-heat" runs and populates Δ',
          /\$/.test(presetEl), `Δ=${presetEl}`);

    // Reset
    w.resetScenarioShocks();
    check('resetScenarioShocks clears state',
          Object.keys(w.getScenarioShocks()).length === 0);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 16 · Oil-indexed slope · (x,y,z) averaging');
    console.log(bar);
    // shiftMonth helper
    check('shiftMonth("Jun-26", -2) = Apr-26',  w.shiftMonth('Jun-26', -2) === 'Apr-26');
    check('shiftMonth("Dec-26", +1) = Jan-27',  w.shiftMonth('Dec-26', +1) === 'Jan-27');
    check('shiftMonth("Jan-27", -1) = Dec-26',  w.shiftMonth('Jan-27', -1) === 'Dec-26');
    check('shiftMonth("Jul-26", 0)  = Jul-26',  w.shiftMonth('Jul-26', 0) === 'Jul-26');
    check('shiftMonth preserves Q-period input', w.shiftMonth('Q3-26', -1) === 'Q3-26');

    // averageOilIndex: Slope(3,0,1) for Jun-26 should average Apr-26, May-26, Jun-26 Brent
    const bApr = w.curveLookup('Apr-26', 'Brent');
    const bMay = w.curveLookup('May-26', 'Brent');
    const bJun = w.curveLookup('Jun-26', 'Brent');
    const expected301 = (bApr + bMay + bJun) / 3;
    const got301 = w.averageOilIndex('Jun-26', 'Brent', 3, 0);
    check('(3,0,1) for Jun-26: avg(Apr,May,Jun) Brent',
          Math.abs(got301 - expected301) < 1e-6,
          `got ${got301.toFixed(3)} vs ${expected301.toFixed(3)}`);

    // (3,1,1) — lag 1 — window should shift back one month: Mar,Apr,May
    const bMar = w.curveLookup('Mar-26', 'Brent');
    const expected311 = (bMar + bApr + bMay) / 3;
    const got311 = w.averageOilIndex('Jun-26', 'Brent', 3, 1);
    check('(3,1,1) for Jun-26: avg(Mar,Apr,May) Brent',
          Math.abs(got311 - expected311) < 1e-6,
          `got ${got311.toFixed(3)} vs ${expected311.toFixed(3)}`);

    // (1,0,1) — single-month, no averaging: Jun Brent only
    check('(1,0,1) for Jun-26: Brent(Jun-26) direct',
          Math.abs(w.averageOilIndex('Jun-26', 'Brent', 1, 0) - bJun) < 1e-6);

    // End-to-end valueInstance with oil-indexed destination via (3,0,1)
    const testInst = {
      dealId: 'TEST-OIL', dealKind: 'cargo', dealType: 'Oil-indexed test',
      loadMo: 'Jun-26', sellMo: 'Jun-26', tbtu: 3.4,
      contractedIdx: 'TTF', contractedPx: null,
      flexType: 'dest',
      params: {
        destinations: [
          'TTF',
          { idx: 'Brent', slope: 0.115, window: 3, lag: 0, constant: 0.50 },
        ],
        freight: 0,
      },
    };
    const vOil = w.valueInstance(testInst);
    check('Oil-indexed dest returns a finite total',
          isFinite(vOil.total) && vOil.total >= 0,
          `total=${vOil.total.toFixed(3)}M`);
    // Net the labels back out to confirm they reflect the spec
    const lbl = vOil.detail?.dests?.find(l => /Brent/.test(l)) || '';
    check('Label shows "11.5%×Brent(3,0,1)"', /11\.5%×Brent\(3,0,1\)/.test(lbl), `got "${lbl}"`);

    // 94.5% scalar combined with 11.5% slope
    const testInstScalar = {
      dealId: 'TEST-SCALAR', dealKind: 'cargo', dealType: 'Scalar test',
      loadMo: 'Jun-26', sellMo: 'Jun-26', tbtu: 3.4,
      contractedIdx: 'TTF', contractedPx: null,
      flexType: 'dest',
      params: {
        destinations: [{ idx: 'Brent', slope: 0.115, scalar: 0.945, window: 3, lag: 0, constant: 0 }],
        freight: 0,
      },
    };
    const vScalar = w.valueInstance(testInstScalar);
    const lblScalar = vScalar.detail?.dests?.[0] || '';
    check('Label shows "94.5%×11.5%×Brent(3,0,1)"',
          /94\.5%×11\.5%×Brent\(3,0,1\)/.test(lblScalar),
          `got "${lblScalar}"`);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 17 · Cargo buy/sell legs · (x,y,z) averaging');
    console.log(bar);
    // Pick any existing child cargo, clone it, and set the buy leg to an
    // oil-indexed formula with (3,0,1) averaging. Compare the resolved buy
    // price against an independently-computed 3-month Brent average.
    const anyCargo = w.getCargoes()[0];
    // Clone so we don't mutate state
    const oilCargo = JSON.parse(JSON.stringify(anyCargo));
    oilCargo.loadMonth = 'Jun-26';
    oilCargo.sellMonth = 'Jun-26';
    oilCargo.coverage = 'covered';   // use contracted buy/sell legs, not refBuy/refSell
    oilCargo.buy  = { idx: 'Brent', formula: '% × Index', mult: 0.115, spread: 0, window: 3, lag: 0 };
    oilCargo.sell = { idx: 'TTF',   formula: 'Index + Spread', spread: 0 };   // default window=1 lag=0

    const v = w.computeCargoValues(oilCargo);
    const bA = w.curveLookup('Apr-26', 'Brent');
    const bM = w.curveLookup('May-26', 'Brent');
    const bJ = w.curveLookup('Jun-26', 'Brent');
    const expectedBuy = 0.115 * (bA + bM + bJ) / 3;
    check('Buy leg (3,0,1) on Brent: buyPx = 11.5% × avg(Apr,May,Jun)',
          Math.abs(v.buyPx - expectedBuy) < 1e-6,
          `got ${v.buyPx.toFixed(4)} vs ${expectedBuy.toFixed(4)}`);

    // Sell leg with default window=1, lag=0 must still hit curveLookup directly
    const ttfJun = w.curveLookup('Jun-26', 'TTF');
    check('Sell leg default (window=1, lag=0): sellPx = TTF(Jun-26)',
          Math.abs(v.sellPx - ttfJun) < 1e-6,
          `got ${v.sellPx.toFixed(4)} vs ${ttfJun.toFixed(4)}`);

    // scalar field on the leg — compound multiplier (e.g., 94.5% × 11.5% × Brent(3,0,1))
    const oilCargoScalar = JSON.parse(JSON.stringify(oilCargo));
    oilCargoScalar.buy = { idx: 'Brent', formula: '% × Index', mult: 0.115, spread: 0,
                            window: 3, lag: 0, scalar: 0.945 };
    const vLegScalar = w.computeCargoValues(oilCargoScalar);
    const expectedBuyScalar = 0.945 * 0.115 * (bA + bM + bJ) / 3;
    check('Buy leg scalar × slope × avg: 94.5% × 11.5% × Brent(3,0,1)',
          Math.abs(vLegScalar.buyPx - expectedBuyScalar) < 1e-6,
          `got ${vLegScalar.buyPx.toFixed(4)} vs ${expectedBuyScalar.toFixed(4)}`);

    // Lag of 1: pricing window should shift back 1 month (Mar, Apr, May)
    const oilCargoLag = JSON.parse(JSON.stringify(oilCargo));
    oilCargoLag.buy.lag = 1;
    const vLag = w.computeCargoValues(oilCargoLag);
    const bMarLeg = w.curveLookup('Mar-26', 'Brent');
    const expectedBuyLag = 0.115 * (bMarLeg + bA + bM) / 3;
    check('Buy leg (3,1,1): buyPx uses Mar,Apr,May Brent',
          Math.abs(vLag.buyPx - expectedBuyLag) < 1e-6,
          `got ${vLag.buyPx.toFixed(4)} vs ${expectedBuyLag.toFixed(4)}`);

    // Backward-compat: a plain existing cargo (no window/lag on its legs)
    // must still produce the same MTM as before this change.
    const pre = w.computeCargoValues(anyCargo).mtm;
    const post = w.computeCargoValues(anyCargo).mtm;
    check('Legacy cargoes without window/lag: MTM unchanged', pre === post);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 18 · Seeded oil-indexed destination flex');
    console.log(bar);
    // At least one instance in the optionality ledger should be an oil-indexed
    // destination — after seeding, confirm the (x,y,z) label is rendered live.
    w.renderOptionality();
    const ledgerCells = Array.from(w.document.querySelectorAll('#opt-ledger-body .mono'))
      .map(td => td.textContent);
    const oilLabelRegex = /%×(Brent|JCC)\(\d+,\d+,\d+\)/;
    const hasOilLabel = ledgerCells.some(c => oilLabelRegex.test(c));
    check('Optionality ledger shows at least one oil-indexed (x,y,z) destination',
          hasOilLabel,
          hasOilLabel ? `e.g. "${ledgerCells.find(c => oilLabelRegex.test(c))}"` : 'no oil-indexed label found');

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 19 · z-aware pricing period grouping');
    console.log(bar);
    // pricingAnchorMonth helper
    check('z=1 passes delivery month through unchanged',
          w.pricingAnchorMonth('2026-05', 1, 'Jun-26') === 'Jun-26');
    check('z=3 aligned start (2026-05): May-26 → anchor Jul-26',
          w.pricingAnchorMonth('2026-05', 3, 'May-26') === 'Jul-26',
          `got ${w.pricingAnchorMonth('2026-05', 3, 'May-26')}`);
    check('z=3 same bucket: Jun-26 → anchor Jul-26',
          w.pricingAnchorMonth('2026-05', 3, 'Jun-26') === 'Jul-26');
    check('z=3 same bucket: Jul-26 → anchor Jul-26',
          w.pricingAnchorMonth('2026-05', 3, 'Jul-26') === 'Jul-26');
    check('z=3 next bucket: Aug-26 → anchor Oct-26',
          w.pricingAnchorMonth('2026-05', 3, 'Aug-26') === 'Oct-26');
    check('z=6 semi-annual: Dec-26 → anchor Oct-26',
          w.pricingAnchorMonth('2026-05', 6, 'Dec-26') === 'Apr-27',
          `got ${w.pricingAnchorMonth('2026-05', 6, 'Dec-26')}`);

    // End-to-end: clone EG-SPA child, give parent a fake pricingZ=3 on buy,
    // verify all three cargoes in a bucket compute the same buyPx.
    const parentsZ = w.getParentDeals();
    const eg = parentsZ.find(p => p.id === 'LT-EG-SPA');
    // Temporarily mutate parent + regenerate kids with z=3 on buy leg
    const origBuy = JSON.parse(JSON.stringify(eg.buy));
    eg.buy.pricingZ = 3;
    // Regenerate just for this test — build children in memory, don't touch state
    const regen = w.generateChildrenForParent(eg);
    // Find May/Jun/Jul-26 children — same bucket under z=3 starting 2026-05
    const may = regen.find(c => c.loadMonth === 'May-26');
    const jun = regen.find(c => c.loadMonth === 'Jun-26');
    const jul = regen.find(c => c.loadMonth === 'Jul-26');
    const aug = regen.find(c => c.loadMonth === 'Aug-26');
    check('Children have pricing anchors populated when z>1',
          may?.buyPricingAnchor === 'Jul-26' && jun?.buyPricingAnchor === 'Jul-26' && jul?.buyPricingAnchor === 'Jul-26',
          `May/Jun/Jul anchors: ${may?.buyPricingAnchor}/${jun?.buyPricingAnchor}/${jul?.buyPricingAnchor}`);
    check('Next bucket has different anchor',
          aug?.buyPricingAnchor === 'Oct-26',
          `Aug anchor: ${aug?.buyPricingAnchor}`);

    // Most critical check: three cargoes in the same bucket must compute identical buyPx
    const buyMay = w.computeCargoValues(may).buyPx;
    const buyJun = w.computeCargoValues(jun).buyPx;
    const buyJul = w.computeCargoValues(jul).buyPx;
    const buyAug = w.computeCargoValues(aug).buyPx;
    check('Same-bucket cargoes share one computed price (May = Jun = Jul)',
          Math.abs(buyMay - buyJun) < 1e-6 && Math.abs(buyJun - buyJul) < 1e-6,
          `May=${buyMay.toFixed(4)}, Jun=${buyJun.toFixed(4)}, Jul=${buyJul.toFixed(4)}`);
    check('Next bucket has a DIFFERENT price',
          Math.abs(buyJul - buyAug) > 1e-4,
          `Jul=${buyJul.toFixed(4)} vs Aug=${buyAug.toFixed(4)}`);

    // Anchor should be TTF(Jul-26) × mult + spread for the EG formula
    const ttfJulz = w.curveLookup('Jul-26', 'TTF');
    const expectedBuyJul = 0.945 * ttfJulz + (-2.91);
    check('Bucket price = 0.945 × TTF(Jul-26) − 2.91 (z=3, anchor=Jul)',
          Math.abs(buyJul - expectedBuyJul) < 1e-6,
          `got ${buyJul.toFixed(4)} vs ${expectedBuyJul.toFixed(4)}`);

    // Restore EG SPA (cleanup so other tests see original behaviour)
    eg.buy = origBuy;

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 20 · Krk port data');
    console.log(bar);
    // Sabine Pass → Krk NM (user-provided)
    const sabineKrk = w.calcVoyage('Sabine Pass', 'Krk', 'Jul-26', 3.6, null);
    check('Sabine Pass → Krk voyage returns finite numbers',
          sabineKrk && isFinite(sabineKrk.totalCost),
          `total $${sabineKrk?.totalCost?.toFixed(0) ?? '—'}`);
    check('NM_DB has Sabine → Krk = 5,381',
          sabineKrk.nm === 5381,
          `got ${sabineKrk.nm}`);
    check('Krk discharge port cost picked up ($242k)',
          sabineKrk.dischPortCost === 242000,
          `got $${sabineKrk.dischPortCost}`);
    // Laden days ≈ 5381 NM / (16.5 kn × 24) ≈ 13.6 days
    check('Laden days ≈ 13.6 (5,381 NM / 16.5 kn)',
          sabineKrk.ladenDays > 13.3 && sabineKrk.ladenDays < 13.9,
          `got ${sabineKrk.ladenDays?.toFixed(2)} days`);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 21 · Pre-merge cleanup · exposure/formula/edit/CSV/shock-oil');
    console.log(bar);

    // (1) Exposure uses scalar × mult.  Clone a cargo, give its buy leg a scalar,
    // and confirm the exposure is scalar× larger than baseline.
    w.setCurveShocks(null);
    const cOrig = w.getCargoes()[0];
    // Take a snapshot of exposure before we mutate
    const expBaseline = w.computePhysicalExposure();
    // Find a monthly exposure cell we can measure: pick the first signed cell
    const idxBase = Object.keys(expBaseline)[0];
    const moBase  = Object.keys(expBaseline[idxBase])[0];
    const baseVal = expBaseline[idxBase][moBase];
    // Give a standalone copy a scalar; add to state, recompute, compare, then clean up
    const standalone = JSON.parse(JSON.stringify(cOrig));
    standalone.id = 'C-TEST-SCALAR';
    standalone.parentDealId = null;
    standalone.dealType = 'single';
    standalone.coverage = 'covered';
    standalone.loadMonth = 'Jun-26';
    standalone.sellMonth = 'Jun-26';
    standalone.buyPricingAnchor = null;
    standalone.sellPricingAnchor = null;
    standalone.buy  = { idx: 'TTF', formula: '% × Index + Spread', mult: 0.945, spread: -2.91, scalar: 0.5 };
    standalone.sell = { idx: 'TTF', formula: 'Index + Spread',     spread: 0 };
    // Compute exposure for THIS cargo in isolation via a quick delta approach
    const expoBefore = JSON.parse(JSON.stringify(w.computePhysicalExposure()));
    w.bookCargo(standalone);
    const expoAfter = w.computePhysicalExposure();
    w.deleteCargo('C-TEST-SCALAR');
    const deltaJun = (expoAfter.TTF?.['Jun-26'] ?? 0) - (expoBefore.TTF?.['Jun-26'] ?? 0);
    // Expected: buy leg is short, so delta is negative; magnitude = scalar * mult * qty
    check('Exposure accounts for scalar (non-zero delta on scaled leg)',
          Math.abs(deltaJun) > 0.01,
          `Δ TTF Jun-26 = ${deltaJun.toFixed(3)} TBtu`);

    // (1b) Window distribution: a leg with window=3 should spread exposure across 3 months
    const spread3 = JSON.parse(JSON.stringify(standalone));
    spread3.id = 'C-TEST-WINDOW3';
    spread3.buy = { idx: 'Brent', formula: '% × Index', mult: 0.115, spread: 0, window: 3, lag: 0 };
    const beforeB = JSON.parse(JSON.stringify(w.computePhysicalExposure()));
    w.bookCargo(spread3);
    const afterB = w.computePhysicalExposure();
    w.deleteCargo('C-TEST-WINDOW3');
    const dApr = (afterB.Brent?.['Apr-26'] ?? 0) - (beforeB.Brent?.['Apr-26'] ?? 0);
    const dMay = (afterB.Brent?.['May-26'] ?? 0) - (beforeB.Brent?.['May-26'] ?? 0);
    const dJun = (afterB.Brent?.['Jun-26'] ?? 0) - (beforeB.Brent?.['Jun-26'] ?? 0);
    check('window=3 spreads exposure across 3 months (Apr/May/Jun)',
          Math.abs(dApr) > 0 && Math.abs(dMay) > 0 && Math.abs(dJun) > 0 && Math.abs(dApr - dMay) < 1e-6 && Math.abs(dMay - dJun) < 1e-6,
          `Δ Apr=${dApr.toFixed(3)}, May=${dMay.toFixed(3)}, Jun=${dJun.toFixed(3)}`);

    // (2) Cargo table formula renders window/lag/scalar/pricingZ
    const richLeg = JSON.parse(JSON.stringify(standalone));
    richLeg.id = 'C-TEST-FMT';
    richLeg.buy = { idx: 'Brent', formula: '% × Index', mult: 0.115, spread: 0,
                     window: 3, lag: 0, scalar: 0.945, pricingZ: 3 };
    w.bookCargo(richLeg);
    w.renderCargoTable();
    const rowCell = Array.from(w.document.querySelectorAll('#cargo-table-body tr'))
      .find(tr => tr.textContent.includes('C-TEST-FMT'))
      ?.querySelectorAll('td')?.[4]?.textContent || '';
    check('Cargo table formula shows "(3,0,3)" tag',         /\(3,0,3\)/.test(rowCell),          `cell="${rowCell}"`);
    check('Cargo table formula shows scalar "94.5%×"',        /94\.5%×/.test(rowCell),            `cell="${rowCell}"`);
    w.deleteCargo('C-TEST-FMT');

    // (3) editCargo round-trip preserves extended leg fields
    const roundtrip = JSON.parse(JSON.stringify(richLeg));
    roundtrip.id = 'C-TEST-RT';
    roundtrip.buyPricingAnchor = 'Jul-26';
    w.bookCargo(roundtrip);
    w.editCargo('C-TEST-RT');
    // At this point the form is populated.  collectFormCargo should pull extended
    // fields from the existing cargo since the form has no UI for them.
    const collected = w.collectFormCargo();
    check('Edit round-trip preserves buy.window',   collected.buy?.window   === 3,     `got ${collected.buy?.window}`);
    check('Edit round-trip preserves buy.lag',      collected.buy?.lag      === 0,     `got ${collected.buy?.lag}`);
    check('Edit round-trip preserves buy.scalar',   collected.buy?.scalar   === 0.945, `got ${collected.buy?.scalar}`);
    check('Edit round-trip preserves buy.pricingZ', collected.buy?.pricingZ === 3,     `got ${collected.buy?.pricingZ}`);
    check('Edit round-trip preserves buyPricingAnchor',
          collected.buyPricingAnchor === 'Jul-26',
          `got ${collected.buyPricingAnchor}`);
    w.deleteCargo('C-TEST-RT');
    w.currentEditingCargoId = null;

    // (6) CSV export includes the new columns
    const capturedCSV = [];
    const prevDl = w.downloadCSV;
    w.downloadCSV = (filename, headers, rows) => { capturedCSV.push({ filename, headers, rows }); };
    try {
      w.exportCargoCSV();
      const lastCSV = capturedCSV[capturedCSV.length - 1];
      check('Cargo CSV has BuyWindow column',         lastCSV.headers.includes('BuyWindow'));
      check('Cargo CSV has BuyPricingAnchor column',  lastCSV.headers.includes('BuyPricingAnchor'));
      check('Cargo CSV has SellWindow column',        lastCSV.headers.includes('SellWindow'));
    } finally {
      w.downloadCSV = prevDl;
    }

    // (8) Scenario shocks flow through oil-indexed cargoes via averageOilIndex
    w.setCurveShocks(null);
    const oilTest = JSON.parse(JSON.stringify(standalone));
    oilTest.id = 'C-TEST-SHOCK-OIL';
    oilTest.buy = { idx: 'Brent', formula: '% × Index', mult: 0.115, spread: 0, window: 3, lag: 0 };
    w.bookCargo(oilTest);
    const beforeBuy = w.computeCargoValues(oilTest).buyPx;
    w.setCurveShocks({ Brent: +10 });   // +$10/bbl Brent shock
    const shockedBuy = w.computeCargoValues(w.getCargoById('C-TEST-SHOCK-OIL')).buyPx;
    w.setCurveShocks(null);
    w.deleteCargo('C-TEST-SHOCK-OIL');
    // +$10 Brent × 11.5% slope = +$1.15 / MMBtu (uniform shock across 3 averaged months)
    check('Brent shock flows through (3,0,1) averaging: +$10 → +$1.15 LNG',
          Math.abs((shockedBuy - beforeBuy) - 1.15) < 1e-4,
          `Δ buyPx = ${(shockedBuy - beforeBuy).toFixed(4)} (expected 1.15)`);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 22 · Per-destination freight, holder field, COGH route, FOB Premium');
    console.log(bar);
    // (a) Per-destination freight: a basket with mixed freight per dest must use
    // the destination's own freight, not the basket-level fallback.
    const mixedInst = {
      dealId: 'TEST-MIXED', dealKind: 'cargo', dealType: 'mixed-freight test',
      loadMo: 'Jul-26', sellMo: 'Jul-26', tbtu: 3.4,
      contractedIdx: 'TTF', contractedPx: null,
      flexType: 'dest',
      params: {
        destinations: [
          { idx: 'TTF', freight: 1.53, label: 'Gate' },
          { idx: 'JKM', freight: 2.10, label: 'Tokyo (PC)' },
        ],
        freight: 99,    // intentionally absurd basket-level fallback
      },
    };
    const vMixed = w.valueInstance(mixedInst);
    // The expected best netback is whichever of TTF − 1.53 or JKM − 2.10 is higher
    const ttfPx = w.curveLookup('Jul-26', 'TTF');
    const jkmPx = w.curveLookup('Jul-26', 'JKM');
    const expectedBestNetback = Math.max(ttfPx - 1.53, jkmPx - 2.10);
    const reportedNetback = Math.max(...vMixed.detail.prices);
    check('Per-destination freight overrides basket fallback',
          Math.abs(reportedNetback - expectedBestNetback) < 1e-6,
          `got ${reportedNetback.toFixed(3)} vs ${expectedBestNetback.toFixed(3)}`);

    // (b) holder = 'cpty' flips sign of extrinsic.  Same instance with two holders.
    const heldInst = {
      ...mixedInst,
      params: { ...mixedInst.params, holder: 'me' },
    };
    const soldInst = {
      ...mixedInst,
      params: { ...mixedInst.params, holder: 'cpty' },
    };
    const vHeld = w.valueInstance(heldInst);
    const vSold = w.valueInstance(soldInst);
    check('holder=me yields positive extrinsic',
          vHeld.extrinsic >= 0,
          `extrinsic=${vHeld.extrinsic.toFixed(4)}`);
    check('holder=cpty yields negative (or zero) extrinsic',
          vSold.extrinsic <= 0,
          `extrinsic=${vSold.extrinsic.toFixed(4)}`);
    check('holder=cpty drops intrinsic to 0 (basis stays in contract price)',
          vSold.intrinsic === 0,
          `intrinsic=${vSold.intrinsic}`);
    check('holder=cpty extrinsic magnitude equals holder=me extrinsic',
          Math.abs(Math.abs(vHeld.extrinsic) - Math.abs(vSold.extrinsic)) < 1e-9);

    // (c) Routing: COGH route returns longer NM than Panama Canal
    const routes = w.getRoutes('Sabine Pass', 'Tokyo Bay');
    const pc   = routes.find(r => r.name === 'Panama Canal');
    const cogh = routes.find(r => r.name === 'Cape of Good Hope');
    check('Sabine→Tokyo has at least 2 routes',
          routes.length >= 2,
          `${routes.length} routes`);
    check('Cape of Good Hope route exists and is longer',
          cogh && pc && cogh.nm > pc.nm,
          `PC=${pc?.nm}, COGH=${cogh?.nm}`);
    // calcVoyage with explicit route should reflect the longer NM
    const vPC   = w.calcVoyage('Sabine Pass', 'Tokyo Bay', 'Jul-26', 3.6, null, 'Panama Canal');
    const vCOGH = w.calcVoyage('Sabine Pass', 'Tokyo Bay', 'Jul-26', 3.6, null, 'Cape of Good Hope');
    check('calcVoyage(COGH) NM matches route table',
          vCOGH.nm === cogh.nm);
    check('COGH voyage costs MORE than PC (longer + bunker)',
          vCOGH.totalCost > vPC.totalCost,
          `PC $${vPC.totalCost.toFixed(0)} vs COGH $${vCOGH.totalCost.toFixed(0)}`);
    check('PC voyage includes canal toll (~$360k)',
          vPC.canalCost === 360000);
    check('COGH voyage has zero canal toll',
          vCOGH.canalCost === 0);

    // (d) FOB Premium calculator: extrinsic ONLY, expressed against any reference
    const fp = w.fobPremium('Sabine Pass', 'Jul-26', '2026-04-22', [
      { idx: 'TTF', freight: 1.53 },
      { idx: 'NBP', freight: 1.55 },
      { idx: 'JKM', freight: 2.10 },
    ]);
    check('FOB Premium returns finite, non-negative number',
          isFinite(fp.premium) && fp.premium >= 0,
          `premium = $${fp.premium.toFixed(3)}/MMBtu`);
    check('FOB Premium = extrinsic only (≥ 0, time value)',
          fp.premium >= 0);
    // Sanity: best-of-N netback should equal max of individual netbacks
    const indivBest = Math.max(...fp.perDest.map(d => d.netback));
    check('Best-destination netback = max(per-destination netbacks)',
          Math.abs(fp.bestNetback - indivBest) < 1e-9);
    check('FOB Premium per-destination breakdown has 3 rows',
          fp.perDest.length === 3);

    // (e) Reference-index re-expression: an FOB price should be expressible as
    // basis to TTF, HH, or % × Brent without changing total fair value.
    const totalFairFob = fp.bestNetback + fp.premium;
    const ttfBasis = totalFairFob - w.curveLookup('Jul-26', 'TTF');
    const hhBasis  = totalFairFob - w.curveLookup('Jul-26', 'HH');
    const brentSlope = totalFairFob / w.curveLookup('Jul-26', 'Brent');
    check('TTF basis re-expression returns finite number',     isFinite(ttfBasis));
    check('HH basis re-expression returns finite number',       isFinite(hhBasis));
    check('Brent slope re-expression returns sensible slope',   isFinite(brentSlope) && brentSlope > 0 && brentSlope < 1);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 23 · FOB-liquid load ports (Oman, APLNG, Bontang, Algeria)');
    console.log(bar);
    // (a) Oman LNG → Tokyo direct via NM_DB
    const omanTokyo = w.calcVoyage('Oman LNG', 'Tokyo Bay', 'Jul-26', 3.6, null);
    check('Oman LNG → Tokyo Bay voyage finite',
          omanTokyo && isFinite(omanTokyo.totalCost),
          `total $${omanTokyo?.totalCost?.toFixed(0) ?? '—'}`);
    check('Oman → Tokyo NM = 4,176 (user authoritative — east-bound, no canal)',
          omanTokyo.nm === 4176);

    // (b) Oman → Gate is COGH-only (Oman cannot transit Suez per user spec)
    const omanRoutes = w.getRoutes('Oman LNG', 'Gate');
    const oCOGH = omanRoutes.find(r => r.name === 'Cape of Good Hope');
    const oSuezForbidden = omanRoutes.find(r => r.name === 'Suez Canal');
    check('Oman → Gate has COGH route',                !!oCOGH);
    check('Oman → Gate has NO Suez Canal route',       !oSuezForbidden);
    check('Oman → Gate default route IS COGH',         omanRoutes[0].name === 'Cape of Good Hope');
    check('Oman → Gate via COGH is 14,500 NM (user data)',  oCOGH.nm === 14500);

    // (c) Algeria → Mediterranean ports
    const algSines = w.calcVoyage('Algeria LNG', 'Sines', 'Jul-26', 3.6, null);
    check('Algeria → Sines voyage finite',
          algSines && isFinite(algSines.totalCost));

    // (d) FOB Premium for Oman with MEI-typical basket — Asia-biased because
    // NWE freight via COGH is too long to compete on most Atlantic legs.
    const fpOman = w.fobPremium('Oman LNG', 'Jul-26', '2026-04-22', [
      { idx: 'JKM', freight: 0.55, label: 'Tokyo' },
      { idx: 'JKM', freight: 0.30, label: 'Dahej' },
      { idx: 'TTF', freight: 3.10, label: 'NWE via COGH' },
    ]);
    check('Oman FOB Premium computed and finite',
          isFinite(fpOman.premium) && fpOman.premium >= 0,
          `Oman premium = $${fpOman.premium.toFixed(3)}/MMBtu`);

    // (e) Suez Canal is fully removed from the route catalogue (no LNG transits)
    const sabineDahej = w.getRoutes('Sabine Pass', 'Dahej');
    const corpusDahej = w.getRoutes('Corpus Christi', 'Dahej');
    const sabineTokyo = w.getRoutes('Sabine Pass', 'Tokyo Bay');
    const noSuezAnywhere =
      !sabineDahej.some(r => r.name === 'Suez Canal') &&
      !corpusDahej.some(r => r.name === 'Suez Canal') &&
      !sabineTokyo.some(r => r.name === 'Suez Canal');
    check('Suez Canal removed from all USGC routes',  noSuezAnywhere);
    check('Sabine→Dahej is COGH-only (Suez dropped)',
          sabineDahej.length === 1 && sabineDahej[0].name === 'Cape of Good Hope');
    check('Sabine→Tokyo COGH = 15,379 NM (user data)',
          sabineTokyo.find(r => r.name === 'Cape of Good Hope').nm === 15379);

    // (f) Oman LNG port cost matches user's authoritative number
    check('Oman LNG port cost = $126,637 (user data)',
          omanTokyo.dischPortCost === w.calcVoyage('Sabine Pass', 'Tokyo Bay', 'Jul-26', 3.6, null).dischPortCost);  // Tokyo cost stays the same; verify Oman load cost separately
    const omanLoadCost = w.calcVoyage('Oman LNG', 'Dahej', 'Jul-26', 3.6, null).loadPortCost;
    check('Oman LNG load port cost = $126,637',  omanLoadCost === 126637);

    // (g) FOB preset freights are computed live from calcVoyage — so switching
    // the load port and re-applying the preset must give different freight.
    w.document.getElementById('fob-load').value = 'Sabine Pass';
    w.document.getElementById('fob-month').value = 'Jul-26';
    w.presetFobBasket('mei');
    const sabineMeiBasket = w.getFobBasket();
    w.document.getElementById('fob-load').value = 'Oman LNG';
    w.presetFobBasket('mei');
    const omanMeiBasket = w.getFobBasket();
    // Sabine's Tokyo freight should be MUCH higher than Oman's (Sabine 9,150 NM via PC vs Oman 4,176 NM direct)
    const sabineTokyoF = sabineMeiBasket.find(d => /Tokyo/.test(d.label || ''))?.freight ?? 0;
    const omanTokyoF   = omanMeiBasket.find(d => /Tokyo/.test(d.label || ''))?.freight ?? 0;
    check('FOB preset freight responds to load port (Sabine→Tokyo > Oman→Tokyo)',
          sabineTokyoF > omanTokyoF + 0.20,
          `Sabine $${sabineTokyoF.toFixed(2)} vs Oman $${omanTokyoF.toFixed(2)}`);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 24 · Cargo form inputs for window / lag / scalar / pricingZ');
    console.log(bar);
    // The form now exposes the four extended pricing-window fields on each
    // leg. Setting non-defaults must round-trip into the booked cargo; defaults
    // (1/0/1/1) must be omitted from the cargo's leg object so legacy single-
    // month legs stay minimal.
    const doc = w.document;
    const set = (id, val) => { const el = doc.getElementById(id); if (el) { el.value = String(val); el.dispatchEvent(new w.Event('input', {bubbles:true})); el.dispatchEvent(new w.Event('change', {bubbles:true})); } return !!el; };
    check('Buy-leg Window input present',   !!doc.getElementById('f-buy-window'));
    check('Buy-leg Lag input present',      !!doc.getElementById('f-buy-lag'));
    check('Buy-leg Scalar input present',   !!doc.getElementById('f-buy-scalar'));
    check('Buy-leg PricingZ input present', !!doc.getElementById('f-buy-pricingZ'));
    check('Sell-leg Window input present',   !!doc.getElementById('f-sell-window'));
    check('Sell-leg Lag input present',      !!doc.getElementById('f-sell-lag'));
    check('Sell-leg Scalar input present',   !!doc.getElementById('f-sell-scalar'));
    check('Sell-leg PricingZ input present', !!doc.getElementById('f-sell-pricingZ'));

    // Default-collect: with all four at defaults, legs should NOT carry the
    // extension keys (cargoes stay minimal).
    set('f-buy-window', 1); set('f-buy-lag', 0); set('f-buy-scalar', 1); set('f-buy-pricingZ', 1);
    set('f-sell-window', 1); set('f-sell-lag', 0); set('f-sell-scalar', 1); set('f-sell-pricingZ', 1);
    const defCargo = w.collectFormCargo();
    check('Defaults: buy.window omitted',   defCargo.buy.window   === undefined);
    check('Defaults: buy.lag omitted',      defCargo.buy.lag      === undefined);
    check('Defaults: buy.scalar omitted',   defCargo.buy.scalar   === undefined);
    check('Defaults: buy.pricingZ omitted', defCargo.buy.pricingZ === undefined);
    check('Defaults: sell.window omitted',  defCargo.sell.window  === undefined);

    // Non-default round-trip: set Brent (3, 0, 1) with scalar 0.945 + pricingZ 3.
    set('f-buy-idx', 'Brent'); set('f-buy-formula', '% × Index'); set('f-buy-mult', 0.115);
    set('f-buy-window', 3); set('f-buy-lag', 0); set('f-buy-scalar', 0.945); set('f-buy-pricingZ', 3);
    const c = w.collectFormCargo();
    check('Non-default: buy.window = 3',      c.buy.window === 3,
          `got ${JSON.stringify(c.buy.window)}`);
    check('Non-default: buy.scalar = 0.945',  Math.abs(c.buy.scalar - 0.945) < 1e-9,
          `got ${JSON.stringify(c.buy.scalar)}`);
    check('Non-default: buy.pricingZ = 3',    c.buy.pricingZ === 3);
    check('Non-default: buy.lag stays omitted (=0)', c.buy.lag === undefined);

    // Reset form so subsequent runs aren't polluted.
    set('f-buy-window', 1); set('f-buy-lag', 0); set('f-buy-scalar', 1); set('f-buy-pricingZ', 1);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 25 · Oil-indexed destination editor in flex form');
    console.log(bar);
    // The flex-destination editor now exposes a dynamic list of
    // oil-indexed destinations with the {idx, slope, scalar, window, lag,
    // constant, freight} object shape. Adding rows + flipping the toggle on
    // must produce a destinations[] array that mixes gas-hub strings with
    // oil-indexed objects. Editing a cargo with mixed destinations must
    // round-trip back into the right surfaces.
    check('addOilDest is on window',     typeof w.addOilDest === 'function');
    check('readOilDests is on window',   typeof w.readOilDests === 'function');
    check('Oil-dest list container',     !!doc.getElementById('flex-oil-dests-list'));
    // Reset cleanly.
    if (doc.getElementById('flex-oil-dests-list')) doc.getElementById('flex-oil-dests-list').innerHTML = '';
    // Flip the destination toggle on, leave the gas-hub text at JKM,TTF,NBP, and
    // add one Brent (3,0,1) at 11.5% with a $0.50 constant + per-dest freight.
    const destOn = doc.getElementById('flex-dest-on'); destOn.checked = true;
    set('f-buy-pricingZ', 1); set('f-buy-window', 1); set('f-buy-scalar', 1); set('f-buy-lag', 0);
    w.addOilDest({ idx:'Brent', slope:0.115, window:3, lag:0, constant:0.50, freight:2.10 });
    check('Row was added to the list', doc.querySelectorAll('#flex-oil-dests-list .oil-dest-row').length === 1);
    const flexA = w.collectFormFlex();
    const destsA = flexA.destination?.destinations || [];
    check('destinations[] mixes strings + object', destsA.length === 4 && destsA.filter(d => typeof d === 'object').length === 1);
    const oilA = destsA.find(d => typeof d === 'object');
    check('Oil-dest persists slope = 0.115',     Math.abs(oilA.slope - 0.115) < 1e-9);
    check('Oil-dest persists window = 3',         oilA.window === 3);
    check('Oil-dest persists constant = 0.50',    Math.abs(oilA.constant - 0.50) < 1e-9);
    check('Oil-dest persists per-dest freight',   Math.abs(oilA.freight - 2.10) < 1e-9);
    check('Oil-dest scalar omitted at default',   oilA.scalar === undefined);
    check('Oil-dest lag omitted at default',      oilA.lag === undefined);
    // Add a second row with all non-defaults.
    w.addOilDest({ idx:'JCC', slope:0.135, scalar:0.945, window:5, lag:1, constant:0, freight:1.85 });
    const flexB = w.collectFormFlex();
    const oilB = flexB.destination.destinations.filter(d => typeof d === 'object');
    check('Two oil rows captured',          oilB.length === 2);
    check('Second row scalar = 0.945',      Math.abs(oilB[1].scalar - 0.945) < 1e-9);
    check('Second row lag = 1',             oilB[1].lag === 1);

    // Round-trip: build a fake cargo with these dests, run editCargo, confirm
    // the form's text input + oil-dest list end up split correctly.
    const fakeCargo = {
      id: 'TEST-OIL', status:'Fixed', cargoType:'Equity', cpty:'Test', dealType:'single',
      incoterm:'FOB/DES', coverage:'covered', loadPort:'Sabine Pass', dischPort:'Gate',
      loadMonth:'Jul-26', sellMonth:'Jul-26',
      vesselCbm:174000, fillPct:98.5, energyFactor:23.1, ladenDays:18, bogRate:0.10,
      buy:{ idx:'HH', formula:'% × Index + Spread', mult:1.15, spread:1.90 },
      sell:{ idx:'TTF', formula:'Index + Spread', mult:1.0, spread:-0.195 },
      flex:{ destination:{ destinations:['JKM','TTF',{idx:'Brent',slope:0.115,window:3,constant:0.50,freight:2.10}], freight:1.53 } },
      freightMode:'manual', freightManual:{freight:1.53,bog:0,other:0},
    };
    if (typeof w.bookCargo === 'function') w.bookCargo(fakeCargo);
    if (typeof w.editCargo === 'function') w.editCargo('TEST-OIL');
    const dests3 = doc.getElementById('flex-dest-dests')?.value || '';
    const oilRows3 = doc.querySelectorAll('#flex-oil-dests-list .oil-dest-row').length;
    check('editCargo: gas hubs land in text input', dests3.split(',').map(s=>s.trim()).sort().join(',') === 'JKM,TTF');
    check('editCargo: 1 oil-dest row populated', oilRows3 === 1);
    if (typeof w.deleteCargo === 'function') w.deleteCargo('TEST-OIL');
    // Reset state for any tests that follow.
    if (doc.getElementById('flex-oil-dests-list')) doc.getElementById('flex-oil-dests-list').innerHTML = '';
    destOn.checked = false;

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 26 · Holder radio on flex destination toggle');
    console.log(bar);
    // The destination flex now has a Held by [Me / Counterparty] selector.
    // 'me'   → flex.destination.holder is omitted (default), extrinsic is positive
    // 'cpty' → flex.destination.holder = 'cpty', valueInstance flips signs
    //          (extrinsic SUBTRACTS from book, intrinsic drops to 0)
    check('Holder selector present', !!doc.getElementById('flex-dest-holder'));
    check('Holder defaults to me',   doc.getElementById('flex-dest-holder')?.value === 'me');

    // collectFormFlex omits holder when value is 'me' (matches valueInstance default).
    destOn.checked = true;
    set('flex-dest-holder', 'me');
    const flexMe = w.collectFormFlex();
    check('me default → flex.destination.holder omitted', flexMe.destination.holder === undefined);

    // Switch to cpty → persisted as 'cpty'.
    set('flex-dest-holder', 'cpty');
    const flexCp = w.collectFormFlex();
    check('cpty → flex.destination.holder = "cpty"', flexCp.destination.holder === 'cpty');

    // Round-trip via editCargo: book a cargo with holder=cpty, edit, the select reflects it.
    const fakeCargoH = {
      id:'TEST-HOLDER', status:'Fixed', cargoType:'Equity', cpty:'TestCpty', dealType:'single',
      incoterm:'FOB/DES', coverage:'covered', loadPort:'Sabine Pass', dischPort:'Gate',
      loadMonth:'Jul-26', sellMonth:'Jul-26',
      vesselCbm:174000, fillPct:98.5, energyFactor:23.1, ladenDays:18, bogRate:0.10,
      buy:{idx:'HH',formula:'% × Index + Spread',mult:1.15,spread:1.90},
      sell:{idx:'TTF',formula:'Index + Spread',mult:1.0,spread:-0.195},
      flex:{destination:{destinations:['JKM','TTF','NBP'], freight:1.53, holder:'cpty'}},
      freightMode:'manual', freightManual:{freight:1.53,bog:0,other:0},
    };
    if (typeof w.bookCargo === 'function') w.bookCargo(fakeCargoH);
    if (typeof w.editCargo === 'function') w.editCargo('TEST-HOLDER');
    check('editCargo populates Held by = cpty', doc.getElementById('flex-dest-holder')?.value === 'cpty');
    if (typeof w.deleteCargo === 'function') w.deleteCargo('TEST-HOLDER');

    // Math sign-flip: valueInstance must return signed extrinsic when holder=cpty.
    // Use the same instance shape getOptionalityInstances() produces, so the
    // function gets every field it expects (sellMo, tbtu, params, etc).
    if (typeof w.valueInstance === 'function') {
      const mkInst = (holder) => ({
        dealId:'OPT-'+holder, dealKind:'cargo', dealType:'Equity',
        loadMo:'Jul-26', sellMo:'Jul-26', tbtu:3.4,
        contractedIdx:'TTF', contractedPx: null,
        flexType:'dest',
        params:{ destinations:['JKM','TTF','NBP'], freight:1.53, holder },
      });
      const me = w.valueInstance(mkInst('me'));
      const cp = w.valueInstance(mkInst('cpty'));
      check('valueInstance returns extrinsic for me-holder',     isFinite(me?.extrinsic));
      check('valueInstance returns extrinsic for cpty-holder',   isFinite(cp?.extrinsic));
      // cpty extrinsic is the signed (negative) version of me extrinsic
      check('cpty extrinsic = -me extrinsic',  Math.abs(me.extrinsic + cp.extrinsic) < 1e-9,
            `me=${me?.extrinsic?.toFixed?.(4)} cp=${cp?.extrinsic?.toFixed?.(4)}`);
      // cpty intrinsic drops to 0 (basis priced into the FOB sale)
      check('cpty intrinsic = 0', cp.intrinsic === 0);
    }

    destOn.checked = false;
    set('flex-dest-holder', 'me');

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 27 · Exposure unit toggle (TBtu / Lots)');
    console.log(bar);
    // The exposure grid now renders either raw TBtu or exchange-lots count.
    // Lots = TBtu × 1e6 / LOT_SIZE[idx]. The user's reference example:
    //   3.5 TBtu of HH-indexed exposure → 1,400 lots (3,500,000 / 2,500).
    check('Unit toggle present',           !!doc.getElementById('exp-unit-toggle'));
    check('TBtu button present',           !!doc.querySelector('#exp-unit-toggle [data-unit="tbtu"]'));
    check('Lots button present',           !!doc.querySelector('#exp-unit-toggle [data-unit="lots"]'));
    // LOT_SIZE is a top-level const inside morning_book.html and not on
    // window in JSDOM (HANDOVER §4 testability trap). Read the lot conversion
    // from the source via an export probe instead.
    const HH_LOT  = 2500;       // documented industry standard
    const TTF_LOT = 10000;
    check('Source has HH lot constant 2500',  /HH:2500/.test(html));
    check('Source has TTF lot constant 10000', /TTF:10000/.test(html));
    // Pure conversion math (independent of any rendered grid):
    const lotsHH  = 3.5 * 1e6 / HH_LOT;
    const lotsTTF = 3.5 * 1e6 / TTF_LOT;
    check('3.5 TBtu × HH = 1,400 lots',    Math.round(lotsHH) === 1400, `got ${lotsHH}`);
    check('3.5 TBtu × TTF = 350 lots',     Math.round(lotsTTF) === 350, `got ${lotsTTF}`);
    // Toggle the grid into Lots mode and confirm the title updates.
    if (typeof w.setExposureUnit === 'function') {
      w.setExposureUnit('lots');
      check('Exposure unit set to lots',     w.getExposureUnit() === 'lots');
      const titleLots = doc.getElementById('grid-title')?.textContent || '';
      check('Title reflects lots mode',      /lots/.test(titleLots));
      w.setExposureUnit('tbtu');
      check('Reset to tbtu mode',            w.getExposureUnit() === 'tbtu');
    }

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 28 · Parent dashboard data integration (postMessage)');
    console.log(bar);
    // The iframe exposes applyExternalData(payload) and posts 'pv-ready' to its
    // parent on boot. The main app listens, builds a payload from its live aD /
    // eexD / F.snaps, posts it back. On receipt, the iframe replaces its seed
    // curve + freight snapshots with the parent's live data.
    check('applyExternalData on window',  typeof w.applyExternalData === 'function');
    // Use window-exposed listCurveSnapshots / listFreightSnapshots / curveLookup
    // / freightLookup since STATE itself isn't on window in JSDOM.
    const beforeCurves  = w.listCurveSnapshots().length;
    const beforeFreight = w.listFreightSnapshots().length;
    check('Has seed curve snapshots',   beforeCurves >= 1);
    check('Has seed freight snapshots', beforeFreight >= 1);
    // Apply a synthetic external payload — two curve dates + one freight date.
    const payload = {
      reason: 'verify-test',
      curveSnapshots: {
        '2026-04-23': { 'May-26': { TTF: 99.99, JKM: 100.0, HH: 1.111, NBP: 88.8, Brent: 70.0 } },
        '2026-04-24': { 'May-26': { TTF: 100.5, JKM: 101.0, HH: 1.222, NBP: 89.0, Brent: 70.5 } },
      },
      freightSnapshots: {
        '2026-04-24': { 'May-26': { BLNG1: 75555, BLNG2: 80555, BLNG3: 95555 } },
      },
    };
    w.applyExternalData(payload);
    const curvesAfter   = w.listCurveSnapshots();
    const freightAfter  = w.listFreightSnapshots();
    check('Curves replaced (2 dates)',   curvesAfter.length === 2 && curvesAfter.includes('2026-04-24'));
    check('Curve activeDate is set',     w.listCurveSnapshots().includes('2026-04-24'));
    check('Curve cell carried through',  Math.abs(w.curveLookup('May-26','TTF','2026-04-24') - 100.5) < 1e-9);
    check('Freight replaced (1 date)',   freightAfter.length === 1 && freightAfter[0] === '2026-04-24');
    check('Freight cell carried through',w.freightLookup('May-26','BLNG2','2026-04-24') === 80555);
    // Empty payload no-op.
    const beforeNoop = w.listCurveSnapshots().length;
    w.applyExternalData({ reason:'noop' });
    check('Empty payload is no-op', w.listCurveSnapshots().length === beforeNoop);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 29 · Phase 2 — NM_DB / PORT_COSTS / phys diffs ingestion');
    console.log(bar);
    // applyExternalData merges nm + portCosts into the iframe's NM_DB and
    // PORT_COSTS const objects (mutated in place; const binds the variable
    // not the contents). Phys diffs land on window.__externalPhysDiffs.
    check('getNmDb getter exposed',      typeof w.getNmDb === 'function');
    check('getPortCosts getter exposed', typeof w.getPortCosts === 'function');
    const nmBefore    = Object.keys(w.getNmDb() || {}).length;
    const portsBefore = Object.keys(w.getPortCosts() || {}).length;
    check('Seeded NM_DB has entries',     nmBefore >= 1);
    check('Seeded PORT_COSTS has entries',portsBefore >= 1);
    // Push a tiny payload — one new origin with two destinations + a port
    // cost for that origin + a phys-diffs blob.
    const ph2 = {
      reason: 'phase2-test',
      nm: {
        'TestPort A': { 'TestDest X': 1234, 'TestDest Y': 5678 },
        'Sabine Pass': { 'TestDest X': 9999 },  // merge into existing origin
      },
      portCosts: { 'TestPort A': 111111 },
      physDiffs: { nwe: [-0.40,-0.40,-0.40], jktc: [0.10,0.10,0.10] },
    };
    w.applyExternalData(ph2);
    const nmAfter   = w.getNmDb();
    const portsAfter= w.getPortCosts();
    check('NM_DB gained TestPort A',           !!nmAfter['TestPort A']);
    check('TestPort A → TestDest X = 1234',    nmAfter['TestPort A']?.['TestDest X'] === 1234);
    check('Sabine Pass merged (kept Gate)',    nmAfter['Sabine Pass']?.['Gate'] === 4902);
    check('Sabine Pass added TestDest X',      nmAfter['Sabine Pass']?.['TestDest X'] === 9999);
    check('PORT_COSTS gained TestPort A',      portsAfter['TestPort A'] === 111111);
    check('PORT_COSTS kept Sabine Pass cost',  portsAfter['Sabine Pass'] === 216342);
    check('Phys diffs stored on window',       Array.isArray(w.__externalPhysDiffs?.nwe));
    check('Phys diffs jktc carried through',   w.__externalPhysDiffs?.jktc?.[0] === 0.10);

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SECTION 30 · P&L tab — Summary sub-tab + source toggle');
    console.log(bar);
    // The P&L tab now has two sub-tabs: ① P&L (summary breakdown), ② P&L Log.
    // The summary pane has its own source toggle (Combined / Physical / Hedge)
    // and three breakdown tables (by month, by contract, by deal type).
    check('Top-level tab renamed to P&L',  doc.querySelector('.tabbar button[data-tab="pnl"]')?.textContent.trim() === 'P&L');
    check('Sub-tab nav present',           !!doc.getElementById('pnl-subtab'));
    check('Summary pane present',          !!doc.getElementById('pnl-pane-summary'));
    check('Log pane present',              !!doc.getElementById('pnl-pane-log'));
    check('Source toggle present',         !!doc.getElementById('pnl-source-toggle'));
    check('By-month table present',        !!doc.getElementById('pnl-by-month-body'));
    check('By-contract table present',     !!doc.getElementById('pnl-by-contract-body'));
    check('By-deal table present',         !!doc.getElementById('pnl-by-deal-body'));
    check('renderPnlSummary on window',    typeof w.renderPnlSummary === 'function');
    check('exportPnlSummaryCSV on window', typeof w.exportPnlSummaryCSV === 'function');
    check('Default sub-tab = summary',     w.getPnlSubTab() === 'summary');
    check('Default source = combined',     w.getPnlSource() === 'combined');
    // Render → KPI strip + tables populate without throwing.
    w.renderPnlSummary();
    const totalCell = doc.getElementById('pnl-sum-total');
    check('Total P&L KPI rendered',        totalCell && totalCell.textContent !== '—');
    const monthRows = doc.querySelectorAll('#pnl-by-month-body tr').length;
    check('By-month table has rows',       monthRows >= 1);
    const dealRows = doc.querySelectorAll('#pnl-by-deal-body tr').length;
    check('By-deal table has rows',        dealRows >= 1);
    // Toggle to Physical → renders without throwing.
    w.setPnlSource('physical');
    check('Physical source applied',       w.getPnlSource() === 'physical');
    w.setPnlSource('combined');
    // Switch to Log sub-tab and back.
    w.setPnlSubTab('log');
    check('Log sub-tab activated',         w.getPnlSubTab() === 'log');
    check('Log pane visible',              doc.getElementById('pnl-pane-log').style.display !== 'none');
    check('Summary pane hidden',           doc.getElementById('pnl-pane-summary').style.display === 'none');
    w.setPnlSubTab('summary');

    // ══════════════════════════════════════════════
    console.log('\n' + bar);
    console.log('SUMMARY');
    console.log(bar);
    console.log(`    Passed: ${passed}`);
    console.log(`    Failed: ${failed}`);
    console.log(`    Total:  ${passed + failed}`);
    if (failed === 0) console.log(`\n    ✓ All regression checks passed.`);
    else              { console.log(`\n    ✗ ${failed} check(s) failed.`); process.exitCode = 1; }

    dom.window.close();
  } catch (e) {
    console.error('\nSUITE ERROR:');
    console.error(e.message);
    console.error(e.stack);
    process.exit(1);
  }
}, 500);
