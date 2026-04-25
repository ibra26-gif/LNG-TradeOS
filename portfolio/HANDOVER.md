# LNG TradeOS dashboard — Handover to Claude Code

You are picking up a working prototype of an LNG trading dashboard. It's one HTML file, opens locally in a browser, persists state via `localStorage`. It will eventually be integrated into the real LNG TradeOS product. Until then, it runs standalone and is genuinely useful for trade capture and MTM.

Read this document end-to-end before touching the file. It is self-contained — you do not need any prior conversation context.

---

## 1. File layout

```
outputs/
├── morning_book.html    # the whole application (~486KB, ~9,460 lines)
├── verify.js            # JSDOM-based regression test suite (23 sections, 160 checks)
└── HANDOVER.md          # this document
```

Single-file HTML by design — the user can open it from the filesystem with no build step. CSS, HTML, and JS all inline. When LNG TradeOS integration happens, the UI will be re-skinned on their framework; what transfers over is the schema, the math, and the architectural seams.

---

## 2. Quickstart

```bash
# Run the regression suite
node outputs/verify.js

# Open the app
open outputs/morning_book.html   # or just double-click it
```

The dashboard initializes with:
- **4 parent deals** with **392 generated child cargoes** (full-tenor materialization — not a rolling window):
  - `LT-EG-SPA` (Equatorial Guinea SPA · 10y · 1/mo FOB buy · `0.945 × TTF − 2.91`) — **120 children**
  - `LT-CHENIERE` (Cheniere Sabine SPA · 20y · 1/mo FOB buy · `115% × HH + 2.50`) — **240 children**
  - `STR-KRK-27Q` (KrK Short 2027-28 · quarterly DES sell · `TTF − 0.60`) — 8 children
  - `STR-GATE-26` (Gate Short · 2y monthly DES sell · `TTF − 0.25`, cpty OMV) — 24 children
- 6 seed paper trades (2 hedges, 4 specs — T-1234 through T-1239)
- Curve snapshots for 2026-04-22 (from the user's EOD Excel) + 2026-04-21 (synthetic baseline)
- Freight snapshots for same dates (from the user's FFA image)
- **30-day P&L history** seeded for equity-curve visualization
- Default settings (174k vessel class, Atlantic basin params)

On first load, all state gets seeded into localStorage under the key `lng-tradeos.state.v1`. Subsequent loads re-hydrate from localStorage and forward-migrate through schema versions.

**Note on full-tenor materialization**: we generate every month of an LT contract (e.g., 120 for a 10y SPA), not a rolling 24-month window. The user wants to "look at the whole thing" for P&L. For far-tenor months where the explicit forward curve runs out, `curveLookup` cascades: monthly → quarter → Cal → last-known → 0. The cargo table has a window pill (All / Next 12mo / Next 24mo / YTD / Actualized) to narrow the view.

---

## 3. What exists today

### Tabs (in order)

| Tab | Status | What it does |
|---|---|---|
| Morning Book | Live | KPI strip + summary cards · all derived from state · top-right Export dropdown |
| Cargo Book | Live | Physical trade capture + dynamic cargo table (parent/child grouping) · CSV export |
| Paper Book | Live | Hedge/spec capture + dynamic paper table · CSV export |
| Exposure Book | Live | Index × month exposure grid (physical, hedges, net) · CSV export via Morning Book dropdown |
| Spec Book | Live | Outright speculative positions with R:R, SL progress, budget · CSV export |
| P&L Log | **Live** | 30-day equity-curve canvas chart + KPI strip + daily log table + EOD snapshot button |
| Curves | Live | Editable LNG forward curves with snapshot management |
| Freight | Live | Editable BLNG1/2/3 curves with route classifier |
| Scenarios | **Live** | 6 preset shocks + custom Δ grid + per-cargo impact table with base/shocked/Δ |
| Optionality | Live | Intrinsic + extrinsic of cargo flex rights · per-instance / by-deal toggle · CSV export · oil-indexed destinations |
| Settings | Live | Global vessel/fuel/emissions/cost params |

Removed from prior versions: `Arb`, `C/U/T`, `Explain P&L` (the user didn't want them).

### State layer (schema v7)

```js
STATE = {
  version: 7,
  paperTrades: [],     // Paper Book records
  cargoes: [],         // Cargo Book records (includes children of parent deals via parentDealId)
  parentDeals: [],     // Strip / LT parent deal records; children link via cargo.parentDealId
  nextIds: { trade, cargo },
  curves: {
    snapshots: { 'YYYY-MM-DD': { 'Jul-26': { JKM, TTF, NBP, HH, Brent }, ... } },
    activeDate: null,      // which snapshot drives MTM
    baselineDate: null,    // compared against active for daily Δ
  },
  freightCurves: {
    snapshots: { 'YYYY-MM-DD': { 'Jul-26': { BLNG1, BLNG2, BLNG3 }, 'Q3-26': {...}, 'Cal-27': {...} } },
    activeDate: null,
    baselineDate: null,
  },
  pnlHistory: [{ date, physMtm, hedgePnl, specMtm, combined }, ...],
  settings: {
    vessel: {   // 18 fields: speed, port days, fuel consumption, heel, CO2 factors, pos/repo %
      speedKnots, loadPortDays, dischPortDays, bufferDays, cooldownDays,
      ladenHFOtpd, ballastHFOtpd, ladenLSMGOtpd, ballastLSMGOtpd, dischLSMGOtpd, loadLSMGOtpd, pilotFuelT,
      heelRetentionM3, heelCostPerM3,
      positioningPct, repositioningPct,
      co2FactorHFO, co2FactorMGO,
    },
    fuel:   { hfoUSD, lsmgoUSD, euaUSD, eurUsd },
    other:  { warRiskPremiumUSDperDay, suezCanalUSD, panamaCanalUSD },
    limits: { specBudgetUSDM },
    updatedAt: 'YYYY-MM-DD',
  },
}
```

### Key capabilities

**Cargo Book form — 8-section trade capture:**
1. Deal type — Single / Strip / LT
2. Incoterm structure — FOB/DES, DES/DES, FOB/FOB, DES/FOB (drives freight applicability)
3. Deal identity — ID, Cargo type, Status (Fixed / Actualized), Counterparty
4. Coverage — Covered / Unsold / Uncovered
5. Voyage & quantity — ports, months, vessel size × fill factor, energy factor, BOG rate, laden days
6. Buy leg — Index, formula (`Flat` / `Index + Spread` / `% × Index` / `% × Index + Spread`), mult, spread
7. Sell leg — same
8. Flex rights — per-cargo toggles for Destination / Volume / Cancellation / Reload

Live preview on the right (sticky): KPIs (Unit margin, Net P&L), P&L decomposition (Revenue − Supply − Shipping), Quantity (Loaded → BOG → EDQ), Unit economics, C/U/T hint, Curve lookup, Exposure delta.

**Math:**
- Loaded = `vesselCbm × (fillPct/100) × energyFactor / 1e6` → TBtu
- BOG = `Loaded × (bogRate/100) × ladenDays` (only when freight applies)
- EDQ = `Loaded − BOG`
- For FOB/DES: Revenue = `sell × EDQ`, Supply = `buy × Loaded`, Shipping = `total voyage $`
- For DES/DES: pass-through, `Net = (sell − buy) × EDQ`, no shipping in book
- For FOB/FOB: title flips at load, `Net = (sell − buy) × Loaded`, no BOG

**Voyage calculator:**
- NM from internal `NM_DB`
- Laden days = `NM / (speed × 24)`
- Hire = `rate × voyageDays`, rate from `freightLookup(loadMonth, routeCurve)`
- Route curve from `routeBasin(loadPort, dischPort)` → BLNG1/2/3
- Fuel = `HFO tonnes × $HFO + LSMGO tonnes × $LSMGO` (all params from Settings)
- Port costs from hardcoded `PORT_COSTS` (extracted from user's real Port Cost DB)
- EU ETS = `CO2 tonnes × voyShare × rampFactor × EUA$`, ramp 40/70/100% for 2024/25/26+
- All params except BOG rate live in `STATE.settings.vessel/fuel` — edit in Settings tab → every voyage re-computes

**Freight curves:**
- Editable grid: 3 routes × (monthly + quarterly + Cal) periods
- Seeded from user's OB FFA Rates image (22-Apr-2026 snapshot)
- Paste from tab-separated Excel block
- Snapshot picker, baseline comparison, Δ / % views
- CSV export, snapshot history, basis spreads (BLNG2−BLNG1, BLNG3−BLNG1, BLNG3−BLNG2)

**LNG curves:**
- Editable grid: 12 indices × 26 months
- Seeded from user's OB LNG EOD Curve 22.04.2026.xlsx (COB sheet, Converted FPs block in $/MMBtu)
- Same paste / snapshot / export pattern as Freight

**Optionality valuation:**
- 4 flex types: destination (Margrabe N-dest via Clark recursion), volume (Bachelier straddle), cancellation (Bachelier put), reload (Margrabe spread)
- Reads flex terms from `cargo.flex` when present, plus a hardcoded seed of LT-Shell + STR-S26
- Editable vols (12 indices) + correlation matrix
- Ledger with per-instance sensitivity (vol ±20%, discount ±1%)
- Verified: Margrabe matches Hull Example 36.1 (3.91), Bachelier ATM identity passes

**Settings:**
- Editable vessel + fuel + emissions + other costs + risk limits
- Live propagation: edit HFO $550 → $700, every voyage re-computes, cargo MTMs shift
- JSON export/import of the full state (backup/restore/handover)
- "Reset to defaults" for settings only; "Reset all state" for nuclear option

---

## 4. Architectural conventions — don't break these

### Single `<script>` block
No modules, no build. Top-level declarations use `let`/`const`/`function`. Most helpers are `function foo()` so they attach to `window` for JSDOM testability.

### Getters for JSDOM testability
`let` and `const` declarations at the top level **do not attach to `window`** in JSDOM. If you add state and need to expose it to tests, wrap it in a getter:

```js
let MKT_VOLS = {...};
function getMktVols() { return MKT_VOLS; }   // window.getMktVols() works in tests
function updateVol(idx, pct) { MKT_VOLS[idx] = pct/100; renderOptionality(); }
```

Patterns that are already on window: `getSetting`, `setSetting`, `getCargoes`, `getPaperTrades`, `getHedges`, `getSpecs`, `curveLookup`, `freightLookup`, `routeBasin`, `computeCargoValues`, `computeHedgeExposure`, `computePhysicalExposure`, `tradePnL`, `nextTradeId`, `nextCargoId`, `bookTrade`, `bookCargo`, `deleteTrade`, `deleteCargo`, `undoDelete`, `undoDeleteCargo`, `resetState`, `resetSettings`, `listCurveSnapshots`, `listFreightSnapshots`, `valueInstance`, `getOptionalityInstances`.

### State mutation pattern
Every mutation:
```js
function bookX(x) {
  const i = STATE.xs.findIndex(...);
  if (i >= 0) STATE.xs[i] = x; else STATE.xs.push(x);
  saveState();
  renderAll();
}
```

`renderAll()` fans out to every top-level `renderFoo()` function that exists. Add your new tab's render to that function.

### Schema versioning
`STATE.version` is currently `7`. Each bump is forward-only and non-destructive:
- v1 → v2: added `cargoes`
- v2 → v3: added `curves`
- v3 → v4: added `freightCurves` + `pnlHistory`
- v4 → v5: cargo `loadedCbm` → `vesselCbm × fillPct` (migration: `vesselCbm = round(loadedCbm / 0.985)`)
- v5 → v6: global `settings` (migration: seed from `DEFAULT_SETTINGS()`)
- v6 → v7: added `parentDeals` array + `cargo.parentDealId` field (nullable, no data to migrate)

When you bump the schema, add your migration inside `loadState()` under the appropriate version check. Never drop fields; only add with defaults. The migration block is idempotent — running it on already-migrated state is a no-op.

### Unit conventions (critical)
- `tradePnL(t)` returns **raw $** (e.g., `364000` for $364k)
- `computeCargoValues(c).mtm` returns **$M** (because `sell[$/MMBtu] × EDQ[TBtu]` gives $M directly)
- When combining cargo MTM with hedge/spec P&L, multiply cargo MTM by `1e6` first
- `fmtBig(v)` expects raw $ input

### CSS design tokens
Use existing variables:
```
--bg, --bg-card, --bg-sunken, --bg-header
--text-1 (primary), --text-2, --text-3, --text-4
--pos, --neg, --warn, --info (each has -bg, -border, -text variants)
--shadow-card, --shadow-card-h
--input-bg, --input-border, --input-focus
```
Both light and dark themes are defined. New colors should go in as tokens, both themes.

### ID naming conventions
- `f-*` — Cargo Book form fields (`f-vessel-cbm`, `f-efactor`, `f-buy-idx`, `f-loadmo`)
- `c-*` — Cargo-level identity (`c-id`, `c-type`, `c-cpty`, `c-notes`)
- `t-*` — Paper Book trade identity (`t-id`, `t-date`, `t-venue`)
- `p-*` — Paper leg (`p-idx`, `p-dir`, `p-mo`, `p-lots`, `p-exec`)
- `s-*` — Spec context (`s-sl`, `s-target`, `s-mark`, `s-strategy`)
- `pv-*` — Cargo Book preview panel (`pv-sell`, `pv-rev`)
- `sb-*` — Spec Book KPIs
- `opt-*` — Optionality tab
- `set-*` — Settings tab
- `flex-*-on/…` — per-cargo flex editor toggles

### Month strings
Always normalized: `'Jul-26'` (monthly), `'Q3-26'` (quarterly), `'Cal-27'` (annual). Use `periodToMonths(p)` to expand Q/Cal into constituent monthlies.

### Curves access
Never read `CURVES[month][idx]` directly. Use `curveLookup(month, idx, date?)`. The legacy `CURVES` const still exists as a fallback inside `curveLookup` itself, but no other code should touch it. Same pattern for freight: `freightLookup(period, route, date?)`.

### Toast API
```js
toast(message, { kind: 'success' | 'error' | 'undo' | 'info', duration, action })
// With undo:
toast('Cargo C-246 deleted', {
  kind: 'undo', duration: 10000,
  action: { label: 'Undo', handler: () => undoDeleteCargo() }
});
```

---

## 5. What's now DONE (since the v7 handover)

Every item that was on the prior "Next priorities" list has landed. Quick digest:

**CSV Exports — done**
- Generic helpers: `csvCell(v)` and `downloadCSV(filename, headers, rows)` emit UTF-8-BOM-prefixed CSV so Excel reads accents cleanly.
- Per-tab builders: `exportCargoCSV()`, `exportPaperCSV()`, `exportSpecCSV()`, `exportOptionalityCSV()`, `exportExposureCSV()`. Each one materializes the live state into a flat row shape; columns match the on-screen tables.
- Wiring: Export button on each of Cargo / Paper / Spec / Optionality tables. Morning Book has a top-right **Export ▾** dropdown with all five destinations.

**P&L Log tab — done**
- KPI strip (Current, Peak, Drawdown, Best/Worst day, Avg Δ)
- Canvas line chart (no library) — retina-scaled, handles JSDOM canvas-missing gracefully (wrapped in try/catch with early-return)
- Daily log table with range toggle (7 / 30 / all days)
- "Snapshot EOD now" button — appends to `STATE.pnlHistory`
- Seeded with 30 synthetic days of perturbed combined-book values

**FOB Premium / `holder` field / per-destination freight / COGH route / FOB calculator widget**

The destination-flex math now distinguishes who OWNS the option:
- `flex.destination.holder = 'me'` (default) → extrinsic adds to my book (option I hold)
- `flex.destination.holder = 'cpty'` → extrinsic SUBTRACTS from my book (I sold the option to my counterparty for an FOB-style premium baked into the sale price). Intrinsic drops to 0 in this case — the basis MTM is already captured in the contracted price.

`flex.destination.destinations[i]` can carry an optional `freight` per destination, so a basket can mix DES_NWE@$1.53 and DES_JKTC@$2.10 simultaneously. The basket-level `params.freight` is now a fallback only.

`ROUTE_ALTERNATIVES` table + `getRoutes(load, disch)` returns named routes (Panama Canal, Cape of Good Hope, Suez Canal). `calcVoyage(load, disch, month, tbtu, rate, routeName)` accepts an optional route override; the canal toll is added to `totalCost` and the longer NM flows through bunker/laden days. Default behaviour unchanged.

**FOB Premium calculator** (Optionality tab, new card):

- Trader picks load port + delivery month + as-of date + reference index (flat $, TTF basis, HH basis, JKM basis, % × Brent, % × JCC)
- Editable basket of reachable destinations with one row per destination — index dropdown, per-destination freight, and (when index is Brent/JCC) inline (x,y,z) + slope + constant fields
- Three preset baskets: Atlantic / Pacific / Global
- Compute button runs `fobPremium(load, month, asof, basket)` → returns `{ bestNetback, premium, vol, T, perDest }`
- **`premium = extrinsic ONLY`** (= time value of the destination option), not intrinsic + extrinsic. Intrinsic is just basis already in the FOB quote.
- Result strip: best-destination netback ($/MMBtu), FOB Premium ($/MMBtu), implied FOB price (re-expressed in trader's chosen reference index), LNG-equivalent vol used by Margrabe/Clark
- Per-destination breakdown table highlighting the best-netback row
- Math reuses the same Margrabe-N + Clark-recursion machinery as `valueInstance`'s destination branch

**Sample run** — Sabine Pass, Jul-26 delivery, basket = {TTF@$1.53, NBP@$1.55, JKM@$2.10}:
```
  Best-destination netback = $15.35/MMBtu  (NBP − $1.55)
  FOB Premium             = $0.73/MMBtu   (extrinsic only)
  Implied FOB price       = $16.08/MMBtu  flat, or HH + $12.78 (basis), or 16.9% × Brent
  Vol (LNG-equivalent)    = ~28%
```

The widget answers two trader questions in one place:
1. "What FOB premium should I charge a buyer for the destination optionality I'm transferring?" → read `FOB Premium ($/MMBtu)`
2. "Where should I quote my FOB sale today?" → read `Implied FOB price` in whichever reference makes most sense for the buyer (HH for HH-linked offtakers, % × Brent for Japanese SPA buyers, etc.)

**Pre-merge cleanup — exposure / formula display / edit round-trip / CSV / oil-shock**

Five known issues that lived between math layer and UI layer have been closed before merging into Claude Code:

- **`computePhysicalExposure` now honours `scalar`, `window`, and `lag`.** Helper `legSlope(leg) = scalar × mult` (when applicable) and `legMonths(anchor, leg)` spreads exposure across the pricing window. A 94.5% × 11.5% × Brent leg now produces the correct combined-slope exposure. Verified: Apr/May/Jun-26 each carry a third of a (3,0,1)-Brent cargo's exposure.
- **Cargo-table `formulaText` shows the full leg shape.** Renders `94.5%×12%×Brent(3,0,3) → TTF + 0.00` instead of the bare `12%×Brent`. The `(x,y,z)` tag and the scalar prefix are conditional — legacy single-month legs render unchanged.
- **`editCargo` round-trips the extended fields.** `collectFormCargo` carries `window`, `lag`, `scalar`, `pricingZ`, plus `buyPricingAnchor`/`sellPricingAnchor` and `parentDealId` from the loaded cargo even though the form has no UI for them. Same pattern for `flex` (oil-indexed destination objects survive an edit). No silent data loss.
- **CSV export carries the new fields.** Columns added: `BuyWindow, BuyLag, BuyScalar, BuyPricingZ, BuyPricingAnchor` and the `Sell*` equivalents.
- **Scenario shocks flow through oil-indexed cargoes.** A `setCurveShocks({Brent: +10})` shock on a `11.5% × Brent(3,0,1)` leg moves the buy price by exactly +$1.15/MMBtu (uniform shock applied across the 3-month window). No leakage after `setCurveShocks(null)`.

**z-aware pricing period grouping**
- Formula legs can carry `pricingZ: N` (default 1). When a parent deal has `pricingZ > 1` on its buy or sell leg, `generateChildrenForParent` stamps each child with a `buyPricingAnchor` / `sellPricingAnchor` equal to the LAST month of its z-month bucket (aligned to the parent's `start` month).
- `resolveLeg` then swaps in the anchor month instead of the cargo's own `loadMonth`/`sellMonth` for all curve lookups — so every cargo in a bucket computes to the identical price.
- Example: LT with start `2026-05`, `pricingZ: 3` → buckets `[May/Jun/Jul-26], [Aug/Sep/Oct-26], …`; anchor = `Jul-26` for the first bucket, `Oct-26` for the second. All three May/Jun/Jul cargoes share one price.
- Regression verified: May/Jun/Jul-26 EG SPA cargoes share buyPx `$11.4824` under z=3, Aug-26 gets a different price.
- Backward compatible: `pricingZ` absent → single-month pricing (today's behaviour); no existing cargo's MTM changed.

**Krk port data wired (Sabine → Krk)**
- Added to `NM_DB`: Sabine Pass → Krk = **5,381 NM**
- Added to `PORT_COSTS`: Krk = **$242,000** discharge cost
- `calcVoyage('Sabine Pass', 'Krk', …)` now returns finite numbers. Laden days ≈ 13.59 at 16.5 kn.
- STR-KRK-27Q strip deal can now be voyage-costed end-to-end.

**Reload flex type — deprioritized**
- Per user: "ignore the reload". The `reload` flex type code remains in place (harmless, already tested) but no further features, seeds, or UI work should target it. Destination, volume, and cancellation remain the three active flex types.

**Cargo buy/sell formula legs — now support (x,y,z) too**
- `resolveLeg` in `computeCargoValues` now accepts optional `window`, `lag`, `scalar` on any formula leg.
- Same semantics as destination flex: `price = scalar × mult × avg(idx over [M−lag−window+1 … M−lag]) + spread`
- With defaults `window=1, lag=0, scalar=1` the formula collapses to the old single-month behaviour — all legacy cargoes unchanged (verified).
- Example leg: `{ idx:'Brent', formula:'% × Index', mult:0.115, spread:0, window:3, lag:0 }` → `11.5% × avg(Brent Apr/May/Jun-26)` for a Jun-26 delivery.
- `'Flat'` legs skip index lookup (no averaging needed — price is just the spread).

**Oil-indexed destination seed on LT-CHENIERE**
- Cheniere's destination optionality now includes an oil-indexed Asian option alongside the gas hubs:
  - `destinations: ['JKM', 'TTF', 'NBP', { idx:'Brent', slope:0.115, window:3, lag:0, constant:0 }]`
- Interpretation: Cheniere cargoes can be diverted to a Japanese buyer priced at 11.5% × Slope(3,0,1), or sold against any of the European/Asian hubs. Margrabe/Clark picks the max-netback destination.
- Optionality ledger now renders `JKM / TTF / NBP / 11.5%×Brent(3,0,1)` in the Underlying column.
- Both the ledger `fmtDest` and the CSV export use the same label format.

**Oil-indexed destinations — done (with full (x,y,z) pricing window)**
- `valueInstance` destination branch accepts two shapes:
  - **String** (`'JKM'`, `'TTF'`) — gas hub, no averaging, no premium
  - **Object** `{ idx, slope, scalar?, window?, lag?, constant? }` — oil-indexed with SPA-style averaging
- Formula: `LNG(M) = scalar × slope × avg(idx over [M−lag−window+1 … M−lag]) + constant − freight`
- **(x, y, z) mapping:**
  - `x` = `window` (months of Brent/JCC averaged)
  - `y` = `lag` (end of pricing period is `lag` months before delivery month; `lag=0` means pricing ends at the delivery month itself)
  - `z` = months the computed price is valid for. Handled at the contract level — `valueInstance` operates on a single delivery month, so `z` doesn't appear in the params here. Grouping cargoes to share one computed price happens upstream when you build the instances.
- Example: `11.5% × Slope(3,0,1)` for Jun-26 delivery → `0.115 × avg(Brent Apr-26, May-26, Jun-26) + constant`. Spec: `{ idx:'Brent', slope:0.115, window:3, lag:0, constant:0 }`.
- The **`scalar`** field supports Japanese/Korean LTSA-style pricing like "94.5% × Slope" (scalar on top of the negotiated slope). Spec: `{ idx:'Brent', slope:0.115, scalar:0.945, window:3, lag:0 }` → `0.945 × 0.115 × avg(…)`. Default `scalar=1`.
- Vols are scaled by `|slope × scalar|` so Margrabe/Clark sees LNG-equivalent vol
- Labels auto-format as `11.5%×Brent(3,0,1)+0.50` or `94.5%×11.5%×Brent(3,0,1)` for readability
- Key helpers: `shiftMonth(monthStr, delta)` and `averageOilIndex(deliveryMonth, idx, window, lag, date?)`

**Group-by-deal toggle in Optionality — done**
- Pill toggle "Per instance | By deal" next to the filter pills
- "By deal" aggregates children into their parent dealId (via `inst.parentDealId`) — e.g., 360 Cheniere rows → 1 `LT-CHENIERE` aggregate row
- Aggregated row shows: combined flex pill pills, count of cargoes and count of flex rights, rolled TBtu, summed intrinsic/extrinsic/total
- Drill-down detail card is hidden in deal view (instance selection doesn't apply)

**Scenarios tab — done**
- 6 preset shocks (European winter, Asian heat wave, supply surplus, oil spike, oil crash, TTF-JKM basis blow)
- Custom shock grid — one row per index (12 rows), absolute Δ in $/MMBtu or $/bbl (oil)
- Shock layer: `setCurveShocks(map)` installs a global override inside `curveLookup`; `setCurveShocks(null)` removes it. No state mutation.
- `runScenario()` pipeline: capture base MTM → install shocks → recompute → diff → **always revert shocks** (critical — tested for leak-through)
- KPI strip (Base, Shocked, Δ, Worst cargo, Best cargo)
- Per-cargo impact table sorted worst-to-best Δ

## 6. Forward backlog (nothing blocking, nice-to-haves)

These are genuinely optional now. The app is a complete trader workflow.

**Nice-to-haves in rough priority:**

1. **LT roll-forward mechanics** — today the full tenor is materialized at seed time. If we want "time advances" behaviour (cargo ages from Fixed → Actualized once its month is past), we'd need a scheduler or a "Roll to today" button. Low urgency since full-tenor is what the user asked for.
2. **Search bar** on Cargo + Paper tables (>30 rows gets painful to eyeball-scan)
3. **Keyboard shortcuts** — Cmd+S save draft, Cmd+Enter book, Esc cancel, `/` focus search
4. **Bulk ops** — multi-select rows → bulk status change / delete / clone
5. **Inline validation** — red-highlight invalid fields instead of toast-only
6. **Per-cargo edit history** — last 5 versions with revert
7. **Drawdown band on the P&L chart** — shade peak-to-trough regions
8. **Scenario: apply permanently** — right now Scenarios is read-only. If the user wants "apply this shock to the active snapshot" we'd add a button that does `updateCurveCell` in a loop.
9. **Correlation-linked shocks** — auto-fill related hubs (TTF +$5 → PEG/PSV/ZTP +$5 via correlation matrix). Currently presets do this manually.

**Not doable in this prototype (backend required)**: multi-user concurrency, authentication, shared state, audit trail, permissions, real-time curve push, email/push notifications. These are for the LNG TradeOS integration phase.

---

## 7. Code map (grep patterns, not line numbers — line numbers shift)

| What | Grep pattern |
|---|---|
| State schema + defaults | `function defaultState` / `DEFAULT_SETTINGS` |
| State mutations | `function bookTrade` / `function bookCargo` / `function setSetting` |
| Seed data | `SEED_TRADES` / `SEED_CARGOES` / `SEED_PARENT_DEALS` / `SEED_CURVES_22_APR_2026` / `SEED_FREIGHT_22_APR_2026` |
| Parent deal mutations | `function bookParentDeal` / `function deleteParentDeal` / `function undoDeleteParent` / `function generateChildrenForParent` |
| Curve lookup + snapshots | `function curveLookup` / `function curveLookupDetailed` / `function updateCurveCell` / `listCurveSnapshots` |
| Scenario shock layer | `_CURVE_SHOCKS` / `function setCurveShocks` (curveLookup adds shock[idx] when set) |
| Freight lookup + route | `function freightLookup` / `function routeBasin` / `function defaultFreightRoute` |
| Cargo math | `function computeCargoValues` / `function cargoLoadedCbm` |
| Voyage calc | `function calcVoyage` (reads from `STATE.settings` via `V` and `F` locals) |
| Hedge exposure | `function computeHedgeExposure` / `function periodToMonths` |
| Physical exposure | `function computePhysicalExposure` |
| Option pricing | `function margrabeExchange` / `function bachelierCall` / `function bachelierPut` |
| Oil-indexed destinations | `function valueInstance` — see `normDests` in the `dest` branch |
| (x,y,z) slope averaging | `function averageOilIndex` / `function shiftMonth` |
| z-aware pricing anchors | `function pricingAnchorMonth` / child `buyPricingAnchor` + `sellPricingAnchor` |
| Cargo form preview | `function cargoPreview` |
| Cargo CRUD handlers | `function handleBookCargo` / `function editCargo` / `function clearCargoForm` |
| Paper CRUD handlers | `function handleBookTrade` / `function editTrade` / `function clearPaperForm` |
| Cargo table | `function renderCargoTable` |
| Paper table | `function renderPaperTable` |
| Morning Book | `function renderMorningBook` |
| Exposure grid | `function renderExposureGrid` |
| Spec Book | `function renderSpecBook` |
| Optionality | `function renderOptionality` / `function valueInstance` / `optGroupBy` flag |
| P&L Log | `function renderPnlLog` / `function drawPnlChart` / `function capturePnlSnapshot` / `getPnlHistory` |
| Scenarios | `function runScenario` / `function loadScenarioPreset` / `SCENARIO_PRESETS` / `SCENARIO_CUSTOM` |
| Curves tab | `function renderCurvesTab` / `function parseCurvePaste` |
| Freight tab | `function renderFreightTab` / `function parseFreightPaste` |
| Settings tab | `function renderSettingsTab` / `function onSettingEdit` |
| CSV export | `function csvCell` / `function downloadCSV` / `function exportCargoCSV` etc. |
| Toast | `function toast` |
| Tab switching | `function switchTab` |

Port costs and NM database are in `const PORT_COSTS` and `const NM_DB` — extracted from the user's Port Cost Database 3.0 and Freight Pricing Model.

---

## 8. Testing approach

Run:
```bash
node outputs/verify.js
```

The suite uses JSDOM with `url: 'https://lng-tradeos.local/'` (not `about:blank`) so localStorage works. Current state: **23 sections, 160 checks, all passing.**

What's covered:
1. State bootstrap + schema v7 (parent deals, paper trades, curves, settings all present)
2. Parent deals + children (4 parents, 392 total children, correct formulas inherited)
3. Child cargo inheritance (coverage, counterparty, formula propagation)
4. Cargo MTM math by incoterm/coverage (all 4 incoterms, unsold + uncovered + covered)
5. Delete parent cascades all children + undo restores all
6. Paper Book CRUD + quarterly hedge decomposition (Q3-26 → Jul/Aug/Sep split)
7. LNG curve lookup + re-mark propagation to cargoes
8. Freight lookup + route classifier (4 routes)
9. Settings propagation (HFO $ → voyage cost) + reset
10. Optionality math (Margrabe = 3.91 per Hull 36.1, Bachelier ATM identity)
11. Morning Book KPIs (live from state, no hardcoded numbers)
12. Cargo table grouping (parent rows collapsed/expanded, window filter, far-tenor fallback)
13. P&L Log (30-day seed, chronological, KPIs populated, range toggle, EOD snapshot appends)
14a. Optionality group-by-deal toggle (364 instances → 4 parent deal rows)
14. CSV export builders (cargo / paper / spec / optionality / exposure)
15. Scenarios (shock layer reverts cleanly, runScenario populates impact table, shocks don't leak into main book, presets load + run)
16. Oil-indexed slope averaging (shiftMonth arithmetic, (3,0,1) = avg(M-2, M-1, M), (3,1,1) = avg(M-3, M-2, M-1), single-month (1,0,1) identity, end-to-end valueInstance label format, scalar×slope compound)
17. Cargo buy/sell legs with (x,y,z) averaging (buy leg (3,0,1) on Brent = 11.5% × avg(Apr,May,Jun); scalar×slope on the buy leg; lag=1 shifts window; legacy cargoes MTM unchanged)
18. Seeded oil-indexed destination on LT-CHENIERE renders live in the Optionality ledger
19. z-aware pricing period grouping (z=3 aligns May/Jun/Jul-26 under anchor Jul-26; same bucket → identical price; next bucket differs; formula = 0.945 × TTF(anchor) − 2.91)
20. Krk port data (Sabine → Krk = 5,381 NM, $242k discharge cost, laden days ≈ 13.59 at 16.5 kn)
21. Pre-merge cleanup (exposure honours scalar+window+lag, cargo-table formula renders (x,y,z)+scalar, editCargo round-trips extended fields, CSV exports the new columns, Brent shock flows through (3,0,1) averaging)
22. Per-destination freight + holder field + COGH route + FOB Premium calculator (per-dest freight overrides basket fallback, holder=cpty flips sign, COGH > Panama in NM and cost, FOB Premium = extrinsic only, basis re-expression in any reference index)
23. FOB-liquid load ports + Suez removal + authoritative Oman/COGH data + live preset freight (Oman LNG at $126,637 port cost with full NM table 4,176→Tokyo, 729→Dahej, 14,500→NWE COGH; Sabine COGH alternatives at user data 15,379→Tokyo etc.; Suez Canal removed everywhere — no LNG transits since Red Sea conflict; FOB Premium presets now compute freight live from calcVoyage so swapping load port auto-updates the basket — Sabine→Tokyo=$2.07 vs Oman→Tokyo=$0.96 verified end-to-end)

Every new feature should extend `verify.js` with a new section. Follow the existing pattern.

### JSDOM gotchas
- **`url: 'https://...'`** is required. `about:blank` is an opaque origin → `localStorage` throws SecurityError.
- `let`/`const` top-level vars aren't on `window`. Use getter functions (see §4 "Getters for JSDOM testability").
- `window.scrollTo()` logs "Not implemented" — harmless, ignore.

### Math reference values (do not break)
- `margrabeExchange(30, 28, 0.30, 0.40, 0.30, 1)` ≈ `3.909` (Hull Example 36.1 = 3.91)
- Bachelier ATM: `bachelierCall(F, F, σ, T) === bachelierPut(F, F, σ, T) === σ√T/√(2π)`
- `extrinsic ≥ 0` for every optionality instance under any positive vol (option value is never negative)
- +20% vol → extrinsic monotonically increases for every instance

---

## 8. Gotchas I've stepped on

1. **Unit mismatch** — `tradePnL()` returns raw $ but `computeCargoValues().mtm` returns $M. Morning Book combines them via `physMtm * 1e6 + hedgePnl + specPnl` in raw $. A subtle bug if you forget the conversion.

2. **Brent and JCC as destinations** — they're priced in $/bbl, not $/MMBtu. The destination valuation handles this via the `{idx, slope, scalar?, window?, lag?, constant?}` object shape in `valueInstance`. Pricing window follows the SPA `(x,y,z)` convention: `x` months averaged, ending `y` months before delivery. `scalar` supports compound multipliers like `94.5% × 11.5%`. String destinations (gas hubs) still work and default to `slope=1, scalar=1, window=1, lag=0`. Don't pass `'Brent'` as a bare string — always the object shape for oil-indexed, with at minimum an explicit `slope`.

3. **Quarterly contracts in the grid** — a `Q3-26` hedge isn't directly in `MONTH_ORDER` (which only has monthly columns). `periodToMonths('Q3-26')` returns `['Jul-26', 'Aug-26', 'Sep-26']`. The hedge exposure aggregator splits TBtu 1/3 across those months. Same logic for Cal (1/12 across the year).

4. **Edit mode vs new mode** — Cargo Book, Paper Book both have this pattern. `currentEditingCargoId` / `currentEditingTradeId` globals. When non-null, Book button label changes to "Update", Delete button appears. Always reset on Cancel + after successful book.

5. **Tab-click re-render** — listeners on `data-tab="..."` buttons trigger render for that tab. Add one for any new tab.

6. **STRIP_STATE is module-level mutable** — it's the month-picker selection for Strip mode. Make sure you reset when switching cargoes or deal types, otherwise stale months carry over.

7. **Freight mode is global** — `freightMode = 'manual' | 'voyage'` is a top-level var. When editing a cargo, call `setFreightMode(c.freightMode)` to sync the UI.

8. **Settings writes don't update legacy consts** — the old `VESSEL` and `FUEL` consts are still in the file as fallbacks. `calcVoyage` reads from `STATE.settings.vessel` via a local `V = STATE.settings?.vessel || VESSEL`. Don't mutate the consts; always go through `setSetting()`.

9. **Dark mode** — most CSS is themed but a few inline-styled elements (toast, notes boxes) look slightly off in dark. Acceptable; fix if you touch those areas.

---

## 9. Definition of done for each priority

### ✓ Strip/LT first-class — DONE (v7)
All items in the prior checklist are complete. The Cargo Book now ships with 4 parent deals (2 LT SPAs, 2 strips) and 80 generated child cargoes. Parent/child grouping in the table, cascade delete with undo, and Optionality reads flex from children automatically.

### Export buttons — DONE (all 5 tabs wired, UTF-8 BOM, verified in Section 14)

### P&L Log — DONE (v7, 30-day seed, canvas chart, verified in Section 13)

### Scenarios — DONE (6 presets + custom grid, shock layer via `setCurveShocks`, verified in Section 15)

### Oil-indexed destinations — DONE (slope + constant shape accepted in `valueInstance`)

### Group-by-deal toggle — DONE (Optionality tab · pill toggle · 364 instances → 4 deal rows)

---

## 10. Philosophical note

This prototype is a **spec**, not a product. When LNG TradeOS integration starts, the UI is re-skinned on their framework and the storage layer is replaced with REST calls. What transfers is:

- **Schema** — Cargo, Trade, Flex, Curves, Freight, Settings shapes
- **Math** — EDQ-based P&L, Margrabe, Bachelier, ETS ramp, hub→index routing, vessel×fill loaded volume
- **Decomposition** — Revenue / Supply / Shipping, Floating / Treasury split
- **UX** — incoterm as first-class, flex editor per cargo, vessel-size+fill input, snapshot-based curve re-marking

Every line of code should aim to make those four transferable. Clean math, clean state shape, commented decisions. Imperative DOM code tightly coupled to this HTML is acceptable only for rendering; business logic should be in pure functions you can lift straight into the LNG TradeOS backend.

---

## 11. Current metrics

```
Total lines of HTML+JS+CSS:    ~9,460
Total JS (inline):             ~486 KB
Schema version:                v7
Parent deals (seeded):         4 (LT-EG-SPA, LT-CHENIERE, STR-KRK-27Q, STR-GATE-26)
Child cargoes (seeded):        392 (full tenor: 120 + 240 + 8 + 24)
Paper trade count (seeded):    6 (2 hedges, 4 specs)
Index catalog:                 12 (JKM, TTF, NBP, HH, Brent, JCC, PEG, PSV, ZTP, THE, PVB, TFU)
Curve snapshots:               2 (today + synthetic baseline)
Freight snapshots:             2
P&L history:                   30 days seeded
Scenario presets:              6 (winter tightness, Asia heat, supply surplus, oil ±$20, basis)
Settings fields (editable):    18 vessel + 4 fuel + 3 other + 1 limit = 26
Tabs live:                     11 (Morning, Cargo, Paper, Exposure, Spec, P&L Log, Curves, Freight, Scenarios, Optionality, Settings)
Tabs stub:                     0
Verify.js sections:            23, 160 checks passing
```

---

Good luck. When in doubt, extend the verify.js regression suite before shipping.
