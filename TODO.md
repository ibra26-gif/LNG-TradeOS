# LNG TradeOS TODO

## EU Gas Balance Full Rework

Goal: rebuild the EU Gas Balance into a traceable historical S&D model and daily long/short monitor.

- Historical monthly S&D table based on ENTSOG + GIE AGSI/ALSI, with source traceability for every line.
- Current month shown as MTD / YTD-to-date, not full-month fabricated values.
- Supply rows: domestic production, pipeline imports, LNG regas/sendout, storage withdrawal.
- Demand should be implied where sectoral demand is unavailable; do not fabricate power / industry / residential lines.
- Daily chart: supply vs implied demand for the last gas days.
- Daily chart: supply less storage injection / withdrawal to show Europe long or short.
- Unit toggle: bcm, mtpa / mtpm, and mcm/d.
- Keep pipeline import rows explicit; avoid duplicated "UK / Iberia / other" buckets.

## ACER Freshness

- Fix ACER freshness heartbeat and scraper cadence so `data/acer_historical.csv` stays within the configured freshness limit.
- Surface stale ACER data clearly in the UI and GitHub heartbeat.
- Confirm laptop launchd / GitHub workflow ownership so the refresh path is not ambiguous.

## Terminal Utilization By Region

Goal: track LNG terminal utilization by European region, not only terminal-by-terminal.

- Regions: NWE, Mediterranean, Baltic, Iberia.
- Use GIE ALSI/AGSI terminal sendout / stock / capacity data already wired where possible.
- Show utilization history, latest utilization, and stale/missing data flags.
- Add regional aggregation without losing terminal-level drill-down.

## Historical Physical Prices

- Build historical physical differential / physical price evolution.
- Track NWE, Mediterranean, Iberia, UK, JKTC and other existing physical-price anchors.
- Keep the same rule as FOB history: use saved daily snapshots only when inputs are time-aligned.
- Add route/index filters where physical prices feed FOB/netback calculations.

## South America Review

- Review South America analytics sections for data sources, freshness, formulas, and UI clarity.
- Check Argentina and Colombia balance logic, seasonal charts, and source notes.
- Identify missing data gaps before adding new functionality.

## South Korea Analytics Reshuffle

- Restructure South Korea analytics into a clearer workflow.
- Separate price/tariff history, gas balance, LNG imports, inventory, and demand views.
- Keep manual-upload fallbacks only where official automated sources are blocked.

## Portfolio Valuation

- Finish Portfolio Valuation.
- Add functionality to identify which term contracts are in the money or out of the money versus spot.

## Freight Historical + FOB Pricing Historical

- Fix freight historical section and restore any removed historical views.
- Add historical freight, spread, and freight-vs-basis analysis.
- Finish FOB pricing historical so users can select observation date, route, indexation, loading point, and tenor.

## Historical Price Analysis Notifications

Goal: add notifications to the Historical Price Analysis / Financial Trading stack when market structure changes materially.

- Spread closing: alert when selected spread narrows beyond configured daily or rolling threshold.
- Spread widening: alert when selected spread widens beyond configured daily or rolling threshold.
- Correlation changing: alert when rolling correlation breaks from its recent regime.
- Regime changing: alert when volatility/correlation structure crosses a defined regime boundary.
- Outright increasing/decreasing: alert when flat price trend changes materially, not only spread structure.

Implementation notes:
- Use existing EOD store only; no new data source.
- Persist user thresholds and selected instruments in localStorage.
- Frame signals as market-structure observations, not trading advice.
- Add visible "last checked" and source date so stale data cannot trigger fresh alerts.

## USGC Arb

- USGC through COGH should be assessed on M+1 as the front decision tenor, not the current assessment month.
- Keep the full M+1 to M+24 curve for context, but make the M+1 readout explicit.
