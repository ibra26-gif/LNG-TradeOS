# LNG TradeOS TODO

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
