# Rich State Data Causality Audit

Generated: 2026-05-19 UTC

## Summary

The rich state path now supports OHLCV-only panels and enriched derivatives panels. Strategies still receive normal bars and do not need to compute funding/OI/basis fields themselves. The engine injects `entry_state_*` fields into signal metadata at order time, and `trades.csv` preserves all `entry_state_` dynamic columns.

## Funding

Funding aliases accepted by the loader/state layer:

- `funding_rate`
- `funding`
- `funding_raw`
- `funding_rate_realized`

If `funding_source_ts`, `funding_available_at`, or generic `available_at` exists, the research panel loader rejects rows where that timestamp is greater than the bar timestamp. The online state layer also skips funding updates whose availability timestamp is after the current decision bar.

Funding state is rolling and causal. Percentiles and z-scores are computed from the state window accumulated up to the current bar only.

## Open Interest

OI aliases accepted:

- `open_interest`
- `oi`
- `oi_value`
- `oi_contracts`
- `oi_usd`

If `oi_source_ts`, `oi_available_at`, or generic `available_at` exists, rows after the current bar timestamp are rejected/skipped. OI change and OI acceleration are computed from prior observed OI values unless explicit causal change columns are present.

## Mark / Index / Basis

Mark aliases accepted:

- `mark_close`
- `mark_price`
- `mark`

Index aliases accepted:

- `index_close`
- `index_price`
- `index`

Basis/premium aliases accepted:

- `basis_close_vs_index`
- `basis`
- `basis_pct`
- `mark_index_basis`
- `mark_index_basis_pct`
- `premium_mark_vs_index`
- `premium`
- `premium_pct`

Mark/index candle values are treated as same-bar candle values from the panel. If `mark_available_at` or `index_available_at` exists, values after the bar timestamp are rejected/skipped.

## CSI

OHLCV-only datasets produce:

- `entry_state_csi_source = "ohlcv_proxy"`

Enriched datasets with funding, OI acceleration, or basis components produce:

- `entry_state_csi_source = "enriched"`
- `entry_state_csi_components_json`
- `entry_state_csi_components_available_json`

Missing rich components are skipped and weights are renormalized. No full-sample percentile is used for online decision-time state.

## Remaining Assumptions

- If no availability timestamp exists, a panel column is assumed to have already been causally aligned by the research_data panel builder.
- Funding/OI joins in canonical panels remain responsible for backward as-of construction; this audit enforces and documents the downstream checks.
- Post-run diagnostics may bucket completed trades by full experiment output, but those diagnostics are not fed back into strategy decisions.
