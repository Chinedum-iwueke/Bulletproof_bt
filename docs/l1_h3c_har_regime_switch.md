# L1-H3C HAR-based Regime Switch Between L1-H1 and L1-H2

## Scope
L1-H3C is a **switch**, not a blend:
- `rvhat_pct_t >= q_high` → activate L1-H1 trend branch.
- `rvhat_pct_t <= q_low` → activate L1-H2 mean-reversion branch.
- otherwise → neutral, no trade.

No simultaneous branch activation is allowed for the same symbol and decision event.

## Locked semantics
- Canonical input data remains `1m`.
- Execution monitoring / stop checks remain `1m`.
- Multi-timeframe branch model:
  - trend branch signal clock = completed `15m` bars.
  - mean-reversion branch signal clock = completed `5m` bars.
- HAR allocation state is computed on completed `15m` bars only and held constant between 15m updates; intervening 5m decisions consume the latest completed 15m allocation state.

## HAR process
- `rv1_t = (ln(close_t / close_{t-1}))^2`
- Features on the HAR basis clock:
  - `RV_d`: trailing 1 calendar day mean of `rv1`.
  - `RV_w`: trailing 7 calendar day mean of `rv1`.
  - `RV_m`: trailing 30 calendar day mean of `rv1`.
- Forecast: `RV_hat_t = a + b*RV_d + c*RV_w + d*RV_m`
- Fit discipline:
  - deterministic rolling OLS
  - fit windows `{180d, 365d}`
  - refit cadence `daily_on_completed_signal_day`
  - coefficients are fit on prior-only rows
  - forecasts use latest prior fit (no leakage)

## Branch logic preservation
- High-vol branch preserves L1-H1 entry family: `trend_dir_t = sign(EMA20 - EMA50)`.
- Low-vol branch preserves L1-H2 entry family: SessionVWAP z-score (`z_vwap_t`) using ATR denominator.
- Exits remain branch-native:
  - trend: L1-H1 time stop + frozen RV_hat stop.
  - mean reversion: L1-H2 time stop + frozen RV_hat stop + SessionVWAP touch exit.

## Stop model
For both branches:
- `stop_distance = k * close_t * sqrt(RV_hat_t)`
- computed once at entry
- frozen for trade life
- never trailed or recomputed

## Required artifacts
L1-H3C emits:
- coefficient history (`a,b,c,d`, fit timestamps, train spans)
- split manifest metadata
- decision metadata:
  - `RV_hat_t`, `rvhat_pct_t`, `regime_label`, `branch_selected`
  - `fit_ts_used`, `fit_window_days`, `stop_distance`
  - branch-native fields (`trend_dir_t`) or (`session_vwap_t`, `z_vwap_t`)
- fill-cost fields remain in downstream trade artifacts
