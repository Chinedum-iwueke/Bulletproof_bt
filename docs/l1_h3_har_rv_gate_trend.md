# L1-H3 HAR-style Realised-Vol Forecast Improves Gates and Stop Distances

## Scope (locked first production version)

L1-H3A is **HAR-gated L1-H1** only:
- signal timeframe: `15m`
- base engine data: canonical `1m`
- entry direction logic: L1-H1 (`sign(EMA20 - EMA50)`)
- replacement components only:
  - ATR percentile gate -> HAR `RV_hat` percentile gate
  - ATR stop distance -> `k * close_t * sqrt(RV_hat_t)` frozen at entry

No ML, no L1-H2 integration, no trailing stop, no extra TP model.

## Realised-vol definitions

On signal bars:
- `rv1_t = (ln(close_t / close_{t-1}))^2`
- `RV_d = mean(rv1 over last 1 calendar day)`
- `RV_w = mean(rv1 over last 7 calendar days)`
- `RV_m = mean(rv1 over last 30 calendar days)`

For 15m bars:
- bars/day = `96`
- `RV_d` window = `96`
- `RV_w` window = `672`
- `RV_m` window = `2880`

## HAR model and fit discipline

Forecast:
- `RV_hat = a + b*RV_d + c*RV_w + d*RV_m`

Fit discipline (locked):
- deterministic OLS
- rolling fit windows: `{180, 365}` calendar days
- coefficients fit on **past-only** observations prior to decision timestamp
- refit cadence: once per completed signal day
- warmup requires: `RV_m` availability + sufficient training observations for OLS + gate history

## Gate rule

- Compute causal rolling percentile of `RV_hat_t` vs trailing history
- allow entry iff `rvhat_pct_t >= gate_quantile`
- first baseline remains high-vol trend gate family (same directional family as L1-H1)

## Stop and hold semantics

At entry:
- `stop_distance = k * close_t * sqrt(RV_hat_t)`
- stop_distance and stop_price are frozen at entry
- no trailing/widening/tightening after entry

Time stop:
- unchanged from L1-H1
- `T_hold` counted in completed signal bars, not 1m bars

## Two-clock semantics

- Signal indicators/entries on completed 15m bars only
- Stop and time-stop monitoring remains responsive on base 1m path

## Artifacts and audit outputs

Per run, strategy artifacts include:
- `har_coefficients.json`: fit timestamps, `a,b,c,d`, fit window days, train spans
- `har_split_manifest.json`: walk-forward fit discipline metadata and fit rows

Decision metadata includes:
- `RV_hat_t`, `rvhat_pct_t`, `gate_pass`, `fit_ts_used`, `fit_window_days`, `trend_dir_t`, `stop_distance`

## Falsification and failure modes

Reject baseline if:
- out-of-sample `EV_r_net` does not improve vs L1-H1 and sensitivity increases.

Expected failure modes:
- overfitting if refit cadence is loosened (locked out in baseline)
- structural breaks where fixed-window coefficients stale rapidly.
