# L1-H2 Compression Regime Favours Mean Reversion

## Hypothesis
L1-H2 tests whether mean-reversion fades against **SessionVWAP** have positive net expectancy during low-realized-volatility compression regimes.

## Locked baseline semantics
- **Canonical base data:** `1m` OHLCV.
- **Default signal timeframe:** `5m`.
- **Two-clock model:** entries/indicators on completed `5m` bars, risk/profit exits monitored on `1m` bars.
- **VWAP primitive:** `SessionVWAP` (`vwap_mode=session`) by default.
- **Stop model:** fixed ATR multiple frozen at entry (`stop_update_policy=frozen_at_entry`).
- **Profit exit:** VWAP touch only (`profit_exit_model=vwap_touch`).
- **Time stop unit:** completed signal bars (`hold_time_unit=signal_bars`).
- **Position discipline:** no pyramiding and no adding to losers in baseline.

## Signal definitions
On each completed signal bar:

- `rv_t = ATR_14_t / close_t`
- Compression gate threshold is trailing quantile over the prior 30 calendar days of signal bars:
  - `comp_gate_t = rv_t <= quantile(rv_{t-L..t-1}, q_comp)`
- `z_vwap_t = (close_t - SessionVWAP_t) / ATR_14_t`

Entry gating:
- allow entries only when `comp_gate_t == True`
- long fade if `z_vwap_t <= -z0`
- short fade if `z_vwap_t >= z0`

Warmup policy:
- no entry until ATR, SessionVWAP, and full 30-day compression history are available.

## Exit model
### 1) Frozen ATR stop
At entry:
- `atr_entry = ATR_14` from signal timeframe
- `stop_distance = k_atr * atr_entry`
- `stop_price` is set once at entry and never moved

There is no trailing, chandelier, or dynamic volatility stop behavior in baseline L1-H2.

### 2) Responsive VWAP-touch exit on base clock
While in position, the strategy maintains a **base-stream SessionVWAP** from incoming `1m` bars and exits when:
- long: `close_1m >= SessionVWAP_1m`
- short: `close_1m <= SessionVWAP_1m`

This keeps execution causal and responsive without waiting for the next `5m` signal close.

### 3) Time stop
`T_hold` is counted in completed signal bars (not 1m bars):
- on `5m`, `T_hold=12` means 12 completed 5m bars
- on `15m`, `T_hold=12` means 12 completed 15m bars

## Parameter grid
- `q_comp ∈ {0.20, 0.30}`
- `z0 ∈ {0.8, 1.0}`
- `k_atr ∈ {1.5, 2.0}`
- `T_hold ∈ {12, 24}`

## Required logged artifacts
Entry metadata includes:
- `rv_t`, `q_comp`, `comp_gate_t`
- `vwap_t`, `z_vwap_t`, `entry_reason`
- `atr_entry`, `stop_distance`, `stop_price`
- `signal_timeframe`, `exit_monitoring_timeframe`, `vwap_mode`

Trade outputs continue to carry existing MFE/MAE and cost attribution fields.

## Known failure modes
- SessionVWAP vs cumulative VWAP mismatches can generate non-portable edges.
- Compression filters can select pre-breakout regimes where fades are penalized.
- Tier2/Tier3 spread and fee frictions can degrade mean-reversion EV substantially.
