# L1-H4B Liquidity Gate Plus Capped Size Adjustment Mean Reversion

## Scope
L1-H4B is the direct follow-up to L1-H4A.
It preserves the L1-H2 SessionVWAP fade family and the L1-H4A liquidity gate, then adds only one overlay: deterministic capped size adjustment.

## Why this follows H4A
H4A first tests whether a spread-proxy liquidity gate adds value by suppressing entries in poor-liquidity regimes.
H4B then tests whether exposure should also be reduced *inside still-allowed regimes* when current spread proxy is worse than a rolling reference.

## Base family and clocks (locked)
- Canonical input data: `1m` OHLCV.
- Signal timeframe default: `5m`.
- Exit monitoring timeframe: `1m`.
- Signal indicators and entries: completed `5m` bars only.
- Stop and VWAP-touch exits: monitored on `1m` bars.
- Time stop: counted in completed signal bars.

## Preserved L1-H2 entry/exit family
- Entry primitive: SessionVWAP mean reversion.
- `z_vwap_t = (close_t - SessionVWAP_t) / ATR_14_t`.
- Long entry when `z_vwap_t <= -z0`.
- Short entry when `z_vwap_t >= z0`.
- Frozen ATR stop at entry (`k_atr * ATR`).
- VWAP-touch profit exit.
- Time stop in signal bars.
- No pyramiding and no same-direction add-ons.

## Liquidity proxy and gate (from H4A)
- `spread_proxy_t = 0.5 * (high_t - low_t) / close_t`.
- Gate threshold is a trailing quantile over past-only history.
- `liq_gate_t = spread_proxy_t <= quantile(history_{t-L..t-1}, q_liq)`.
- Entries are allowed only when `liq_gate_t = True`.

## New H4B size adjustment
Inside gate-allowed states, the strategy computes:

- `spread_proxy_ref = rolling_median(spread_proxy_history_{t-L..t-1})`
- `size_factor_t = min(1.0, max(cap_multiplier, spread_proxy_ref / spread_proxy_t))`
- `qty_adj = qty_base * size_factor_t`

Where `qty_base` remains the baseline constant-R sizing quantity.

Locked bounds:
- `size_factor_t <= 1.0`
- `size_factor_t >= cap_multiplier`

## Required entry logging fields
At entry, H4B records at minimum:
- `spread_proxy_t`, `spread_proxy_ref`, `liq_gate_t`, `q_liq`, `q_threshold_t`
- `cap_multiplier`, `qty_base`, `size_factor_t`, `qty_adj`
- `session_vwap_t`, `z_vwap_t`, `entry_reason`
- `signal_timeframe`, `exit_monitoring_timeframe`
- execution assumptions snapshot where available

## Falsification discipline
Reject H4B baseline if:
- survivability does not improve versus H4A gate-only, or
- realized cost drag does not improve in poorer liquidity buckets, or
- size factor seldom activates in the intended regimes.

## Important interpretation boundary
H4B remains a proxy-based liquidity hypothesis.
It does not observe true executable quote spread directly and must be evaluated against realized slippage and cost attribution.
