# State Feature Layer (Phase 9)

The state layer computes causal `entry_state_*` features per symbol/timestamp using past-only rolling windows.

Highlights:
- Trend: EMA fast/slow, slopes, relationship.
- Volatility: ATR, ATR%, realized vol percentiles.
- Liquidity: spread proxy percentile, volume/dollar volume, liquidity regime.
- Displacement: true range, `tr_over_atr`, displacement regime.
- CSI proxy (no aux feeds required):
  - `csi_proxy = 0.35*vol_pctile + 0.35*tr_over_atr_pctile - 0.30*spread_proxy_pctile` clipped to `[0,1]`.
- Readiness flags: `entry_state_trend_ready`, `entry_state_vol_ready`, `entry_state_liquidity_ready`, `entry_state_csi_ready`, `entry_state_htf_ready`.

No-lookahead rule: percentiles and rolling metrics are computed using only rows at or before `ts`.

## Enriched Derivatives State Features

The state layer accepts OHLCV-only inputs and enriched derivatives panels. OHLCV-only inputs continue to emit `entry_state_csi_source = "ohlcv_proxy"`.

When source columns exist, the layer also emits:

- Funding: `entry_state_funding_raw`, `entry_state_funding_pctile`, `entry_state_funding_z`, `entry_state_funding_regime`.
- OI: `entry_state_oi_level`, `entry_state_oi_change`, `entry_state_oi_change_pct`, `entry_state_oi_accel`, `entry_state_oi_accel_pctile`, `entry_state_oi_z`, `entry_state_oi_regime`.
- Mark/index/basis: `entry_state_mark_price`, `entry_state_index_price`, `entry_state_basis_raw`, `entry_state_basis_pct`, `entry_state_basis_pctile`, `entry_state_premium_raw`, `entry_state_premium_pctile`, `entry_state_basis_regime`.
- Crowding/stress: `entry_state_crowding_proxy_pctile`, `entry_state_constraint_stress_pctile`.
- CSI metadata: `entry_state_csi_source`, `entry_state_csi_components_json`, `entry_state_csi_components_available_json`.

Accepted aliases include `funding`, `oi`, `mark_price`, `index_price`, `basis`, `basis_pct`, `premium`, and `premium_pct`. If `available_at`, `funding_available_at`, `oi_available_at`, `mark_available_at`, or `index_available_at` exists, it must be less than or equal to the decision bar timestamp.
