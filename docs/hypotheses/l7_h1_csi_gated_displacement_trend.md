# L7-H1 — CSI‑Gated Displacement Trend

**Hypothesis (one sentence):** Large trend outliers occur predominantly when a composite Constraint-forcing Signal Index (CSI) indicates proximity to constraint-forced flow and a first displacement bar appears.

**Exact testable statement:** Condition on signal bars where D_t >= d0 (displacement measured as TR / ATR_14(t-1)) and CSI >= θ. The hypothesis claims that, conditional on these gates, the distribution of returns over the next H signal bars has a heavier right tail (higher incidence of multi‑ATR wins) compared to baseline conditional samples matched by volatility and symbol-tier. Specifically, Tier2 must show positive EV_r_net and a non-zero rate of >=10R wins for promotion; Tier3 should preserve max‑R under stress sampling.

Data: Use research_data sets containing `ohlcv`, `mark`, `index`, `funding`, `oi`, `volume`.

Signal construction:
- ATR_14 computed on closed signal bars (period = 14).
- Displacement D_t = TR_t / ATR_14(t-1); require D_t >= d0 to qualify as a displacement bar.
- CSI composed of funding percentile (fallback: basis percentile), OI z-score acceleration (fallback: volume z), D_t, and ranked spread proxy S_t = 0.5*(high-low)/close. CSI normalized to [0,1].

Entry/exit logic:
- Entry: at bar close when D_t >= d0 and CSI >= θ, go long if displacement positive, short if negative.
- Exit: chandelier trailing stop using `k_trail*ATR` and fixed initial stop `k_stop*ATR`; exhaustion exit if CSI drops below `theta_low` for `N` bars.

Sizing:
- Constant‑R sizing: risk_amount_t = equity_t * r_per_trade; stop_distance = k_stop * ATR_14(t-1); qty = risk_amount_t / stop_distance.

Evaluation & logging:
- Required logging fields mirror the hypothesis YAML; include CSI, all components, state_vector for ML, entry_reason, trailing stop evolution, and complete trade-level R-series.
- Metrics: count >=10R, max R, right-tail CCDF slope, Tier2 EV, drawdown duration, capacity proxy (notional/dollar_volume).

Falsification criteria:
- Tier2 shows ~0 trades >=10R across sample → CSI not capturing forced flow.
- Winners fully explained by ATR spikes without CSI signal → CSI is only volatility proxy.

Notes:
- Ensure funding timestamps are aligned to signal timeframe to avoid leakage.
- Use index-based basis when funding series absent; require spot/index availability for that symbol.

References:
- See hypothesis YAML: [research/hypotheses/l7_h1_csi_gated_displacement_trend.yaml](research/hypotheses/l7_h1_csi_gated_displacement_trend.yaml)
