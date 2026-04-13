# L1-H7E Flow-Sensitive Runner Variant

Keeps baseline H7 entries and applies flow-sensitive adaptation to runner handling only.

## Grid (12 runs)
- signal_timeframe: {15m, 1h}
- runner_flow_mode: {off, moderate, strict}
- sigma_z_threshold: {0.5, 1.0}

Uses deterministic proxies: rolling sigma z-score, spread proxy, imbalance proxy.
