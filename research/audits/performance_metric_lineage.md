# Performance Metric Lineage

Primary generation path:
1. Engine writes `trades.csv`, `fills.jsonl`, `equity.csv`.
2. `src/bt/metrics/performance.py::compute_performance()` computes `PerformanceReport` from those artifacts.
3. `src/bt/metrics/performance.py::write_performance_artifacts()` writes `performance.json` and `performance_by_bucket.csv`.
4. `scripts/post_run_analysis.py` now runs `orchestrator/forensics/audit_run_metrics.py` and writes `analysis/performance_validation.*`.

Key lineage:
- `initial_equity`,`final_equity`,`total_return`: `equity.csv` -> `compute_performance`.
- `gross_pnl`,`net_pnl`,`fee_total`,`slippage_total`,`spread_total`: `compute_cost_attribution()` from `trades.csv` (+`fills.jsonl` if present).
- `ev_net`,`win_rate`: trade `pnl_net` series.
- `ev_r_net`,`win_rate_r`,`avg_r_win`,`avg_r_loss`,`profit_factor_r`,`payoff_ratio_r`: `summarize_r()` on `trades.r_multiple_net` (or derived fallback if reported values are inconsistent).
- `ev_by_bucket`,`trades_by_bucket`: currently from PnL buckets (`_bucket_metrics`), not R buckets.
- `performance_by_bucket.csv`: from `report.ev_by_bucket` in `write_performance_artifacts()`.

Observed first bad transformation:
- In `compute_cost_attribution()` and PnL metric path, numeric coercion used zero-fill semantics (`_coerce_numeric(...).fillna(0)`), allowing malformed/non-numeric `pnl_net` to collapse to zeros and produce contradictory `net_pnl`/EV while equity showed losses.
- R fallback (`_resolve_r_multiple_series`) could recompute from `pnl_net / risk_amount`; with invalid/near-zero risk this can create absurd R if source data is damaged.
