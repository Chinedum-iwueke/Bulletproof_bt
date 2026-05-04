# PERFORMANCE_METRIC_MISMATCH_INVESTIGATION

## Root cause
Primary corruption point is in `src/bt/metrics/performance.py`, where numeric coercion used zero-fill semantics on trade monetary fields. If `pnl_net` was malformed/missing after schema shifts, it was converted to `0.0`, causing `net_pnl`, `ev_net`, and win-rate style fields to become contradictory vs equity and costs.

## Exact bad transformation
- `_coerce_numeric(...).fillna(0.0)` applied to `pnl_net` path in `compute_cost_attribution()` and `compute_performance()`.
- This silently transformed invalid `pnl_net` data into zeros.
- Combined with R fallback logic and possible invalid risk denominators, it could produce huge `ev_r_net` while monetary PnL stayed near zero.

## Affected metrics
`net_pnl`, `gross_pnl`, `ev_net`, `win_rate`, `ev_r_net`, `win_rate_r`, `avg_r_win/loss`, `ev_by_bucket` consistency diagnostics.

## Engine correctness
No strategy/execution logic was changed; investigation indicates corruption is in reporting/post-processing layer.

## Patch applied
- Added forensic auditors:
  - `orchestrator/forensics/audit_run_metrics.py`
  - `orchestrator/forensics/audit_experiment_metrics.py`
- Integrated validation into `scripts/post_run_analysis.py` to write:
  - `analysis/performance_validation.json`
  - `analysis/performance_validation.md`
  - and patch `performance.json.metrics_valid`/error fields.
- Hardened metric compute path to avoid zero-filling malformed `pnl_net` when computing PnL EV/win-rate.

## Verify one run
`python orchestrator/forensics/audit_run_metrics.py --run-dir outputs/<experiment>/runs/<run_id>`

## Audit all runs
`python orchestrator/forensics/audit_experiment_metrics.py --experiment-root outputs/<experiment>`
