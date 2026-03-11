# Post-Run Diagnostics Framework

## Purpose

`post_run_analysis` provides a reusable post-processing layer for serial runs and parallel experiment roots. It generates:

- `summaries/run_summary.csv` (one row per run)
- `summaries/diagnostics/*` machine-readable post-mortem tables
- `summaries/diagnostics_warnings.csv` for missing/partial artifacts

## Inputs

The framework reads existing stable artifacts (no new run artifact contract):

- `run_status.json` (completion state)
- `performance.json` (core metrics)
- `trades.csv` (per-trade outcomes)
- `fills.jsonl` (entry metadata used for grouped diagnostics)
- `config_used.yaml` (strategy params + context)

Completed-run filtering reuses existing `detect_run_artifact_status` semantics (`PASS` + required run artifacts).

## Core Summary Output

`run_summary.csv` includes:

- run identity/context (`run_id`, `hypothesis_id`, `tier`, symbols/timeframe/window)
- core R metrics (`ev_r_net`, `ev_r_gross`, `win_rate`, `avg_r_win`, `avg_r_loss`, `payoff_ratio`)
- risk/path metrics (`max_consecutive_losses`, `max_drawdown_r`, `drawdown_duration`, tail losses)
- post-trade quality (`mfe_mean_r`, `mae_mean_r`, `capture_ratio_mean`)
- recoverable hypothesis parameters (`theta_vol`, `k_atr`, `T_hold`, `q_comp`, `z0`, `fit_window_days`, `gate_quantile`)

## Common Diagnostics

Generated under `summaries/diagnostics/common/` when data is available:

- `conditional_ev_by_vol_bucket.csv`
- `conditional_ev_by_timeframe.csv`
- `conditional_ev_by_symbol.csv`
- `mfe_capture_summary.csv`
- `mfe_capture_by_symbol.csv`
- `cost_drag_summary.csv`

## Hypothesis-Specific Diagnostics

A lightweight registry dispatches additional groupings:

- `L1-H1`: `gate_pass`, `vol_pct_t`
- `L1-H2`: `comp_gate_t`, `q_comp`, `z_vwap_t`
- `L1-H3`: `rvhat_pct_t`, `fit_window_days`

Outputs are placed in `summaries/diagnostics/<hypothesis_id>/`.

## CLI

```bash
PYTHONPATH=src python scripts/post_run_analysis.py \
  --experiment-root outputs/l1_h1_parallel_stable \
  --completed-only \
  --include-diagnostics
```

Useful flags:

- `--runs-glob` (default: `runs/*`)
- `--skip-existing`
- `--completed-only`
- `--include-diagnostics`

## Known limitations

- If a required source artifact is missing, diagnostics are skipped and warnings are emitted.
- Volatility-percentile diagnostics require normalized entry metadata in `fills.jsonl`.
- Timeframe grouping falls back to `unknown` when entry timeframe metadata is unavailable.
