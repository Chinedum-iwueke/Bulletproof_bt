# L1-H9 Post-Run Diagnostics

Diagnostics output root:
`<experiment_root>/summaries/diagnostics/l1_h9/`

Key outputs:
- `h9_trade_diagnostics.csv`
- `breakout_quality_summary.csv`
- `ev_by_breakout_distance_bucket.csv`
- `ev_by_signal_timeframe.csv`
- `continuation_strength_summary.csv`
- `failure_mode_summary.csv`
- `failure_mode_by_variant.csv`
- `runner_capture_summary.csv`
- `runner_capture_by_variant.csv`
- `cost_kill_summary.csv`
- `cost_kill_by_breakout_bucket.csv`
- `cost_kill_by_timeframe.csv`
- `cost_kill_by_symbol.csv`

Failure-mode labels:
- `false_breakout`
- `exhaustion_entry`
- `trend_filter_weak`
- `followthrough_failed`
- `runner_gave_back`
- `time_stop_no_extension`
- `cost_killed`
- `signal_noise`

Canonical R discipline:
- EV / win-loss / payoff / MFE / MAE always read from engine trade outputs (`r_multiple_gross`, `r_multiple_net`, `mfe_r`, `mae_r`).
