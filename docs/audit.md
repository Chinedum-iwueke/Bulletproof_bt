# Stability Audit Harness

Enable with config:

```yaml
audit:
  enabled: true
  level: full      # basic|full
  max_events_per_file: 5000
  determinism_check: true
```

When enabled, the runner writes `run_dir/audit/` artifacts:

- `data_audit.json`
- `resample_audit.jsonl`
- `signal_audit.jsonl`
- `order_audit.jsonl`
- `fill_audit.jsonl`
- `position_audit.jsonl`
- `portfolio_audit.jsonl`
- `alignment_audit.jsonl`
- `determinism_report.json` (when determinism check enabled)
- `stability_report.json`

Each event includes `run_id`, `config_hash`, and violation context.

Overhead is near-zero when `audit.enabled: false` because hooks short-circuit before writing.
