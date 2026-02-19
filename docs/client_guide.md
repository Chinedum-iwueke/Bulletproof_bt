# Client Guide (Start Here)

## 1) Pick your data mode and layout
- **Single-file quickstart**: pass a `.csv`/`.parquet` file.
- **Production-style dataset-dir**: use `manifest.yaml` + per-symbol parquet files and `data.mode: streaming`.

Repo Evidence: `src/bt/data/load_feed.py::load_feed`, `docs/dataset_contract.md`.

## 2) Pick execution tier
- Default baseline: `execution.profile: tier2`
- Low-friction: `tier1`
- Conservative: `tier3`
- Fully explicit assumptions: `custom`

Repo Evidence: `src/bt/execution/profile.py::_BUILTIN_PROFILES`.

## 3) Pick safe vs strict risk pack
- Beginner: `configs/examples/safe_client.yaml`
- Research/production discipline: `configs/examples/strict_research.yaml`

Repo Evidence: `configs/examples/safe_client.yaml`, `configs/examples/strict_research.yaml`.

## 4) Run a baseline backtest
```bash
python scripts/run_backtest.py \
  --data data/curated/sample.csv \
  --config configs/engine.yaml \
  --override configs/examples/safe_client.yaml
```

## 5) Where to find results
In the created run directory:
- `summary.txt` for human-readable overview
- `run_status.json` for pass/fail + diagnostics
- `performance.json` for machine-readable metrics
- `trades.csv`, `fills.jsonl`, `equity.csv` for detailed reconciliation

Repo Evidence: `src/bt/logging/run_contract.py`, `src/bt/logging/summary.py::write_summary_txt`, `src/bt/metrics/performance.py::write_performance_artifacts`.

## 6) Common fixes
- **`execution.profile=tier2 forbids overrides`** -> switch to `execution.profile: custom` if overriding execution fields.
- **dataset-dir + dataframe mode error** -> set `data.mode: streaming`.
- **UTC/non-monotonic timestamp errors** -> normalize to UTC and strictly increasing per symbol.
- **strict stop errors (`risk_rejected:stop_unresolvable:*`)** -> supply explicit stop price/spec, or use safe pack for onboarding.

Repo Evidence: `src/bt/execution/profile.py::resolve_execution_profile`, `src/bt/data/load_feed.py::load_feed`, `src/bt/data/symbol_source.py::_validate_row`, `src/bt/risk/reject_codes.py`.
