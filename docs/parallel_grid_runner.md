# Parallel hypothesis grid runner (spawn-safe v2)

This repo keeps the existing CLI surface for `scripts/run_parallel_hypothesis_grid.py` unchanged, while strengthening internals for stability and observability.

## What changed

- The process pool now uses `multiprocessing.get_context("spawn")` and `ProcessPoolExecutor(..., mp_context=ctx)`.
- Work is dispatched in deterministic **waves** (chunked scheduling) instead of unbounded submission.
- The runner accepts canonical research panels through `--data-root`, `--data-kind research_panel`, `--exchange`, `--universe`, and `--timeframe`; legacy `--data` remains supported.
- Worker bootstrap now enforces native thread caps by default (without overriding user-provided values):
  - `OMP_NUM_THREADS`
  - `OPENBLAS_NUM_THREADS`
  - `MKL_NUM_THREADS`
  - `NUMEXPR_NUM_THREADS`
  - `VECLIB_MAXIMUM_THREADS`
  - `POLARS_MAX_THREADS`
- Every worker enables `faulthandler` and writes run-local diagnostics.
- Parallel runs build experiment-level shared cache metadata:
  - `summaries/shared_cache_manifest.json`
  - `summaries/precompute_registry.json`
- Workers attach read-only dataset/precompute plans via deterministic signatures and emit run context metadata.

## Shared dataset + precompute behavior

- A deterministic dataset fingerprint is generated from canonical dataset path + file metadata.
- Dataset source mode is logged as one of:
  - `opened_memory_mapped` (single-file parquet)
  - `attached_from_cache` (dataset directory streaming path)
  - `fallback_loaded_normally`
- Parquet streaming now opens through `pyarrow.memory_map(..., "r")` when pyarrow is available.
- Precompute registry uses deterministic cache keys over:
  - dataset identity
  - timeframe
  - family
  - params
  - engine version

## New per-run artifacts

Each run directory can now include:

- `worker.log` (phase checkpoints, timing, pid, memory snapshots)
- `worker_exception.txt` (traceback for normal Python exceptions)
- `faulthandler.log` (fatal crash diagnostics)
- `run_context.json` (run metadata + effective thread caps + cache attach context)

## Scheduler behavior

- Wave size defaults to `2 * max_workers`.
- Each wave is submitted, awaited, finalized, and cleaned (`gc.collect()`) before next wave.
- Parent records richer failure context in `summaries/parallel_failures.json`.

## Research data mode

Use `--data-root research_data --data-kind research_panel` when a grid should read the canonical panel library instead of a curated folder. Stable runs resolve symbols from `research_data/manifests/stable_universe.parquet`; volatile runs resolve active windows from `research_data/manifests/volatile_universe_membership.parquet`.

```bash
python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/tier2/l1_h7c_parallel_stable \
  --manifest outputs/tier2/l1_h7c_parallel_stable/manifests/l1_h7c_high_selectivity_regime_tier2_grid.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --data-kind research_panel \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --max-workers 6 \
  --skip-completed
```

Preflight resolves the data profile before workers launch and fails early for missing manifests, missing panel parquet files, non-UTC timestamps, missing OHLCV columns, or future-dated causal source timestamps.

## Limits / assumptions

- This iteration focuses on robust process orchestration, shared-read dataset planning, deterministic cache signatures, and observability.
- Indicator-family materialization remains conservative to preserve causal/no-lookahead semantics; registry entries are deterministic and auditable, and can be expanded to deeper precomputed series in future work.
