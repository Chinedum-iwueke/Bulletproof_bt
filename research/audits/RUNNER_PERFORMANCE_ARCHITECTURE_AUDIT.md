# Runner Performance Architecture Audit

Generated: 2026-05-19 UTC

## Scope

This audit covers the long-running research path:

1. `scripts/build_hypothesis_grid.py`
2. `scripts/run_parallel_hypothesis_grid.py`
3. per-run execution via `bt.experiments.grid_runner.execute_hypothesis_variant`
4. artifact writing under each run directory
5. `scripts/post_run_analysis.py`
6. `scripts/extract_experiment_dataset.py`
7. `scripts/cleanup_experiment_runs.py`
8. `orchestrator/research_daemon.py` and `orchestrator/run_experiment_pipeline.py`

The pass intentionally preserves Bulletproof_bt truth semantics: no lookahead, bar-by-bar execution, no synthetic bars, strict HTF completeness, realistic execution costs, deterministic configs, and full research artifacts.

## Source Of Truth

Every completed run is still required to produce:

- `config_used.yaml`
- `decisions.jsonl`
- `fills.jsonl`
- `trades.csv`
- `equity.csv`
- `performance.json`

`run_status.json` is now treated as an atomic lifecycle marker, not a substitute for those artifacts. Under strict resume, a completed status is not trusted unless required artifacts exist and `performance.json` is valid JSON with no `metrics_valid=false` marker.

## Execution Path Findings

Grid build is mostly CPU-light and deterministic. Its main cost is YAML parsing and manifest expansion. No large market data is loaded here.

Parallel run was the dominant RAM and stability risk. Each worker is a separate process and previously could start without checking free RAM. With research panels, naive loading can amplify per-worker RSS. The research panel loader had already been moved to streaming per-symbol parquet batches; this pass added runner backpressure, strict resume, atomic run status, configurable failure isolation, and progress/memory reporting.

Per-run engine execution remains event driven. Indicators and state features are still updated only as bars arrive. No indicator cache crosses incompatible configs. A configurable online state feature profile was added, defaulting to `full`; `minimal` is available for production throughput when rich perp enrichment is not required.

Artifact writing is streamed. JSONL writers now flush every N lines and on close instead of forcing a flush for every line. This keeps crash visibility while reducing write overhead.

Post-run analysis already uses run-level artifacts. The CLI now supports `--jobs` and `--force`; the default remains safe and skip-aware.

Dataset extraction preserves enriched prefixes and now records extraction metadata including columns written, skipped runs, failed runs, and estimated parquet memory size.

Cleanup already validates that summaries and extracted datasets exist before deleting logs/non-retained runs. This guard remains critical.

The daemon no longer needs to hold full command outputs in memory; command logs stay on disk. Heartbeats now include stage, uptime, queue counts, and memory snapshots where available. `gc.collect()` runs after each job.

## Implemented Controls

- `--max-workers-auto`
- `--reserve-ram-gb`
- `--max-ram-per-worker-gb`
- `--min-free-ram-gb`
- `--run-timeout-seconds`
- `--fail-fast` / `--no-fail-fast`
- `--resume-strict` / `--no-resume-strict`
- per-run atomic `run_status.json`
- memory backpressure before new waves
- progress logs with completed/running/skipped/failed/ETA/free RAM
- daemon pass-through for runner resource controls
- default daemon workers restored to 6

## Data Loading

The safest high-impact optimization is the streaming `ResearchPanelLoader` path:

- loads only required row groups/date windows where possible
- streams batches instead of materializing the full multi-symbol panel
- preserves volatile universe timestamp gating
- preserves funding/OI source timestamp checks

Measured before this audit in local smoke checks: stable research panel initialization dropped from roughly 4.1 GB RSS to roughly 1.1 GB RSS, with four workers settling around 1.1-1.2 GB each after startup.

## Rejected Optimizations

- Cross-run shared mutable DataFrame cache: rejected because it risks accidental mutation and stale config reuse.
- Forward-filled OHLCV gaps: rejected because missing bars must remain missing decisions.
- Reusing indicators across incompatible parameter sets: rejected because it can silently change strategy behavior.
- Compressing JSONL before downstream extraction: rejected unless downstream readers explicitly support gzip.
- Materializing volatile symbols as always active: rejected because it leaks future membership.

## Remaining Risks

- `ProcessPoolExecutor` cannot reliably kill only one running child on every Python/platform combination. `--run-timeout-seconds` marks timed-out work failed when cancellation succeeds; very hard hangs may still require wave-level termination in a future subprocess-per-run backend.
- Post-run `--jobs` is currently a compatibility/control flag; deeper parallel analysis should be added only around independent per-run diagnostics.
- Peak per-worker RSS still depends heavily on strategy logs and active symbol count.

## Validation

Focused tests executed:

`PYTHONPATH=src pytest -q tests/test_runner_resource_controls.py tests/test_runner_resume_safety.py tests/test_parallel_grid_hardening.py tests/test_parallel_grid_runner.py tests/test_online_state_layer.py tests/test_post_run_analysis_efficiency.py tests/test_dataset_extraction_preserves_enriched_fields.py tests/test_extract_experiment_dataset_phase9.py tests/test_research_orchestration_data_profiles.py`

Result: 38 passed.
