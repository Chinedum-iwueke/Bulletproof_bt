# Dataset Contract

## What this contract covers
This contract defines accepted market-data inputs, validation rules, and dataset-directory behavior for client runs.

Implementation: src/bt/data/load_feed.py, src/bt/data/dataset.py, src/bt/data/symbol_source.py, src/bt/data/validation.py

## V1 support
- Input forms:
  - Single file (`.csv` or `.parquet`).
  - Dataset directory containing `manifest.yaml`.
- Dataset manifest schemas:
  - Strict v1 schema (`version: 1`, `format: parquet`, `files: ...`).
  - Legacy schema (`format: per_symbol_parquet`, `symbols`, `path: "{symbol}..."`).

Implementation: src/bt/data/load_feed.py, src/bt/data/dataset.py

## Inputs and guarantees
- `ts` must be timezone-aware UTC.
- Per-symbol timestamps must be strictly increasing (no duplicates, no non-monotonic rows).
- OHLC sanity must hold: `low <= min(open, close)`, `high >= max(open, close)`, `high >= low`.
- `volume >= 0`.
- Gaps are preserved; missing bars are not synthesized or interpolated.
- Dataset directory scope knobs supported:
  - `data.symbols_subset` (canonical)
  - `data.symbols` (alias of `data.symbols_subset`; setting both with different values is an error)
  - `data.max_symbols`
  - `data.date_range` (start inclusive, end exclusive)
  - `data.row_limit_per_symbol`
  - `data.chunksize` (performance-only)

Implementation: src/bt/data/symbol_source.py, src/bt/data/stream_feed.py, src/bt/data/config_utils.py, src/bt/logging/trades.py

## Rejections and failure modes
- Invalid/missing manifest, invalid YAML, unsupported schema, unknown symbols in subset, missing referenced files.
- Non-UTC timestamps, non-monotonic timestamps, invalid OHLC, negative volume.
- Invalid streaming knobs (for example non-positive `chunksize` or `row_limit_per_symbol`).
- Dataset directory in `dataframe` mode is rejected.

Implementation: src/bt/data/dataset.py, src/bt/data/symbol_source.py, src/bt/data/stream_feed.py, src/bt/data/load_feed.py

## Artifacts and where to look
- `data_scope.json` is written only when scope-reducing knobs are used.
- Includes requested scope plus effective symbols when dataset-dir inputs are used.

Implementation: src/bt/logging/trades.py

## Examples
Single-file run (CSV):

```yaml
# run: python scripts/run_backtest.py --data data/curated/sample.csv --config configs/engine.yaml
```

Dataset-dir run (strict v1 manifest):

```yaml
# dataset_dir/manifest.yaml
version: 1
format: parquet
files:
  - symbol: BTCUSDT
    path: symbols/BTCUSDT.parquet
  - symbol: ETHUSDT
    path: symbols/ETHUSDT.parquet
```

Dataset-dir run (legacy manifest):

```yaml
# dataset_dir/manifest.yaml
format: per_symbol_parquet
symbols: [BTCUSDT, ETHUSDT]
path: "symbols/{symbol}.parquet"
```

## Versioning
- Contract version: v1.
- Manifest schema versioning:
  - Strict schema exposes `version: 1`.
  - Legacy schema is supported for compatibility.
- Schema versioning for all dataset-related artifacts is not yet uniformly exposed; treat this doc plus tests as source of truth.

Observation points: tests/test_dataset_manifest_normalization.py, tests/test_symbol_source_streaming_validation.py, tests/test_streaming_knobs.py, tests/test_data_scope_artifact.py
