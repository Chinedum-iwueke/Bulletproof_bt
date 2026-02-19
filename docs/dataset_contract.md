# Dataset Contract

## Stable contract (client-facing)

### Supported input modes
1. **Single file** (`.csv` or `.parquet`): loaded as in-memory dataframe feed.
2. **Dataset directory** (`manifest.yaml` + per-symbol parquet files): loaded as streaming feed.

**Single-file vs dataset-dir difference:** dataset directories default to `data.mode=streaming`; single files default to `data.mode=dataframe` (and `streaming` is currently coerced back to dataframe for single files).  
Repo Evidence: `src/bt/data/load_feed.py::load_feed`, `src/bt/data/load_feed.py::_resolve_mode`.

### Manifest schemas accepted for dataset directories
- **Strict v1 manifest**
  - Required: `version: 1`, `format: parquet`, `files`.
  - `files` supports either:
    - list of strings (path-only; internal synthetic symbols are assigned), or
    - list of objects `{symbol, path}`.
- **Legacy manifest**
  - Required: `format: per_symbol_parquet`, `symbols`, `path`.
  - `path` must include `{symbol}`.

Repo Evidence: `src/bt/data/dataset.py::_normalize_v1_manifest`, `src/bt/data/dataset.py::_normalize_legacy_manifest`.

### Universe scoping rules (dataset-dir)
Precedence and behavior:
1. `data.symbols_subset` (canonical list)
2. `data.symbols` (alias; used when `symbols_subset` absent)
3. If both present and differ -> error
4. `data.max_symbols` applies after symbol list selection (caps first N symbols)
5. If no symbol scope keys -> full manifest universe

Repo Evidence: `src/bt/data/dataset.py::_apply_optional_filters`, `src/bt/core/config_resolver.py::_resolve_data_symbols_alias`.

### Gap policy
- Missing bars are **not interpolated**.
- Streaming tick output is a dictionary for the current timestamp; symbols with no bar at that timestamp are simply absent from that dict.

Repo Evidence: `src/bt/data/resample.py` module docstring, `src/bt/data/stream_feed.py::StreamingHistoricalDataFeed.next`, `src/bt/portfolio/portfolio.py::mark_to_market`.

### Timestamp/ordering requirements
- Timestamps must be timezone-aware UTC.
- Per-symbol timestamps must be strictly increasing.
- Duplicate `(symbol, ts)` rows are rejected.

Repo Evidence: `src/bt/data/symbol_source.py::_parse_ts_utc`, `src/bt/data/symbol_source.py::_validate_row`, `src/bt/data/validation.py::validate_bars_df`.

### HTF strictness behavior
- HTF resampling strict mode (`strict=True`) drops incomplete buckets.
- Incomplete = minute gaps inside bucket or wrong bar count vs expected timeframe bars.

Repo Evidence: `src/bt/data/resample.py::TimeframeResampler._finalize`, `src/bt/data/resample.py::TimeframeResampler.update`.

### Streaming knobs
- `data.date_range` (`start` inclusive, `end` exclusive)
- `data.row_limit_per_symbol`
- `data.chunksize`

`data_scope.json` is written only when scope-reducing knobs are active (`symbols_subset/symbols`, `max_symbols`, `date_range`, `row_limit_per_symbol`). `chunksize` is performance-only and does not trigger `data_scope.json`.

Repo Evidence: `src/bt/data/symbol_source.py::_validate_row`, `src/bt/data/stream_feed.py::StreamingHistoricalDataFeed.reset`, `src/bt/logging/trades.py::write_data_scope`.

## Copy/paste YAML examples

### BTC-only subset
```yaml
data:
  mode: streaming
  symbols_subset: [BTCUSDT]
```

### Basket subset
```yaml
data:
  mode: streaming
  symbols_subset: [BTCUSDT, ETHUSDT, SOLUSDT]
```

### Cap first N symbols
```yaml
data:
  mode: streaming
  max_symbols: 20
```

### Date range
```yaml
data:
  mode: streaming
  date_range:
    start: "2024-01-01T00:00:00Z"
    end: "2024-06-01T00:00:00Z"
```

## FAQ / common failure modes
- **"Dataset directories are not supported in dataframe mode"**  
  Fix: set `data.mode: streaming` for dataset-dir inputs.  
  Repo Evidence: `src/bt/data/load_feed.py::load_feed`.

- **"data.symbols and data.symbols_subset both set but differ"**  
  Fix: keep only one, or make them identical.  
  Repo Evidence: `src/bt/data/dataset.py::_apply_optional_filters`.

- **"ts must be timezone-aware UTC" / non-monotonic timestamp errors**  
  Fix: normalize timestamps to UTC and sort per symbol strictly increasing with no duplicates.  
  Repo Evidence: `src/bt/data/symbol_source.py::_parse_ts_utc`, `src/bt/data/symbol_source.py::_validate_row`.

## Repo Evidence index
- `src/bt/data/load_feed.py::load_feed`
- `src/bt/data/dataset.py::load_dataset_manifest`
- `src/bt/data/dataset.py::_apply_optional_filters`
- `src/bt/data/stream_feed.py::StreamingHistoricalDataFeed.reset`
- `src/bt/data/stream_feed.py::StreamingHistoricalDataFeed.next`
- `src/bt/data/symbol_source.py::SymbolDataSource`
- `src/bt/data/resample.py::TimeframeResampler`
- `src/bt/logging/trades.py::write_data_scope`
- `tests/test_data_symbols_alias_scoping.py`
- `tests/test_streaming_knobs.py`
