# Config Layering Contract

## Stable contract (client-facing)

### Three config layers
Runtime stack for `run_backtest`:
1. `configs/engine.yaml` (or `--config` base file)
2. Overlay defaults: `configs/fees.yaml`, then `configs/slippage.yaml`
3. User overrides (`--override`) in provided order
4. `resolve_config(...)` normalization, then `config_used.yaml` write as resolved truth

Repo Evidence: `src/bt/api.py::run_backtest`, `src/bt/logging/trades.py::write_config_used`.

### Deep merge behavior
`deep_merge(base, override)` rules:
- dict + dict => recursive merge
- scalar/list => override replaces base value
- merge is deep-copied (no shared references)

Repo Evidence: `src/bt/config.py::deep_merge`, `tests/test_config_deep_merge_overrides.py`.

### Path resolution semantics
`resolve_paths_relative_to(base_path_dir, override_paths)` currently resolves non-absolute override paths relative to **current working directory** (`Path.cwd()`), not `base_path_dir`.

Repo Evidence: `src/bt/config.py::resolve_paths_relative_to`, `tests/test_config_resolver_dedup.py`.

### Determinism contract
For a fixed code version:
- same dataset
- same resolved `config_used.yaml`
- same run path semantics
=> deterministic outputs expected (excluding run-id/time metadata).

Repo Evidence: `tests/test_config_packs_stop_contract.py::test_configs_resolve_deterministically`, `tests/test_artifact_deterministic_serialization.py`, `tests/test_run_status_execution_metadata.py::test_run_status_execution_metadata_is_deterministic`.

## Copy/paste examples

### Override only benchmark symbol
```yaml
benchmark:
  enabled: true
  symbol: BTCUSDT
```

### Override only subset list
```yaml
data:
  symbols_subset: [BTCUSDT, ETHUSDT]
```

### Override execution custom fields (must use custom profile)
```yaml
execution:
  profile: custom
  maker_fee: 0.0
  taker_fee: 0.001
  slippage_bps: 2.0
  spread_bps: 1.0
  delay_bars: 1
```

## FAQ / common failure modes
- **Override file not found**  
  Check working directory and override path; non-absolute paths are cwd-relative.

- **My partial nested override removed sibling keys**  
  For dicts it should merge, not replace; verify YAML shape (mapping vs scalar/list).

- **Custom execution values rejected**  
  Ensure `execution.profile: custom` and all required fields are present.

Repo Evidence: `src/bt/config.py`, `src/bt/execution/profile.py::resolve_execution_profile`.
