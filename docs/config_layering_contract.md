# Config Layering Contract

## What this contract covers
This contract defines config merge order, override semantics, and canonical risk/config normalization.

Implementation: src/bt/api.py, src/bt/config.py, src/bt/core/config_resolver.py

## V1 support
Merge order for backtests:
1. Base config (`--config`)
2. `configs/fees.yaml`
3. `configs/slippage.yaml`
4. Each `--override` in argument order

Then config is normalized and validated before engine build.

Implementation: src/bt/api.py

## Inputs and guarantees
- Deep-merge semantics:
  - Mapping + mapping merges recursively.
  - Scalar/list values are replaced by override value.
- Canonical defaults and aliases are resolved (for example risk aliases and stop-resolution normalization).
- Deterministic resolved config behavior for same inputs.

Implementation: src/bt/config.py, src/bt/core/config_resolver.py

## Rejections and failure modes
- Missing/unparseable YAML config files.
- Conflicting alias values (for example canonical and legacy keys disagreeing).
- Invalid bounds/types for normalized risk/execution fields.

Implementation: src/bt/config.py, src/bt/core/config_resolver.py

## Artifacts and where to look
- `config_used.yaml`: effective resolved run config.
- `run_status.json`: derived execution snapshot and run outcome.

Implementation: src/bt/logging/trades.py, src/bt/api.py

## Examples
Single-file run with one override:

```yaml
# CLI
# python scripts/run_backtest.py --data data/curated/sample.csv --config configs/engine.yaml --override configs/examples/safe_client.yaml
```

Dataset-dir run with strict pack:

```yaml
# CLI
# python scripts/run_backtest.py --data <dataset_dir> --config configs/engine.yaml --override configs/examples/strict_research.yaml
```

## Versioning
- Contract version: v1.
- Config normalization is code-defined and backward compatibility is maintained via alias handling where implemented.
- Schema versioning: not yet exposed as a unified config schema field; treat docs/tests as source of truth.

Observation points: tests/test_config_deep_merge_overrides.py, tests/test_config_resolver_dedup.py, tests/test_config_resolver_risk_aliases.py, tests/test_experiment_grid_local_config_overlay.py
