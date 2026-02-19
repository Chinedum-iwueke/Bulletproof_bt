# Beginner vs Pro Contract

## What this contract covers
This contract explains supported client usage tiers using current built-in config packs and execution profiles.

Implementation: configs/examples/safe_client.yaml, configs/examples/strict_research.yaml, src/bt/execution/profile.py

## V1 support
Beginner-safe path:
- `execution.profile: tier2` (default if unset)
- `risk.stop_resolution: safe`
- `risk.allow_legacy_proxy: true` via `configs/examples/safe_client.yaml`

Pro/research path:
- explicit execution tier (`tier1`/`tier2`/`tier3`/`custom`)
- `risk.stop_resolution: strict`
- `risk.allow_legacy_proxy: false` via `configs/examples/strict_research.yaml`

Implementation: src/bt/execution/profile.py, src/bt/core/config_resolver.py, configs/examples/safe_client.yaml, configs/examples/strict_research.yaml

## Inputs and guarantees
| Area | Beginner (safe) | Pro (strict) |
| --- | --- | --- |
| Stop fallback | Allowed | Rejected |
| Missing stop handling | May use legacy proxy fallback | Entry rejected |
| Execution overrides | Use profile presets | Preset or fully specified `custom` |
| Determinism | Same data + same config => same outputs | Same data + same config => same outputs |

Implementation: src/bt/risk/stop_resolver.py, src/bt/execution/profile.py, tests/test_stage_f_presentation_contract.py

## Rejections and failure modes
- Strict mode rejects incompatible config (`allow_legacy_proxy=true`).
- Strict mode rejects unresolved/missing stop intent on entries.
- Custom execution profile rejects missing override fields.

Implementation: src/bt/core/config_resolver.py, src/bt/risk/risk_engine.py, src/bt/execution/profile.py

## Artifacts and where to look
- `run_status.json` for stop/execution metadata and PASS/FAIL.
- `decisions.jsonl` for per-intent reasons and fallback evidence.
- `performance.json` for final metrics.

Implementation: src/bt/api.py, src/bt/experiments/grid_runner.py

## Examples
Beginner-safe run:

```yaml
# CLI
# python scripts/run_backtest.py --data <PATH> --config configs/engine.yaml --override configs/examples/safe_client.yaml
```

Pro-strict run:

```yaml
# CLI
# python scripts/run_backtest.py --data <PATH> --config configs/engine.yaml --override configs/examples/strict_research.yaml
```

## Versioning
- Contract version: v1.
- Safe/strict packs are concrete config files in-repo and versioned by Git history.
- Schema versioning: not yet exposed for this tiering model; treat docs and tests as source of truth.

Observation points: tests/test_config_packs_stop_contract.py, tests/test_stop_resolution_enforcement_modes.py, tests/test_stage_f_presentation_contract.py
