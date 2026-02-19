# Portfolio and Risk Contract

## What this contract covers
This contract defines portfolio/risk controls, stop-resolution modes, and client-safe vs strict behavior.

Implementation: src/bt/portfolio/portfolio.py, src/bt/risk/risk_engine.py, src/bt/risk/stop_resolver.py, src/bt/core/config_resolver.py

## V1 support
- Risk normalization and validation for canonical `risk.*` keys.
- Stop resolution modes:
  - `safe`
  - `strict`
  - legacy alias `allow_legacy_proxy` (normalized to safe+allow)
- Config packs shipped:
  - `configs/examples/safe_client.yaml`
  - `configs/examples/strict_research.yaml`

Implementation: src/bt/core/config_resolver.py, configs/examples/safe_client.yaml, configs/examples/strict_research.yaml

## Inputs and guarantees
- Risk keys validated with bounds (for example `min_stop_distance_pct`, `max_notional_pct_equity`, `maintenance_free_margin_pct`).
- `strict` mode forbids `allow_legacy_proxy=true`.
- Stop contract reporting is deterministic for identical inputs.

Implementation: src/bt/core/config_resolver.py, src/bt/risk/stop_contract_reporting.py

## Rejections and failure modes
- Invalid risk key types/ranges raise config errors.
- Strict mode + missing/unresolvable stop rejects entry intents.
- Margin and notional guardrail violations are rejected.

Implementation: src/bt/core/config_resolver.py, src/bt/risk/risk_engine.py, src/bt/risk/reject_codes.py

## Artifacts and where to look
- `run_status.json` includes:
  - `stop_resolution`
  - `used_legacy_stop_proxy`
  - `r_metrics_valid`
  - `stop_resolution_counts`
  - optional `stop_contract` payload
- `decisions.jsonl` contains per-decision reason metadata.

Implementation: src/bt/experiments/grid_runner.py, src/bt/risk/stop_contract_reporting.py

## Examples
Safe client pack:

```yaml
risk:
  stop_resolution: safe
  allow_legacy_proxy: true
  stop:
    mode: legacy_proxy
```

Strict research pack:

```yaml
risk:
  stop_resolution: strict
  allow_legacy_proxy: false
  stop: {}
```

## Versioning
- Contract version: v1.
- Stop contract report includes `version: 1` today.
- Other risk schema versioning is not explicitly tagged; treat docs and tests as source of truth.

Observation points: tests/test_config_packs_stop_contract.py, tests/test_stop_resolution_enforcement_modes.py, tests/test_run_status_stop_resolution.py, tests/test_risk_engine_stop_resolution_and_invariants.py
