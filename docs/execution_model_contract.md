# Execution Model Contract

## What this contract covers
This contract defines how orders are processed into fills, what execution profiles are supported, and what clients can rely on in v1.

Implementation: src/bt/execution/execution_model.py, src/bt/execution/profile.py, src/bt/execution/effective.py

## V1 support
- `OrderType.MARKET` is supported.
- `OrderType.LIMIT` is defined but rejected by execution processing.
- Execution profiles:
  - `tier1`
  - `tier2` (default)
  - `tier3`
  - `custom` (all override keys required)

Implementation: src/bt/execution/execution_model.py, src/bt/execution/profile.py, src/bt/core/enums.py

## Inputs and guarantees
- Deterministic profile resolution from config.
- Delay handling via `delay_bars`.
- Fill pricing applies, in order:
  1. Intrabar market fill model.
  2. Spread adjustment.
  3. Slippage adjustment.
  4. Fee computation on notional.
- Effective execution metadata is written into run status.

Implementation: src/bt/execution/execution_model.py, src/bt/execution/profile.py, src/bt/execution/effective.py, src/bt/experiments/grid_runner.py

## Rejections and failure modes
- Non-market orders: `NotImplementedError("Only MARKET orders are supported in v1.")`.
- Invalid `execution.profile` values or forbidden profile overrides.
- Invalid `custom` profile payload (missing required fields).

Implementation: src/bt/execution/execution_model.py, src/bt/execution/profile.py

## Artifacts and where to look
- `run_status.json` contains effective execution snapshot keys (`execution_profile`, fees/slippage/spread/delay, intrabar mode).

Implementation: src/bt/execution/effective.py, src/bt/api.py, src/bt/experiments/grid_runner.py

## Examples
Tier preset:

```yaml
execution:
  profile: tier2
```

Custom profile:

```yaml
execution:
  profile: custom
  maker_fee: 0.0
  taker_fee: 0.001
  slippage_bps: 2.0
  delay_bars: 1
  spread_bps: 1.0
```

## Versioning
- Contract version: v1.
- Execution profile schema is config-driven and additive changes should be backward compatible.
- Schema versioning: not yet exposed as a dedicated execution schema field; treat docs and tests as source of truth.

Observation points: tests/test_execution_model.py, tests/test_execution_profile_resolution.py, tests/test_execution_profile_override_policy.py, tests/test_run_status_execution_metadata.py
