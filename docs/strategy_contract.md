# Strategy Contract

## What this contract covers
This contract defines the strategy interface, allowed strategy outputs, and stop intent expectations.

Implementation: src/bt/strategy/base.py, src/bt/core/types.py, src/bt/strategy/context_view.py

## V1 support
- Required interface: `Strategy.on_bars(ts, bars_by_symbol, tradeable, ctx) -> list[Signal]`.
- Strategy output is intent-only `Signal` objects.
- `ctx` is read-only via adapter.
- `bars_by_symbol` only includes symbols that have a bar at that timestamp (gaps preserved).

Implementation: src/bt/strategy/base.py, src/bt/strategy/htf_context.py, src/bt/strategy/context_view.py

## Inputs and guarantees
- Timestamp fields in signals and bars must be timezone-aware UTC.
- Signal confidence must be in `[0, 1]`.
- Stop intent can be provided as:
  - `signal.stop_price` (when strategy/dataclass usage includes it in metadata conventions), or
  - `signal.metadata.stop_spec`.
- Not specified / implementation-defined: strategy-level side effects beyond emitted signals. Clients should treat strategy logic as user code and validate through integration tests.

Implementation: src/bt/core/types.py, src/bt/strategy/base.py

## Rejections and failure modes
- Invalid signal payloads are rejected by type validation and downstream risk resolution.
- In strict stop mode, entries without resolvable stop data are rejected.
- By default, multiple signals for the same `(ts, symbol)` are rejected before the engine loop with an actionable `ValueError` that points to `strategy.signal_conflict_policy`.

Implementation: src/bt/core/types.py, src/bt/risk/stop_resolver.py, src/bt/risk/risk_engine.py, src/bt/strategy/signal_conflicts.py

## Signal conflict policy
- Config key: `strategy.signal_conflict_policy`.
- Supported values:
  - `reject` (default): fail fast on same `(ts, symbol)` multi-signal emissions.
  - `first_wins`: keep first emitted signal for that `(ts, symbol)`.
  - `last_wins`: keep last emitted signal.
  - `net_out`: deterministic resolver with exit-priority semantics:
    - opposite entry sides (`BUY` + `SELL`) net to no-op,
    - exit-like (`metadata.is_exit`, `metadata.reduce_only`, or `signal_type` ending in `_exit`) wins over entries,
    - multiple exits keep last exit,
    - multiple same-side entries keep last entry.

Implementation: src/bt/strategy/signal_conflicts.py, src/bt/strategy/htf_context.py, src/bt/api.py

## Artifacts and where to look
- `decisions.jsonl`: accepted/rejected order intents with metadata.
- `run_status.json`: stop-resolution rollups and stop contract report when risk stop config is present.

Implementation: src/bt/logging/jsonl.py, src/bt/experiments/grid_runner.py, src/bt/risk/stop_contract_reporting.py

## Examples
Minimal strategy behavior contract:

```yaml
strategy:
  name: coinflip
  seed: 42
```

Strict stop-ready signal metadata pattern:

```yaml
# signal metadata shape used by stop resolver
metadata:
  stop_spec:
    contract_version: 1
    kind: atr
    atr_multiple: 2.0
```

## Versioning
- Contract version: v1.
- Strategy interface changes should be additive-only for compatibility.
- Schema versioning for signal metadata is partially exposed (`stop_spec.contract_version`) and otherwise implementation-defined.

Observation points: tests/test_strategy_context_readonly.py, tests/test_client_strategy_template_import.py, tests/test_stop_spec_normalization.py
