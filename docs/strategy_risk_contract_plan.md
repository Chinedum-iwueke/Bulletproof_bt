# Repo-driven design plan: Client-proof Strategy ↔ Risk contract + StopResolver

## 1) Repo Current-State Map

### Strategy contract today
- **Primary strategy interface:** `bt.strategy.base.Strategy.on_bars(ts, bars_by_symbol, tradeable, ctx) -> list[Signal]`.
- **Signal type in use:** `bt.core.types.Signal` dataclass with fields:
  - required: `ts`, `symbol`, `side`, `signal_type`, `confidence`
  - optional/extensible: `metadata: dict[str, Any]`
- **Strategy registration/instantiation:** `bt.strategy.register_strategy` and `bt.strategy.make_strategy`.
  - `make_strategy` filters kwargs against strategy `__init__` signature; no schema-level validation by strategy name yet.

### Risk engine + stop sizing path today
- **Entry path:** `RiskEngine.signal_to_order_intent(...)`.
  - Exit/reduce-only bypasses stop resolution (`close_only` / `_exit` signal type handling).
  - Entry/increase-risk path calls `compute_position_size_r(...)`.
- **Stop distance resolution:** `resolve_stop_distance(...)` in `src/bt/risk/stop_distance.py`.
  - priority order: explicit `stop_price` → ATR mode (`risk.stop.mode=atr`) → legacy high/low proxy (`risk.stop.mode=legacy_proxy`) → error.
  - provenance labels from `src/bt/risk/stop_resolution.py`.
- **Stop-resolution mode behavior in risk engine:**
  - current `risk.stop_resolution` accepts `strict | allow_legacy_proxy`.
  - strict: reject unresolved entry stop with actionable reason.
  - allow_legacy_proxy: fallback to temporary `risk.stop.mode=legacy_proxy` path.

### Risk metadata propagation today
- `RiskEngine.signal_to_order_intent` adds metadata including:
  - `risk_amount`, `stop_distance`, `stop_source`, `stop_details`, `r_metrics_valid`, `used_legacy_stop_proxy`, `stop_resolution_mode`, plus margin/cap fields.
- Engine creates `Order(metadata=dict(order_intent.metadata))` and execution copies order metadata into `Fill.metadata`.
- Portfolio extracts `risk_amount`, `stop_distance`, `stop_source` from fill metadata into trade metadata.
- Trades writer emits `risk_amount`, `stop_distance`, and computes `r_multiple_gross/net`.

### Config parsing/defaults today
- Runtime config is normalized by `resolve_config(...)` (`src/bt/core/config_resolver.py`):
  - injects defaults for core, execution, data, strategy, and risk keys.
  - resolves top-level aliases into `risk.*` canonical keys.
  - validates known ranges and enums (including `risk.stop_resolution`).
- Risk spec parsed by `parse_risk_spec(...)` (`src/bt/risk/spec.py`) requiring `risk.mode` + `risk.r_per_trade`.
- `config_used.yaml` is written by `write_config_used(run_dir, config)`.

### Artifacts + summaries today
- `data_scope.json`: written by `write_data_scope(...)` when scope-reducing data knobs are active.
- `run_status.json`: written by `_write_run_status(...)` in `src/bt/experiments/grid_runner.py` (used by both single-run API and grid).
  - derives stop-resolution rollup from `decisions.jsonl` order metadata.
- `run_manifest.json`: written by `write_run_manifest(...)` in `src/bt/logging/run_manifest.py` (currently via CLI script path).
- `summary.txt`: written by `write_summary_txt(...)` in `src/bt/logging/summary.py` (currently via CLI script path).

---

## 2) Proposed Strategy ↔ Risk Contract (StopSpec + Resolution)

Goal: formalize strategy stop intent without touching `engine.py` loop.

### Proposed type placement
1. **`src/bt/risk/contract.py` (new)**
   - keep risk-boundary types with existing stop/risk modules.
2. **`src/bt/risk/stop_distance.py`**
   - adapt resolver to consume normalized StopSpec (adapter layer), preserving current call signature for compatibility.
3. **`src/bt/risk/risk_engine.py`**
   - call boundary validator/normalizer before sizing.

### StopSpec (strategy → risk input) — compatible with `Signal.metadata`
`StopSpec` is a normalized dataclass parsed from signal payload (`signal.stop_price` or `signal.metadata`).

```python
@dataclass(frozen=True)
class StopSpec:
    kind: Literal["explicit", "structural", "atr", "hybrid", "legacy_proxy"]
    stop_price: float | None = None

    # structural
    structural_stop: float | None = None

    # atr
    atr_multiple: float | None = None
    atr_indicator: str | None = None  # default "atr" if omitted

    # hybrid
    hybrid_policy: Literal["wider", "tighter"] | None = None
    hybrid_components: tuple[Literal["structural", "atr"], ...] | None = None

    # diagnostics
    raw_source: str | None = None  # "signal.stop_price", "signal.metadata.stop_spec", etc.
```

#### Parsing compatibility rules
- **Explicit stop (existing behavior):**
  - read from `signal.stop_price` or `signal.metadata.stop_price`.
- **Structural stop:**
  - from `signal.metadata.stop_spec.kind=structural` + `structural_stop`.
- **ATR stop:**
  - from `signal.metadata.stop_spec.kind=atr` + `atr_multiple` (+ optional `atr_indicator`).
- **Hybrid stop:**
  - from `signal.metadata.stop_spec.kind=hybrid` with both structural and ATR components.
  - policy controlled by config (`wider|tighter`).
- **Legacy proxy (safe mode only):**
  - no strategy field required; only generated by boundary adapter when safe mode + explicit opt-in allow fallback.

### StopResolutionResult (risk output)
`src/bt/risk/contract.py`:

```python
@dataclass(frozen=True)
class StopResolutionResult:
    stop_price: float | None
    stop_distance: float
    stop_source: str
    is_valid: bool
    used_fallback: bool
    reason_code: str
    details: dict[str, Any]
```

Reason code examples (string constants in same module):
- `resolved_explicit`
- `resolved_structural`
- `resolved_atr`
- `resolved_hybrid`
- `fallback_legacy_proxy`
- `rejected_missing_stop`
- `rejected_invalid_direction`
- `rejected_indicator_not_ready`

### Compatibility with current provenance labels
- Keep writing current `stop_source` labels in metadata (`explicit_stop_price`, `atr_multiple`, `legacy_high_low_proxy`) for artifact compatibility.
- Add optional richer fields (`stop_reason_code`, `stop_resolution_valid`) without removing old keys.

---

## 3) Config Design (Safe/Strict + defaults)

### Canonical keys (compatible extension)
Under `risk:`
- `stop_resolution_mode`: `safe | strict` (**new canonical key**)
- `allow_legacy_proxy`: `true | false` (**new explicit safety gate**)
- `hybrid_policy`: `wider | tighter` (**new, used only for hybrid stop specs**)
- existing keys retained:
  - `stop_resolution` (legacy alias)
  - `min_stop_distance_pct`
  - `min_stop_distance`
  - `stop.*` (existing ATR/global stop config)

### Resolver normalization policy
In `resolve_config`:
- Accept either:
  - legacy `risk.stop_resolution: strict|allow_legacy_proxy`
  - new `risk.stop_resolution_mode: safe|strict`
- Normalize to canonical `risk.stop_resolution_mode` and backfill legacy `risk.stop_resolution` for compatibility:
  - `strict` -> `strict`
  - `safe` + `allow_legacy_proxy=true` -> legacy value `allow_legacy_proxy`
  - `safe` + `allow_legacy_proxy=false` -> still safe mode but no fallback
- Reject contradictory combinations with `ConfigError`.

### YAML snippets

#### `configs/engine.yaml` (safe client defaults)
```yaml
risk:
  mode: equity_pct
  r_per_trade: 0.005
  stop_resolution_mode: safe
  allow_legacy_proxy: false
  hybrid_policy: wider
  min_stop_distance_pct: 0.001
  min_stop_distance: null
  stop_resolution: strict   # legacy compatibility mirror; resolver keeps consistent
  stop: {}
```

#### `configs/examples/safe_client.yaml` (new)
```yaml
risk:
  stop_resolution_mode: safe
  allow_legacy_proxy: true
  hybrid_policy: wider
  min_stop_distance_pct: 0.001
  stop:
    mode: atr
    atr_indicator: atr
    atr_multiple: 2.5
```

#### `configs/examples/strict_research.yaml` (new)
```yaml
risk:
  stop_resolution_mode: strict
  allow_legacy_proxy: false
  hybrid_policy: tighter
  min_stop_distance_pct: 0.001
  stop: {}
```

---

## 4) Validation & Error Taxonomy

### Validation entry points (fail early)

1. **Config-time validation** (`src/bt/core/config_resolver.py`)
   - validate/normalize `risk.stop_resolution_mode`, `risk.allow_legacy_proxy`, `risk.hybrid_policy`.
   - enforce legacy/new alias consistency.

2. **Strategy-construction validation keyed by strategy.name**
   - add `validate_strategy_config(strategy_name: str, strategy_cfg: dict[str, Any]) -> None` in `src/bt/strategy/validation.py`.
   - called in `_build_engine` immediately after deriving `strategy_name/strategy_kwargs` and before `make_strategy(...)`.

3. **Signal boundary validation (before sizing)**
   - add `normalize_stop_spec(signal: Signal, *, config: dict[str, Any], side: str, entry_price: float, ctx: dict[str, Any], bar: Bar) -> StopSpec | RiskRejection`.
   - invoke inside `RiskEngine.signal_to_order_intent` before `compute_position_size_r`.

4. **Resolver-time validation**
   - keep deterministic checks in `resolve_stop_distance` and adapt to return structured `StopResolutionResult`.

### Error taxonomy and raising style

- `ConfigError` (already exists): config conflicts/invalid enum/range.
- `StrategySchemaError` (new, subclass `ValueError` in `src/bt/strategy/validation.py`): invalid strategy-specific config shape.
- `StrategyContractError` (new, subclass `ValueError` in `src/bt/risk/contract.py`): signal missing/invalid stop contract.
- `RiskRejection` (new lightweight dataclass or exception wrapper in `src/bt/risk/contract.py`): structured rejection reason mapped to `risk_rejected:*` decisions.
- `DataError` (existing style currently `ValueError` in data modules): keep as `ValueError` unless repo already introduces a dedicated class.

### Surfacing
- **CLI output:** exception bubbles from `run_backtest` / script entrypoint.
- **run_status.json:** already captures `error_type`, `error_message`, traceback on fail.
- **summary.txt:** include run_status-derived failure block if present (existing summary already reads run_status).
- **run_manifest.json:** no schema break required; keep config summary unchanged unless explicitly adding a `contract_version` field.

### Example actionable errors
- `ConfigError`: 
  - `Invalid risk.stop_resolution_mode: expected 'safe' or 'strict' got 'aggressive'. Fix: set risk.stop_resolution_mode to safe|strict.`
- `StrategySchemaError`:
  - `strategy=volfloor_donchian requires atr_stop_multiple > 0 when stop_mode in {'atr','hybrid'}. Fix: set strategy.atr_stop_multiple: 2.5.`
- `StrategyContractError`:
  - `BTCUSDT entry signal missing stop_spec/stop_price in strict mode. Fix: attach metadata.stop_price or metadata.stop_spec.kind with required fields.`
- `RiskRejection` (decision reason):
  - `risk_rejected:stop_unresolvable:strict:signal_type=h1_volfloor_donchian_entry:hint=attach_stop_spec_or_enable_safe_mode`

---

## 5) Task List (5–9 tasks)

### Task 1 — Contract inventory guardrail tests (baseline freeze)
- **Goal:** lock current behavior before refactor.
- **Files:**
  - add/extend `tests/test_risk_engine_stop_resolution_and_invariants.py`
  - add/extend `tests/test_stop_distance_resolver.py`
- **Signatures:** none.
- **Tests:**
  - strict unresolved stop rejection reason unchanged.
  - allow_legacy_proxy fallback still sets `used_legacy_stop_proxy`.
  - exit signals still bypass stop resolution.
- **Verification:**
  - `pytest -q tests/test_stop_distance_resolver.py tests/test_risk_engine_stop_resolution_and_invariants.py tests/test_exit_stop_resolution_strict.py`

### Task 2 — Add stop contract types + constants
- **Goal:** introduce explicit `StopSpec`/`StopResolutionResult` without behavior change.
- **Files:**
  - add `src/bt/risk/contract.py`
  - update `src/bt/risk/__init__.py` (exports)
- **Public signatures:**
  - `class StopSpec`
  - `class StopResolutionResult`
  - `class StrategyContractError(ValueError)`
- **Tests:**
  - `tests/test_risk_contract_types.py`: dataclass defaults, serialization-safe fields.
- **Verification:**
  - `pytest -q tests/test_risk_contract_types.py`

### Task 3 — Config-mode normalization (safe/strict + compatibility)
- **Goal:** add canonical mode while preserving legacy key.
- **Files:**
  - modify `src/bt/core/config_resolver.py`
  - modify `src/bt/validation/config_completeness.py`
  - modify `tests/test_default_engine_config_contains_all_supported_keys.py`
  - add `tests/test_config_stop_resolution_modes.py`
- **Public signatures:**
  - none (resolver internal changes).
- **Tests:**
  - parse `risk.stop_resolution_mode=safe|strict`.
  - legacy `risk.stop_resolution` still accepted.
  - contradictory settings raise `ConfigError`.
- **Verification:**
  - `pytest -q tests/test_config_resolver_risk_aliases.py tests/test_config_stop_resolution_modes.py tests/test_config_completeness_validator.py`

### Task 4 — Strategy config schema validation keyed by strategy.name
- **Goal:** fail fast on invalid strategy stop params.
- **Files:**
  - add `src/bt/strategy/validation.py`
  - modify `src/bt/api.py` (`_build_engine` call site)
  - add `tests/test_strategy_schema_validation.py`
- **Public signatures:**
  - `validate_strategy_config(strategy_name: str, strategy_cfg: dict[str, Any]) -> None`
- **Tests:**
  - volfloor stop-mode combos (`structural|atr|hybrid`) validated.
  - unknown strategy still errors from registry as today.
- **Verification:**
  - `pytest -q tests/test_strategy_schema_validation.py tests/test_integration_smoke_strategies.py`

### Task 5 — StopSpec boundary normalizer + resolver adapter
- **Goal:** parse signal metadata into normalized spec pre-sizing.
- **Files:**
  - modify `src/bt/risk/stop_distance.py`
  - modify `src/bt/risk/risk_engine.py`
  - add `tests/test_stop_spec_normalization.py`
- **Public signatures:**
  - `normalize_stop_spec(...) -> StopSpec`
  - `resolve_stop_distance(...) -> StopResolutionResult` (or wrapper preserving old `StopDistanceResult` for compatibility)
- **Tests:**
  - explicit, structural, atr, hybrid(wider/tighter), invalid-direction.
  - deterministic outcome for same inputs.
- **Verification:**
  - `pytest -q tests/test_stop_spec_normalization.py tests/test_stop_distance_resolver.py`

### Task 6 — Safe/strict enforcement + explicit legacy proxy gate
- **Goal:** no silent guessing; fallback only by explicit opt-in.
- **Files:**
  - modify `src/bt/risk/risk_engine.py`
  - extend `tests/test_stop_unresolvable_rejected.py`
  - extend `tests/test_run_status_stop_resolution.py`
- **Public signatures:** none.
- **Tests:**
  - strict rejects missing stop.
  - safe + `allow_legacy_proxy=false` rejects (no fallback).
  - safe + `allow_legacy_proxy=true` can fallback and sets metadata flags.
- **Verification:**
  - `pytest -q tests/test_stop_unresolvable_rejected.py tests/test_run_status_stop_resolution.py tests/test_risk_engine.py`

### Task 7 — Artifact compatibility pass
- **Goal:** keep old artifact keys; append new optional fields.
- **Files:**
  - modify `src/bt/experiments/grid_runner.py` (optional derived notes/reason codes)
  - modify `src/bt/logging/summary.py` (optional messaging)
  - add `tests/test_run_artifact_contract.py`
- **Public signatures:** none.
- **Tests:**
  - existing artifact contract remains valid.
  - run_status still has deterministic keys and accepted stop labels.
- **Verification:**
  - `pytest -q tests/test_run_artifact_contract.py tests/test_run_status_stop_resolution.py tests/test_summary_txt.py`

### Task 8 — Docs/config examples for clients vs research
- **Goal:** publish clear usage and migration examples.
- **Files:**
  - modify `configs/engine.yaml`
  - add `configs/examples/safe_client.yaml`
  - add `configs/examples/strict_research.yaml`
  - update `README.md` (risk stop contract section)
- **Public signatures:** none.
- **Tests:**
  - parser smoke for each example config.
- **Verification:**
  - `pytest -q tests/test_engine_smoke.py tests/test_api_overrides_smoke.py`

---

## 6) Migration Notes & Compatibility

### Expected breakpoints
1. **Strategies that emit entry signals without stop metadata**
   - currently may pass in `allow_legacy_proxy`; under strict or safe-without-legacy they will reject.
2. **Strategies that put non-canonical stop info only in custom metadata keys**
   - need adapter mapping to `metadata.stop_price` or `metadata.stop_spec`.
3. **Configs using only `risk.stop_resolution` legacy key**
   - still accepted; resolver should normalize and emit canonical key.

### Existing strategies likely impact
- `coinflip`: already emits `metadata.stop_price`; should remain compatible.
- `volfloor_donchian`: emits `metadata.stop_price` + stop details; should remain compatible and can optionally migrate to explicit `stop_spec`.
- client template strategies that omit stops may now fail earlier in strict mode.

### Legacy-safe behavior without silent guessing
- Safe mode does **not** imply fallback by default.
- Require `risk.allow_legacy_proxy: true` for legacy high/low fallback.
- On fallback, always set metadata:
  - `used_legacy_stop_proxy: true`
  - `r_metrics_valid: false`
  - `stop_source: legacy_high_low_proxy`
  - `stop_reason_code: fallback_legacy_proxy`

### Artifact schema compatibility
- Keep existing fields untouched where already consumed by tests/tools.
- Add only optional additive fields (`stop_reason_code`, `stop_resolution_valid`, `contract_version`).
- `run_status.stop_resolution` continues using current canonical labels set.

---

## 7) Risks / Open Questions (minimal)

1. **Mode-key naming overlap risk**
   - current repo uses `risk.stop_resolution` with values `strict|allow_legacy_proxy`; introducing `risk.stop_resolution_mode` must avoid ambiguity and resolver conflicts.
2. **Hybrid policy semantics vs existing strategy logic**
   - `volfloor_donchian` currently hardcodes hybrid to wider-stop behavior (`min` for long, `max` for short). Need explicit decision whether `tighter` is globally allowed or strategy-specific.
3. **Where to surface contract failures in summary**
   - `summary.txt` currently focuses on performance + benchmark; decide whether to include stop-contract rejection counts from `decisions.jsonl` or keep in run_status notes only.

