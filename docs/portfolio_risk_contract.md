# Portfolio and Risk Contract

## What this contract covers
This contract defines portfolio/risk controls, stop-resolution modes, and client-safe vs strict behavior.

Implementation: `src/bt/portfolio/portfolio.py`, `src/bt/risk/risk_engine.py`, `src/bt/risk/stop_resolver.py`, `src/bt/core/config_resolver.py`.

## V1 support
- Risk normalization and validation for canonical `risk.*` keys.
- Stop resolution modes:
  - `safe`
  - `strict`
  - legacy alias `allow_legacy_proxy` (normalized to safe+allow)
- Config packs shipped:
  - `configs/examples/safe_client.yaml`
  - `configs/examples/strict_research.yaml`

## Inputs and guarantees
- Risk keys validated with bounds (for example `min_stop_distance_pct`, `max_notional_pct_equity`, `maintenance_free_margin_pct`).
- `strict` mode forbids `allow_legacy_proxy=true`.
- Stop contract reporting is deterministic for identical inputs.

## Instrument-aware sizing (T3)

Sizing remains R-normalized (`risk_amount` and stop-distance based), with deterministic instrument-aware conversion:

- **Crypto / no instrument block**
  - Keeps existing sizing behavior (no new rounding changes by default).
- **Equity**
  - Quantity is shares.
  - Shares are rounded down to whole integers.
- **Forex**
  - Quantity is lots.
  - Uses `instrument.contract_size` and `risk.fx.lot_step`.
  - Rounds down to lot step deterministically.

### New optional risk keys
- `risk.fx.lot_step` (required when `instrument.type=forex`)
- `risk.fx.pip_value_override` (optional)
- `risk.margin.leverage` (optional; if set must be `> 0`)

### Validation guardrails
- `instrument.type=forex` requires:
  - `instrument.contract_size`
  - `risk.fx.lot_step`
- Invalid/missing sizing keys raise actionable `ValueError`/`ConfigError` with key paths.

## Rejections and failure modes
- Invalid risk key types/ranges raise config errors.
- Strict mode + missing/unresolvable stop rejects entry intents.
- Margin and notional guardrail violations are rejected.
- Instrument-aware sizing can reject too-small orders (for example lot-step rounding to zero).

## Artifacts and metadata
- `decisions.jsonl` includes risk metadata fields such as:
  - `risk_amount`, `stop_distance`, `stop_source`
  - `qty_rounding_unit`, `instrument_type`, `sizing_notional`, `sizing_margin_required`
- `run_status.json` includes stop-resolution summary fields.

## Versioning
- Contract version: v1.
- Stop contract report includes `version: 1` today.
- Other risk schema versioning is not explicitly tagged; docs + tests are source of truth.
