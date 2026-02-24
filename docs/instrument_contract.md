# Instrument Contract (T1)

`instrument` is an optional top-level config block used to carry instrument metadata in a normalized, validated form.

## Supported keys

- `instrument.type`: one of `crypto`, `forex`, `equity`, `futures`.
- `instrument.symbol`: instrument identifier for the configured run.
- `instrument.tick_size`: optional minimum price increment (> 0).
- `instrument.contract_size`: optional notional contract size (> 0).
- `instrument.pip_size`: optional FX pip size (> 0).
- `instrument.pip_value`: optional FX pip value (> 0).
- `instrument.account_currency`: optional account currency code.
- `instrument.quote_currency`: optional quote currency code.
- `instrument.base_currency`: optional base currency code.

## T1 behavior

- The `instrument` block is optional.
- If present, it is validated during config resolution.
- If `instrument.type` is omitted, it defaults to `crypto`.
- T1 is metadata-only plumbing: it does **not** change execution, PnL, accounting, or risk behavior.

## Forward use

Later stages will use these fields for:

- spread/pip-aware execution modeling,
- instrument-aware risk sizing,
- market/session-hours controls.
