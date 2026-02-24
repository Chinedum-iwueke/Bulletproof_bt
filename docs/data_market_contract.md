# Data Market Contract

`data.market` controls market-aware timestamp validation for incoming bars.

Supported values:
- `crypto_24x7` (default)
- `fx_24x5`
- `equity_session`

## Common invariants
- Internal timestamps must be timezone-aware UTC.
- Validation checks bars that exist; it does not synthesize/fill bars.
- Gaps remain gaps (no interpolation).

## FX (`fx_24x5`)
- Weekend bars (Saturday/Sunday UTC) are rejected by default.
- Override only when intentional via:
  - `data.allow_weekend_bars: true`

## Equity (`equity_session`)
Requires `data.equity_session`:
- `timezone` (IANA TZ, e.g. `America/New_York`)
- `open_time` (`HH:MM`)
- `close_time` (`HH:MM`)
- `trading_days` (e.g. `['Mon','Tue','Wed','Thu','Fri']`)

Rules:
- Localized timestamp must be on an allowed trading day.
- Localized clock time must satisfy:
  - inclusive open (`>= open_time`)
  - exclusive close (`< close_time`)

## Error format expectations
Validation errors include:
- source path (`dataset_dir`/file path)
- `symbol`
- offending timestamp (UTC and local where applicable)
- failed rule
- actionable fix (config key + example)
