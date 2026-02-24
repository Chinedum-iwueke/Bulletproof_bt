# Execution Model Contract

## Stable contract (client-facing)

### Order support matrix (v1)
| Order type | Support |
| --- | --- |
| `MARKET` | ✅ supported |
| `LIMIT` | ❌ defined in enums, but execution rejects non-market orders in v1 |

### Fill timing semantics
- `delay_bars` means an order waits that many bars before it becomes fill-eligible.
- Delay countdown is tracked in order metadata (`delay_remaining`).

### Intrabar mode options
- `worst_case`: BUY at high, SELL at low.
- `best_case`: BUY at low, SELL at high.
- `midpoint`: `(high + low) / 2`.

Default intrabar mode is `worst_case`.

## Cost model pipeline (deterministic)
1. Intrabar raw market fill price.
2. Spread application (`execution.spread_mode`).
3. Slippage application.
4. Exchange fee on final notional.
5. Instrument commission (if configured).

No randomized behavior is introduced.

## Spread semantics by instrument

- `execution.spread_mode=none`: no spread adjustment.
- `execution.spread_mode=fixed_bps`: total spread = `price * spread_bps / 10000`.
- `execution.spread_mode=bar_range_proxy`: deterministic bar-range proxy spread.
- `execution.spread_mode=fixed_pips`: **FX-only** spread mode.
  - Uses `execution.spread_pips` and `instrument.pip_size`.
  - Fallback: `instrument.tick_size` if `pip_size` is absent (pip-as-tick fallback).

Spread is always adverse to the trader:
- BUY: `price + half_spread`
- SELL: `price - half_spread`

FX V1 guardrail:
- If `instrument.type=forex`, spread modeling is required (`fixed_pips` or `fixed_bps`).

## Commission semantics by instrument

`execution.commission.mode` options:
- `none`: 0
- `per_trade`: fixed value per fill (`execution.commission.per_trade`)
- `per_share`: equity-only, `abs(qty) * per_share`
- `per_lot`: forex-only, `abs(qty) * per_lot`

Compatibility guardrails:
- `per_share` requires `instrument.type=equity`
- `per_lot` requires `instrument.type=forex`

## Tier profiles and overrides

Execution tier presets (`tier1`, `tier2`, `tier3`) and `custom` override policy remain unchanged:
- preset tiers still lock override fields (`maker_fee`, `taker_fee`, `slippage_bps`, `delay_bars`, `spread_bps`)
- `custom` still requires all override fields

New spread/commission knobs are additive execution controls and do not change tier enforcement behavior.
