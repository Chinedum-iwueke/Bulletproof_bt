# L1-H7A Baseline Squeeze Expansion Pullback Continuation

## What L1-H7A tests

Whether the base squeeze → expansion → pullback continuation structure has positive cost-adjusted edge when implemented with fixed ATR initial risk, TP1 partial, and protected runner management.

## Signal semantics (completed HTF bars)

- Squeeze: BB inside KC for `squeeze_min_bars`.
- Expansion (after squeeze):
  - BB breakout in directional bias, **or**
  - ADX filter (`adx >= adx_min`) plus EMA fast/slow directional confirmation.
- Pullback: must occur within `pullback_max_wait` bars after expansion and must probe/reclaim:
  - EMA21 and/or SessionVWAP (`OR` logic).
- Entry only when directional bias aligns with expansion direction and no same-symbol position exists.

## Exit / management semantics (1m monitoring)

- Initial stop: frozen ATR multiple at entry (`stop_atr_mult * ATR_signal`).
- TP1: partial close at `partial_at_r`.
- After TP1: activate protection (breakeven-or-better) and ATR-based runner trail.
- No local R accounting; performance uses engine canonical accounting outputs.

## Two-clock behavior

- Signal clock: configurable HTF (`5m` or `15m` in grid), completed bars only.
- Execution clock: `1m` for fill path and open-position exit checks.
- Base data frequency expected: `1m`.

## Exact L1-H7A grid (24 runs)

- `signal_timeframe ∈ {5m, 15m}`
- `adx_min ∈ {18, 25}`
- `pullback_max_wait ∈ {8, 12, 16}`
- `partial_at_r ∈ {1.5, 2.0}`

Total: `2 × 2 × 3 × 2 = 24`.

## Decision metadata emphasis

Entry decisions include squeeze state, expansion state, directional context, pullback reference hits, stop details, and management mode flags (`tp1_hit`, `protection_activated`). Trade-level costs and R statistics remain sourced from canonical engine artifacts.
