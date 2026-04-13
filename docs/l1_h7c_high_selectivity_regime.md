# L1-H7C High-Selectivity Regime Variant

Uses L1-H7 squeeze → expansion → pullback continuation with stricter regime filters only.

## Grid (12 runs)
- signal_timeframe: {15m, 1h}
- adx_min: {30, 35, 40}
- fail_fast_bars: {4, 8}

## Locked semantics
- Base data: 1m canonical.
- Signal clock: completed HTF bars only.
- Exit monitoring clock: 1m.
- Risk accounting: `engine_canonical_R`.
