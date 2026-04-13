# L1-H9A — Baseline Momentum Breakout

## What it tests
Whether trend-aligned breakout continuation has standalone edge before advanced runner controls.

## Entry semantics
- Trend from `EMA(9) vs EMA(21)` on closed signal bars.
- ADX gate enforced (`adx_min` sweep).
- Breakout confirm requires HTF close beyond prior-bar boundary by `breakout_atr_mult * ATR`.
- Entry direction follows breakout/trend alignment.

## Exit semantics
- Frozen ATR initial stop (`stop_atr_mult=2.0`).
- TP1 partial at `tp1_at_r` with baseline protection (`post_tp1_lock_r=0.0`).
- No ATR trailing (`trail_atr_mult=null`).
- Exit monitoring on 1m.
