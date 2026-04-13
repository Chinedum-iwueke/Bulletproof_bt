# L1-H9 Parent Family — Momentum Breakout Continuation

## Hypothesis card (parent)
- Claim: directional breakout events, when aligned with strong trend/participation, can continue enough to produce positive **net** EV under execution-aware conditions.
- Sequence: trend context -> ATR-scaled breakout threshold breached -> breakout confirmed on closed HTF bar -> directional entry -> continuation extends or fails -> management determines captured tail.

## Runtime semantics
- Base/execution data frequency: **1m**.
- Signal timeframe: variant-specific HTF (`15m` or `1h`).
- Breakout logic computed only on closed HTF bars.
- Stop/exit monitoring on 1m execution clock.
- Canonical R accounting source: `engine_canonical_R` only.

## Mandatory stop semantics
- ATR source timeframe = signal timeframe.
- `stop_distance = stop_atr_mult * ATR_entry`.
- `stop_price = entry_price ± stop_distance` by side.
- Stop is frozen at entry and defines canonical initial risk/R.
- H9A: no dynamic ATR recomputation.
- H9B: may lock/trail post-TP1 runner but never rewrites initial R.

## Variant cards
- **L1-H9A (Baseline Momentum Breakout):**
  - Grid: `signal_timeframe {15m,1h} × adx_min {25,30} × breakout_atr_mult {0.75,1.0,1.25} × tp1_at_r {2.0,3.0}` = **24**.
- **L1-H9B (Breakout With Partial + Runner):**
  - Grid: `signal_timeframe {15m,1h} × tp1_at_r {2.0,3.0,4.0} × post_tp1_lock_r {0.5,1.0} × trail_atr_mult {2.5,3.0}` = **24**.

## Fixed reusable settings used by implementation
- `ema_fast_period=9`, `ema_slow_period=21`, `adx_period=14`, `atr_period=14`.
- Breakout level type: prior closed signal-bar extreme (`prior_bar_extreme`).
- Entry reference price for risk model: current 1m close at submit instant.
