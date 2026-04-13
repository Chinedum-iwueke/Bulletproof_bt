# L1-H9B — Breakout With Partial + Runner

## What it tests
Whether momentum-breakout EV improves with TP1 monetization + protected runner continuation capture.

## Entry semantics
Same family entry logic as H9A.

## Management semantics
- TP1 partial at `tp1_at_r`.
- After TP1, stop lock engages at `entry ± post_tp1_lock_r * initial_stop_distance`.
- ATR trail active only after TP1 using `trail_atr_mult * ATR_entry` from high/low since entry.
- Initial stop and initial R remain frozen.
