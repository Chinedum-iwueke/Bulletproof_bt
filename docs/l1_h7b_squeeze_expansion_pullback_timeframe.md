# L1-H7B Timeframe Robustness Study (Squeeze Expansion Pullback)

## What L1-H7B tests

Whether the exact L1-H7 base logic is structurally timeframe-dependent.

L1-H7B reuses the same strategy family mechanics as L1-H7A and varies only the signal timeframe as a first-class study axis.

## Locked semantics

- Base data frequency expected: `1m`.
- Exit monitoring timeframe: `1m`.
- Family pattern: `squeeze_expansion_pullback`.
- Risk accounting: `engine_canonical_R`.
- No pyramiding.

## Exact L1-H7B grid (24 runs)

- `signal_timeframe ∈ {5m, 15m, 1h}`
- `adx_min ∈ {18, 25}`
- `pullback_max_wait ∈ {8, 12}`
- `partial_at_r ∈ {1.5, 2.0}`

Total: `3 × 2 × 2 × 2 = 24`.

## Interpretation notes

If edge appears only in one timeframe bucket and fails to persist across the others after costs, treat the family as timeframe-sensitive and not yet robust. If results are stable across buckets, prioritize follow-on selectivity variants in a separate hypothesis generation step.
