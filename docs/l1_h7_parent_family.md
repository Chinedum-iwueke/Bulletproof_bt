# L1-H7 Parent Family: Squeeze → Expansion → Pullback Continuation

L1-H7 defines a continuation family with three causal stages on completed signal bars:

1. Compression regime (`BB inside KC`) sustained for a minimum duration.
2. Directional expansion after compression (band break or ADX+EMA directional confirmation).
3. Controlled pullback to continuation references (EMA21 and/or SessionVWAP) within a bounded wait window.

## Parent claim

A sustained compression regime followed by directional expansion and a controlled pullback creates continuation opportunities with positive net EV under realistic costs and disciplined post-entry management.

## Family variants currently formalized

- **L1-H7A**: baseline strategy definition and baseline parameter study.
- **L1-H7B**: dedicated timeframe robustness study over the same base family logic.

## Locked institutional semantics

- Canonical input stream: **1m OHLCV**.
- Signal decisions: completed HTF bars only.
- Execution monitoring: **1m** for stop, TP1 partial, protection, and runner trailing.
- Risk accounting truth: **engine canonical R path only** (`ev_r_gross`, `ev_r_net`, MAE/MFE R, payoff ratio, average win/loss R).
- Position discipline: no pyramiding, one active same-symbol position at a time.
