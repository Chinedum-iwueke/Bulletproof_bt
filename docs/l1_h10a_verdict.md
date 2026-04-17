# L1-H10A verdict on EV(R) realism and FAILED-with-results runs

## What the code is doing today

1. `ev_r_net` / `ev_r_gross` in `performance.json` are direct means of per-trade `r_multiple_net` / `r_multiple_gross` from `trades.csv` via `summarize_r(...)`. There is no extra scaling in the performance layer.
2. Per-trade R is computed when writing `trades.csv` as:
   - `r_multiple_gross = pnl_price / risk_amount`
   - `r_multiple_net = pnl_net / risk_amount`
3. `risk_amount` used for those formulas is overwritten in `PositionBook._extract_risk_metadata(...)` as `abs(entry_qty) * stop_distance` (if available), regardless of any prior `risk_amount` supplied upstream.
4. Run status can still be `FAILED` **after** `trades.csv` and `performance.json` are written, because `reconcile_execution_costs(run_dir)` runs after performance artifacts are emitted and throws on mismatch.

## Why you can see "FAILED" runs with full-looking metrics

This is expected from control flow:

- engine runs,
- performance artifacts are written,
- then strict reconciliation checks `performance.json fee_total/slippage_total/spread_total` vs summed `fills.jsonl` costs,
- if mismatch: run marked `FAIL` but existing artifacts remain.

So these are not "no-output" failures; they are **post-hoc accounting failure** runs.

## Why the cost mismatch appears in your sample

The reconciliation error in your sample is:

- expected (`performance.fee_total`) = `252.552112607978`
- actual (`sum(fills.fee_cost)`) = `260.4754556940668`

That difference indicates performance cost attribution and fill-ledger totals diverged. Current implementation prefers trade-ledger fee totals when `trades.csv` has `fees_paid`, while reconciliation treats fill-ledger totals as source of truth.

Typical causes under this architecture:

- fees on fills that never formed a closed trade row,
- partial flip/close allocation drift,
- any ledger inclusion mismatch between `trades.csv` and `fills.jsonl`.

## Are EV(R) values mathematically miscomputed?

**Verdict:** the EV(R) arithmetic is internally consistent with the recorded `trades.csv` values, but the denominator (`risk_amount`) is potentially distorted by metadata rewriting logic.

So:

- **Not an averaging bug** in `ev_r_net/ev_r_gross` computation.
- **Potentially a risk-denominator semantics bug** (or at least a fragile normalization choice), which can make R magnitudes look unrealistic if `risk_amount = qty * stop_distance` is not the intended canonical risk basis for these instruments/settings.

## Practical conclusion for your L1-H10A outputs

1. The "FAILED but has metrics" runs are genuine reconciliation failures, not fake failures.
2. The EV(R) numbers are probably "correct given current trade rows," but not necessarily trustworthy as canonical risk-normalized outputs until risk denominator handling is fixed/confirmed.
3. Treat `FAIL` rows as invalid for model selection until cost reconciliation is resolved.
