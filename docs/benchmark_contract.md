# Benchmark Contract

Supported benchmark modes:

- `buy_hold` (default when `benchmark.enabled=true` and `benchmark.type` omitted)
- `flat`
- `baseline_strategy` (currently supports `ma_cross`)

## Intended use
- Crypto: typically `buy_hold`
- FX: typically `flat` or `baseline_strategy`
- Equity: typically `buy_hold`

Benchmarks are context tools for comparison, **not** portfolio allocators.

## Mode semantics
- `buy_hold`: invest initial equity at first benchmark bar; no further trading.
- `flat`: constant equity equal to initial equity across benchmark timestamps.
- `baseline_strategy`:
  - deterministic long-only baseline strategy,
  - `ma_cross` uses params `{fast, slow}` and trades with full-equity exposure,
  - no slippage/spread/fees in benchmark baseline execution.

## Validation
- `benchmark.type=buy_hold` requires `benchmark.symbol` when enabled.
- `benchmark.type=baseline_strategy` requires `benchmark.baseline_strategy.name`.
- Unknown baseline names are rejected with supported-name list.
