# FX/Traditional V1 Freeze Contract

## FX Currency Assumption (V1)

V1 FX sizing is deterministic but has limited currency conversion support.

- If `instrument.account_currency == instrument.quote_currency`, sizing works under the quote-currency assumption.
- If account and quote differ, V1 requires explicit pip value input (`instrument.pip_value` or `risk.fx.pip_value_override`) because automatic conversion is not implemented.

Example V1 FX config (quote-currency assumption):

```yaml
instrument:
  type: forex
  symbol: EURUSD
  tick_size: 0.0001
  pip_size: 0.0001
  contract_size: 100000
  account_currency: USD
  quote_currency: USD
  pip_value: null  # optional under quote-currency assumption
risk:
  mode: r_fixed
  r_per_trade: 0.01
  fx:
    lot_step: 0.01
```

Example when account currency differs from quote:

```yaml
instrument:
  type: forex
  symbol: EURUSD
  contract_size: 100000
  account_currency: EUR
  quote_currency: USD
  pip_value: 10.0  # explicit conversion input required in V1
```

If you violate this assumption, sizing fails with a `ValueError` indicating conversion input is required (set `instrument.pip_value` or `risk.fx.pip_value_override`).

## Execution Pack / Profile Interaction Rules

Execution profiles are enforced by `execution.profile`:

- Preset profiles (`tier1`, `tier2`, `tier3`) are locked presets.
- `custom` is the only profile that allows explicit override fields.

Forbidden override keys for non-`custom` profiles:

- `execution.maker_fee`
- `execution.taker_fee`
- `execution.slippage_bps`
- `execution.delay_bars`
- `execution.spread_bps`

Legacy top-level override keys (`maker_fee_bps`, `taker_fee_bps`, `signal_delay_bars`, `fixed_bps`) also require `execution.profile=custom` when used as overrides.

For FX/equity runs: if your spread/fee/slippage/delay assumptions must differ from a preset, use `execution.profile=custom` and provide all required override fields.

| `execution.profile` | Includes | Per-run direct override keys allowed? |
| --- | --- | --- |
| `tier1` | maker/taker fees, slippage, delay, spread preset | No |
| `tier2` | maker/taker fees, slippage, delay, spread preset | No |
| `tier3` | maker/taker fees, slippage, delay, spread preset | No |
| `custom` | user-specified maker/taker fees, slippage, delay, spread | Yes (all required) |

## Margin / Leverage / Liquidation Policy (V1)

- Margin usage is modeled from notional and leverage (`used_margin = notional / max_leverage`).
- FX sizing and futures-style leverage usage rely on margin-aware checks via risk + portfolio accounting.
- Liquidation behavior:
  - Forced liquidation path can trigger on negative free margin when `risk.may_liquidate=true`.
  - End-of-run flatten always closes remaining positions and is tagged separately from forced liquidation.

Equity cash-account style in V1:

- Use conservative leverage assumptions (e.g., `risk.margin.leverage` unset or `1.0`) for cash-account behavior.
- Orders that cannot satisfy risk/margin checks are rejected before fill (with deterministic reject reasons / metadata).

Artifacts and where to inspect:

- Margin utilization metrics: `performance.json` (`margin.*` section) and `equity.csv` (`used_margin`, `free_margin`).
- Liquidation evidence: `fills.jsonl` / `decisions.jsonl` / `trades.csv` metadata and run-level summary in `summary.txt` + `run_status.json`.

## Portfolio Scope Limitation (V1)

V1 freeze scope is intentionally limited:

- Supports single-strategy backtest evaluation per run.
- Supports multi-symbol universes for strategy evaluation.
- Excludes portfolio optimization/allocation engines, multi-strategy blending, and multi-broker comparison.
- Excludes tick-level simulation and FX swap/rollover financing modeling.

## Freeze Achieved Declaration

### FX/Traditional V1 Feature Freeze Achieved When:

- Deterministic regression tests pass for three fixture cases:
  - crypto baseline
  - FX baseline
  - equity baseline
- Each case runs twice with matching normalized outputs and required artifacts present.
- The regression harness is CI-safe (small local fixtures, no internet, deterministic outputs).
