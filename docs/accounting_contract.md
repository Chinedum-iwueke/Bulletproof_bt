# Accounting Contract

## Stable contract (client-facing)

### Canonical definitions
- **Executed fill price (`fills.jsonl.price`)**: final execution price after intrabar choice, spread adjustment, and slippage adjustment.
- **Fee debit location**: fees are debited from portfolio cash at fill application time.
- **Trade PnL fields**:
  - `trade.pnl` / `trades.csv.pnl_price`: gross price PnL (before fees)
  - `trade.fees` / `trades.csv.fees_paid`: accumulated entry+exit fees for the closed trade
  - `trades.csv.pnl_net`: `pnl_price - fees_paid`

Repo Evidence: `src/bt/execution/execution_model.py::ExecutionModel.process`, `src/bt/portfolio/portfolio.py::apply_fills`, `src/bt/logging/trades.py::TradesCsvWriter.write_trade`, `src/bt/portfolio/position.py::_build_trade`.

### Equity identity
Portfolio recalculation uses:

`equity = cash + realized_pnl + unrealized_pnl`

Repo Evidence: `src/bt/portfolio/portfolio.py::_recalculate_state`.

### Reconciliation guidance
Expected reconciliation chain:
1. `performance.json.final_equity` equals the last equity point from `equity.csv`.
2. `performance.json.gross_pnl/net_pnl/fee_total/slippage_total/spread_total` are computed from artifacts via `compute_cost_attribution`.
3. `trades.csv` fields align with gross/net trade equations (`pnl_net = pnl_price - fees_paid`).

Potential expected mismatches:
- If you add external/manual adjustments not represented in run artifacts, external sheets will diverge.

Repo Evidence: `src/bt/metrics/performance.py::compute_performance`, `src/bt/metrics/performance.py::compute_cost_attribution`, `src/bt/logging/trades.py::TradesCsvWriter`.

## Artifact mapping table
| Artifact | Key fields | Meaning |
| --- | --- | --- |
| `fills.jsonl` | `price`, `fee_cost`, `slippage_cost`, `spread_cost`, metadata | Per-fill executed economics |
| `trades.csv` | `pnl_price`, `fees_paid`, `pnl_net`, `slippage` | Closed-trade ledger |
| `equity.csv` | `cash`, `equity`, `realized_pnl`, `unrealized_pnl` | Time-series account state |
| `performance.json` | `final_equity`, `gross_pnl`, `net_pnl`, cost totals | Aggregate report |
| `summary.txt` | human summary from run artifacts | quick review, not authoritative for math |

Repo Evidence: `src/bt/logging/jsonl.py::_with_canonical_fill_costs`, `src/bt/logging/trades.py::TradesCsvWriter._columns`, `src/bt/core/engine.py::_write_equity_header`, `src/bt/metrics/performance.py::write_performance_artifacts`, `src/bt/logging/summary.py::write_summary_txt`.

## FAQ / common failure modes
- **I’m double-counting costs in my spreadsheet.**  
  Use either net fields (`pnl_net`) or gross fields plus explicit costs, not both.

- **Why does realized PnL not already include fees?**  
  Realized PnL tracks price movement; fees are debited via cash and carried separately in trade/performance cost fields.

- **Spread/slippage attribution seems separate from realized PnL.**  
  Correct: execution price already reflects spread/slippage, and explicit cost attribution is also reported for analysis.

Repo Evidence: `src/bt/portfolio/portfolio.py`, `src/bt/portfolio/position.py`, `src/bt/metrics/performance.py`.

## Repo Evidence index
- `src/bt/execution/execution_model.py::ExecutionModel.process`
- `src/bt/portfolio/portfolio.py::_recalculate_state`
- `src/bt/portfolio/portfolio.py::apply_fills`
- `src/bt/portfolio/position.py::_build_trade`
- `src/bt/logging/trades.py::TradesCsvWriter`
- `src/bt/logging/jsonl.py::_with_canonical_fill_costs`
- `src/bt/metrics/performance.py::compute_performance`
