"""Backtest engine main event loop."""
from __future__ import annotations

from pathlib import Path
import csv

from bt.core.enums import OrderState
from bt.core.types import Order
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine


class BacktestEngine:
    """Event-driven backtest engine."""

    def __init__(
        self,
        *,
        datafeed: HistoricalDataFeed,
        universe: UniverseEngine,
        strategy: Strategy,
        risk: RiskEngine,
        execution: ExecutionModel,
        portfolio: Portfolio,
        decisions_writer: JsonlWriter,
        fills_writer: JsonlWriter,
        trades_writer: TradesCsvWriter,
        equity_path: Path,
        config: dict,
    ) -> None:
        self._datafeed = datafeed
        self._universe = universe
        self._strategy = strategy
        self._risk = risk
        self._execution = execution
        self._portfolio = portfolio
        self._decisions_writer = decisions_writer
        self._fills_writer = fills_writer
        self._trades_writer = trades_writer
        self._equity_path = equity_path
        self._config = config
        self._order_counter = 0

    def _next_order_id(self) -> str:
        self._order_counter += 1
        return f"order_{self._order_counter}"

    def _write_equity_header(self, writer: csv.writer) -> None:
        writer.writerow(
            [
                "ts",
                "cash",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "used_margin",
                "free_margin",
            ]
        )

    def run(self) -> None:
        """
        Loop:
        1) bars = feed.next()
        2) universe.update(...) for each bar
        3) build bars_by_symbol dict for this ts
        4) strategy.on_bars(ts, bars_by_symbol, tradeable_set)
        5) for each Signal: risk.signal_to_order_intent(...)
        6) turn OrderIntent into Order and submit to open_orders list
        7) execution.process(ts, bars_by_symbol, open_orders) -> (open_orders, fills)
        8) portfolio.apply_fills(fills) -> trades_closed
        9) portfolio.mark_to_market(bars_by_symbol)
        10) log decisions, fills, trades, and equity per timestamp
        """
        open_orders: list[Order] = []
        self._equity_path.parent.mkdir(parents=True, exist_ok=True)
        with self._equity_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if self._equity_path.stat().st_size == 0:
                self._write_equity_header(writer)

            while True:
                bars = self._datafeed.next()
                if bars is None:
                    break

                if isinstance(bars, dict):
                    bars_by_symbol = bars
                    bars_list = list(bars.values())
                else:
                    bars_list = list(bars)
                    bars_by_symbol = {bar.symbol: bar for bar in bars_list}

                if not bars_list:
                    continue

                ts = bars_list[0].ts

                for bar in bars_list:
                    self._universe.update(bar)

                tradeable = self._universe.tradeable_at(ts)
                signals = self._strategy.on_bars(ts, bars_by_symbol, tradeable)

                for signal in signals:
                    bar = bars_by_symbol.get(signal.symbol)
                    if bar is None:
                        decision_reason = "risk_rejected:no_bar"
                        self._decisions_writer.write(
                            {
                                "ts": ts,
                                "symbol": signal.symbol,
                                "signal": signal,
                                "approved": False,
                                "reason": decision_reason,
                            }
                        )
                        continue

                    open_positions = self._portfolio.position_book.open_positions_count()
                    order_intent, decision_reason = self._risk.signal_to_order_intent(
                        ts=ts,
                        signal=signal,
                        bar=bar,
                        equity=self._portfolio.equity,
                        open_positions=open_positions,
                    )

                    if order_intent is None:
                        self._decisions_writer.write(
                            {
                                "ts": ts,
                                "symbol": signal.symbol,
                                "signal": signal,
                                "approved": False,
                                "reason": decision_reason,
                            }
                        )
                        continue

                    order = Order(
                        id=self._next_order_id(),
                        ts_submitted=ts,
                        symbol=order_intent.symbol,
                        side=order_intent.side,
                        qty=order_intent.qty,
                        order_type=order_intent.order_type,
                        limit_price=order_intent.limit_price,
                        state=OrderState.NEW,
                        metadata=dict(order_intent.metadata),
                    )
                    open_orders.append(order)

                    self._decisions_writer.write(
                        {
                            "ts": ts,
                            "symbol": signal.symbol,
                            "signal": signal,
                            "approved": True,
                            "reason": decision_reason,
                            "order_qty": order_intent.qty,
                            "notional_est": order_intent.metadata.get("notional_est"),
                            "order": order,
                        }
                    )

                open_orders, fills = self._execution.process(
                    ts=ts,
                    bars_by_symbol=bars_by_symbol,
                    open_orders=open_orders,
                )
                open_orders = [
                    order
                    for order in open_orders
                    if order.state
                    not in {
                        OrderState.FILLED,
                        OrderState.CANCELLED,
                        OrderState.REJECTED,
                    }
                ]

                for fill in fills:
                    self._fills_writer.write(
                        {
                            "ts": fill.ts,
                            "symbol": fill.symbol,
                            "order_id": fill.order_id,
                            "side": fill.side,
                            "qty": fill.qty,
                            "price": fill.price,
                            "fee": fill.fee,
                            "slippage": fill.slippage,
                            "metadata": fill.metadata,
                        }
                    )

                trades_closed = self._portfolio.apply_fills(fills)
                for trade in trades_closed:
                    self._trades_writer.write_trade(trade)

                self._portfolio.mark_to_market(bars_by_symbol)

                writer.writerow(
                    [
                        ts.isoformat(),
                        self._portfolio.cash,
                        self._portfolio.equity,
                        self._portfolio.realized_pnl,
                        self._portfolio.unrealized_pnl,
                        self._portfolio.used_margin,
                        self._portfolio.free_margin,
                    ]
                )
                handle.flush()

        self._decisions_writer.close()
        self._fills_writer.close()
        self._trades_writer.close()
