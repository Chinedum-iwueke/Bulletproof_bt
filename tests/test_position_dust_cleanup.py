from __future__ import annotations

import csv
import io
from pathlib import Path

import pandas as pd

from bt.core.engine import BacktestEngine
from bt.core.enums import PositionState, Side
from bt.core.types import Bar, Fill, Position
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.constants import QTY_EPSILON
from bt.portfolio.portfolio import Portfolio
from bt.portfolio.position import PositionBook
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy
from bt.universe.universe import UniverseEngine


class NoopStrategy(Strategy):
    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list:
        return []


def _fill(*, side: Side, qty: float, price: float = 100.0, order_id: str = "o1") -> Fill:
    return Fill(
        order_id=order_id,
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        side=side,
        qty=qty,
        price=price,
        fee=0.0,
        slippage=0.0,
        metadata={},
    )


def test_reduce_clamps_tiny_residual_to_zero() -> None:
    book = PositionBook()

    book.apply_fill(_fill(side=Side.BUY, qty=1.0, order_id="open"))
    position, trade = book.apply_fill(_fill(side=Side.SELL, qty=0.9999999999995, order_id="close"))

    assert trade is not None
    assert position.qty == 0.0
    assert "AAA" not in book.all_positions()


def test_flip_does_not_leave_dust_position() -> None:
    book = PositionBook()

    book.apply_fill(_fill(side=Side.BUY, qty=1.0, order_id="open"))
    position, trade = book.apply_fill(_fill(side=Side.SELL, qty=1.3, order_id="flip"))

    assert trade is not None
    assert position.side == Side.SELL
    assert position.qty == 0.3
    assert abs(position.qty) > QTY_EPSILON


def test_end_of_run_liquidation_skips_dust_positions(tmp_path: Path, monkeypatch) -> None:
    bars = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "symbol": ["AAA"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [1000.0],
        }
    )
    engine = BacktestEngine(
        datafeed=HistoricalDataFeed(bars),
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=NoopStrategy(),
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop": {}}}),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
            slippage_model=SlippageModel(k=0.0),
            delay_bars=0,
        ),
        portfolio=Portfolio(initial_cash=10000.0),
        decisions_writer=JsonlWriter(tmp_path / "decisions.jsonl"),
        fills_writer=JsonlWriter(tmp_path / "fills.jsonl"),
        trades_writer=TradesCsvWriter(tmp_path / "trades.csv"),
        equity_path=tmp_path / "equity.csv",
        config={},
    )

    engine._portfolio.position_book._positions["AAA"] = Position(
        symbol="AAA",
        state=PositionState.OPEN,
        side=Side.BUY,
        qty=QTY_EPSILON / 2,
        avg_entry_price=100.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        mae_price=100.0,
        mfe_price=100.0,
        opened_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        closed_ts=None,
    )

    def _should_not_run(*args, **kwargs):
        raise AssertionError("Execution should not be called for dust-only liquidation")

    monkeypatch.setattr(engine._execution, "process", _should_not_run)

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    engine._force_liquidate_open_positions(
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        bars_by_symbol={},
        writer=writer,
        liquidation_reason="end_of_run",
    )

    assert buffer.getvalue() == ""


def test_meaningful_quantity_accounting_unchanged() -> None:
    portfolio = Portfolio(initial_cash=10000.0)
    open_fill = _fill(side=Side.BUY, qty=2.0, price=100.0, order_id="open")
    close_fill = _fill(side=Side.SELL, qty=2.0, price=110.0, order_id="close")

    trades = portfolio.apply_fills([open_fill])
    assert trades == []

    trades = portfolio.apply_fills([close_fill])

    assert len(trades) == 1
    assert trades[0].pnl == 20.0
    assert portfolio.realized_pnl == 20.0
    assert portfolio.equity == portfolio.cash + portfolio.realized_pnl + portfolio.unrealized_pnl
