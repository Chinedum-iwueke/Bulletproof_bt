from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from bt.core.enums import Side
from bt.core.types import Fill
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio


def test_costs_roll_up_to_trade_and_csv(tmp_path: Path) -> None:
    portfolio = Portfolio(initial_cash=10_000, max_leverage=2)
    ts_open = pd.Timestamp("2024-01-01T00:00:00Z")
    ts_close = pd.Timestamp("2024-01-01T01:00:00Z")

    open_fee = 1.25
    open_slippage = 0.4
    close_fee = 0.75
    close_slippage = 0.6

    portfolio.apply_fills([
        Fill(
            order_id="order-open",
            ts=ts_open,
            symbol="BTC",
            side=Side.BUY,
            qty=1.0,
            price=100.0,
            fee=open_fee,
            slippage=open_slippage,
            metadata={},
        )
    ])
    trades = portfolio.apply_fills([
        Fill(
            order_id="order-close",
            ts=ts_close,
            symbol="BTC",
            side=Side.SELL,
            qty=1.0,
            price=110.0,
            fee=close_fee,
            slippage=close_slippage,
            metadata={},
        )
    ])

    assert len(trades) == 1
    trade = trades[0]

    total_fees = open_fee + close_fee
    total_slippage = open_slippage + close_slippage

    assert portfolio.cash == pytest.approx(portfolio.initial_cash - total_fees)
    assert trade.fees == pytest.approx(total_fees)
    assert trade.slippage == pytest.approx(total_slippage)

    path = tmp_path / "trades.csv"
    writer = TradesCsvWriter(path)
    writer.write_trade(trade)
    writer.close()

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    fees_index = TradesCsvWriter._columns.index("fees")
    slippage_index = TradesCsvWriter._columns.index("slippage")
    assert float(rows[1][fees_index]) == pytest.approx(total_fees)
    assert float(rows[1][slippage_index]) == pytest.approx(total_slippage)
