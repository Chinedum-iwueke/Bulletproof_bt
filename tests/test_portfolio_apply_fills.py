from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Fill
from bt.portfolio.portfolio import Portfolio


def _fill(*, ts: pd.Timestamp, symbol: str, side: Side, qty: float, price: float, fee: float) -> Fill:
    return Fill(
        order_id="order",
        ts=ts,
        symbol=symbol,
        side=side,
        qty=qty,
        price=price,
        fee=fee,
        slippage=0.0,
        metadata={},
    )


def test_open_long_then_close_long_realized_pnl() -> None:
    portfolio = Portfolio(initial_cash=10000, max_leverage=2)
    ts_open = pd.Timestamp("2024-01-01T00:00:00Z")
    ts_close = pd.Timestamp("2024-01-01T01:00:00Z")

    portfolio.apply_fills([
        _fill(ts=ts_open, symbol="BTC", side=Side.BUY, qty=1, price=100, fee=1),
    ])
    portfolio.mark_to_market(
        {
            "BTC": Bar(
                ts=ts_open,
                symbol="BTC",
                open=100,
                high=100,
                low=100,
                close=100,
                volume=1,
            )
        }
    )
    portfolio.apply_fills([
        _fill(ts=ts_close, symbol="BTC", side=Side.SELL, qty=1, price=110, fee=1),
    ])

    assert portfolio.realized_pnl == 10
    assert portfolio.cash == 10000 - 2
    assert portfolio.equity == 10000 + 10 - 2


def test_open_short_then_close_short_realized_pnl() -> None:
    portfolio = Portfolio(initial_cash=10000, max_leverage=2)
    ts_open = pd.Timestamp("2024-01-01T00:00:00Z")
    ts_close = pd.Timestamp("2024-01-01T01:00:00Z")

    portfolio.apply_fills([
        _fill(ts=ts_open, symbol="ETH", side=Side.SELL, qty=2, price=100, fee=2),
    ])
    portfolio.apply_fills([
        _fill(ts=ts_close, symbol="ETH", side=Side.BUY, qty=2, price=90, fee=2),
    ])

    assert portfolio.realized_pnl == 20
    assert portfolio.cash == 10000 - 4
    assert portfolio.equity == 10000 + 20 - 4


def test_flip_position() -> None:
    portfolio = Portfolio(initial_cash=10000, max_leverage=2)
    ts_open = pd.Timestamp("2024-01-01T00:00:00Z")
    ts_flip = pd.Timestamp("2024-01-01T01:00:00Z")

    portfolio.apply_fills([
        _fill(ts=ts_open, symbol="BTC", side=Side.BUY, qty=1, price=100, fee=0),
    ])
    trades = portfolio.apply_fills([
        _fill(ts=ts_flip, symbol="BTC", side=Side.SELL, qty=2, price=110, fee=0),
    ])

    position = portfolio.position_book.get("BTC")

    assert len(trades) == 1
    assert position.side == Side.SELL
    assert position.qty == 1
    assert position.avg_entry_price == 110
