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


def test_free_margin_non_negative_after_mark() -> None:
    portfolio = Portfolio(initial_cash=1000, max_leverage=2)
    ts_open = pd.Timestamp("2024-01-01T00:00:00Z")

    portfolio.apply_fills([
        _fill(ts=ts_open, symbol="BTC", side=Side.BUY, qty=1, price=100, fee=0),
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

    assert portfolio.free_margin >= 0


def test_missing_bar_does_not_change_mark() -> None:
    portfolio = Portfolio(initial_cash=1000, max_leverage=2)
    ts_open = pd.Timestamp("2024-01-01T00:00:00Z")
    ts_mark = pd.Timestamp("2024-01-01T01:00:00Z")

    portfolio.apply_fills([
        _fill(ts=ts_open, symbol="ETH", side=Side.BUY, qty=1, price=100, fee=0),
    ])
    portfolio.mark_to_market(
        {
            "ETH": Bar(
                ts=ts_mark,
                symbol="ETH",
                open=105,
                high=105,
                low=105,
                close=105,
                volume=1,
            )
        }
    )
    unrealized_before = portfolio.unrealized_pnl

    portfolio.mark_to_market({})

    assert portfolio.unrealized_pnl == unrealized_before
