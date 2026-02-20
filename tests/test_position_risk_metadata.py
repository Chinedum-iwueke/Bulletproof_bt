from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import Side
from bt.core.types import Fill
from bt.portfolio.position import PositionBook


def _fill(*, ts: str, side: Side, qty: float, price: float, risk_amount: float | None = None, stop_distance: float | None = None) -> Fill:
    metadata: dict[str, float] = {}
    if risk_amount is not None:
        metadata["risk_amount"] = risk_amount
    if stop_distance is not None:
        metadata["stop_distance"] = stop_distance
    return Fill(
        order_id="o",
        ts=pd.Timestamp(ts),
        symbol="AAA",
        side=side,
        qty=qty,
        price=price,
        fee=0.0,
        slippage=0.0,
        metadata=metadata,
    )


def test_round_trip_trade_metadata_uses_entry_qty_times_stop_distance() -> None:
    book = PositionBook()
    book.apply_fill(_fill(ts="2024-01-01T00:00:00Z", side=Side.BUY, qty=2.0, price=100.0, risk_amount=500.0, stop_distance=25.0))
    _, trade = book.apply_fill(_fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=2.0, price=105.0))

    assert trade is not None
    assert float(trade.metadata["entry_qty"]) == pytest.approx(2.0)
    assert float(trade.metadata["entry_stop_distance"]) == pytest.approx(25.0)
    assert float(trade.metadata["risk_amount"]) == pytest.approx(50.0)


def test_partial_close_trade_keeps_full_entry_risk_context() -> None:
    book = PositionBook()
    book.apply_fill(_fill(ts="2024-01-01T00:00:00Z", side=Side.BUY, qty=1.0, price=100.0, risk_amount=500.0, stop_distance=50.0))
    position, trade = book.apply_fill(_fill(ts="2024-01-01T00:30:00Z", side=Side.SELL, qty=0.4, price=104.0))

    assert trade is None
    assert position.qty == pytest.approx(0.6)

    _, trade2 = book.apply_fill(_fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=0.6, price=106.0))
    assert trade2 is not None
    assert float(trade2.metadata["entry_qty"]) == pytest.approx(1.0)
    assert float(trade2.metadata["risk_amount"]) == pytest.approx(50.0)


def test_flip_trade_preserves_old_entry_risk_and_new_leg_gets_new_context() -> None:
    book = PositionBook()
    book.apply_fill(_fill(ts="2024-01-01T00:00:00Z", side=Side.BUY, qty=1.0, price=100.0, risk_amount=500.0, stop_distance=50.0))
    new_pos, closed_trade = book.apply_fill(
        _fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=1.5, price=98.0, risk_amount=999.0, stop_distance=20.0)
    )

    assert closed_trade is not None
    assert float(closed_trade.metadata["entry_qty"]) == pytest.approx(1.0)
    assert float(closed_trade.metadata["risk_amount"]) == pytest.approx(50.0)

    assert new_pos.side == Side.SELL
    assert new_pos.qty == pytest.approx(0.5)
    _, close_new = book.apply_fill(_fill(ts="2024-01-01T02:00:00Z", side=Side.BUY, qty=0.5, price=97.0))
    assert close_new is not None
    assert float(close_new.metadata["entry_qty"]) == pytest.approx(0.5)
    assert float(close_new.metadata["entry_stop_distance"]) == pytest.approx(20.0)
    assert float(close_new.metadata["risk_amount"]) == pytest.approx(10.0)
