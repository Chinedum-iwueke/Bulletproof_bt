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


def test_round_trip_trade_metadata_preserves_explicit_risk_amount() -> None:
    book = PositionBook()
    book.apply_fill(_fill(ts="2024-01-01T00:00:00Z", side=Side.BUY, qty=2.0, price=100.0, risk_amount=500.0, stop_distance=25.0))
    _, trade = book.apply_fill(_fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=2.0, price=105.0))

    assert trade is not None
    assert float(trade.metadata["entry_qty"]) == pytest.approx(2.0)
    assert float(trade.metadata["entry_stop_distance"]) == pytest.approx(25.0)
    assert float(trade.metadata["risk_amount"]) == pytest.approx(500.0)


def test_partial_close_trade_keeps_full_entry_risk_context() -> None:
    book = PositionBook()
    book.apply_fill(_fill(ts="2024-01-01T00:00:00Z", side=Side.BUY, qty=1.0, price=100.0, risk_amount=500.0, stop_distance=50.0))
    position, trade = book.apply_fill(_fill(ts="2024-01-01T00:30:00Z", side=Side.SELL, qty=0.4, price=104.0))

    assert trade is None
    assert position.qty == pytest.approx(0.6)

    _, trade2 = book.apply_fill(_fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=0.6, price=106.0))
    assert trade2 is not None
    assert float(trade2.metadata["entry_qty"]) == pytest.approx(1.0)
    assert float(trade2.metadata["risk_amount"]) == pytest.approx(500.0)


def test_flip_trade_preserves_old_entry_risk_and_new_leg_gets_new_context() -> None:
    book = PositionBook()
    book.apply_fill(_fill(ts="2024-01-01T00:00:00Z", side=Side.BUY, qty=1.0, price=100.0, risk_amount=500.0, stop_distance=50.0))
    new_pos, closed_trade = book.apply_fill(
        _fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=1.5, price=98.0, risk_amount=999.0, stop_distance=20.0)
    )

    assert closed_trade is not None
    assert float(closed_trade.metadata["entry_qty"]) == pytest.approx(1.0)
    assert float(closed_trade.metadata["risk_amount"]) == pytest.approx(500.0)

    assert new_pos.side == Side.SELL
    assert new_pos.qty == pytest.approx(0.5)
    _, close_new = book.apply_fill(_fill(ts="2024-01-01T02:00:00Z", side=Side.BUY, qty=0.5, price=97.0))
    assert close_new is not None
    assert float(close_new.metadata["entry_qty"]) == pytest.approx(0.5)
    assert float(close_new.metadata["entry_stop_distance"]) == pytest.approx(20.0)
    assert float(close_new.metadata["risk_amount"]) == pytest.approx(999.0)


def test_risk_amount_falls_back_to_entry_qty_times_stop_distance_when_missing() -> None:
    book = PositionBook()
    book.apply_fill(_fill(ts="2024-01-01T00:00:00Z", side=Side.BUY, qty=2.0, price=100.0, stop_distance=25.0))
    _, trade = book.apply_fill(_fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=2.0, price=105.0))

    assert trade is not None
    assert float(trade.metadata["entry_qty"]) == pytest.approx(2.0)
    assert float(trade.metadata["entry_stop_distance"]) == pytest.approx(25.0)
    assert float(trade.metadata["risk_amount"]) == pytest.approx(50.0)


def test_entry_stop_distance_is_preserved_when_stop_distance_key_missing() -> None:
    book = PositionBook()
    open_fill = Fill(
        order_id="o",
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        side=Side.BUY,
        qty=1.0,
        price=100.0,
        fee=0.0,
        slippage=0.0,
        metadata={"entry_stop_distance": 12.5, "risk_amount": 125.0},
    )
    book.apply_fill(open_fill)
    _, trade = book.apply_fill(_fill(ts="2024-01-01T00:30:00Z", side=Side.SELL, qty=1.0, price=101.0))

    assert trade is not None
    assert float(trade.metadata["entry_stop_distance"]) == pytest.approx(12.5)
    assert float(trade.metadata["risk_amount"]) == pytest.approx(125.0)


def test_entry_context_metadata_survives_until_trade_close() -> None:
    book = PositionBook()
    book.apply_fill(
        Fill(
            order_id="o",
            ts=pd.Timestamp("2024-01-01T00:00:00Z"),
            symbol="AAA",
            side=Side.BUY,
            qty=1.0,
            price=100.0,
            fee=0.0,
            slippage=0.0,
            metadata={
                "risk_amount": 10.0,
                "stop_distance": 10.0,
                "strategy": "l1_h1_vol_floor_trend",
                "rv_t": 0.012,
                "vol_pct_t": 0.91,
                "gate_pass": True,
                "trend_dir_t": 1,
                "atr_entry": 5.0,
                "tp_price": 110.0,
                "tp_distance": 10.0,
                "signal_bars_held": 48,
                "vwap_t": 101.5,
                "custom_quality_score": 0.77,
            },
        )
    )

    _, trade = book.apply_fill(_fill(ts="2024-01-01T01:00:00Z", side=Side.SELL, qty=1.0, price=103.0))

    assert trade is not None
    assert trade.metadata["strategy"] == "l1_h1_vol_floor_trend"
    assert float(trade.metadata["rv_t"]) == pytest.approx(0.012)
    assert float(trade.metadata["vol_pct_t"]) == pytest.approx(0.91)
    assert trade.metadata["gate_pass"] is True
    assert trade.metadata["trend_dir_t"] == 1
    assert float(trade.metadata["atr_entry"]) == pytest.approx(5.0)
    assert float(trade.metadata["tp_price"]) == pytest.approx(110.0)
    assert trade.metadata["signal_bars_held"] == 48
    assert float(trade.metadata["vwap_t"]) == pytest.approx(101.5)
    assert float(trade.metadata["custom_quality_score"]) == pytest.approx(0.77)
