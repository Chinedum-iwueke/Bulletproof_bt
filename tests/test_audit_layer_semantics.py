from __future__ import annotations

import pandas as pd

from bt.audit.fill_audit import inspect_fill
from bt.audit.order_audit import inspect_order
from bt.audit.position_audit import inspect_position
from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Bar, Fill, Order, Position


def test_order_audit_market_order_without_price_reference_is_not_violation() -> None:
    order = Order(
        id="o1",
        ts_submitted=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        side=Side.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.NEW,
        metadata={},
    )

    _, violations = inspect_order(ts=order.ts_submitted, order=order)

    assert violations == []


def test_order_audit_limit_order_missing_price_reference_is_violation() -> None:
    order = Order(
        id="o2",
        ts_submitted=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        side=Side.BUY,
        qty=1.0,
        order_type=OrderType.LIMIT,
        limit_price=100.0,
        state=OrderState.NEW,
        metadata={},
    )

    _, violations = inspect_order(ts=order.ts_submitted, order=order)

    assert len(violations) == 1
    assert violations[0]["type"] == "missing_price_reference"


def test_fill_audit_allows_small_slippage_beyond_bar_extrema() -> None:
    bar = Bar(
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=10.0,
    )
    fill = Fill(
        order_id="o1",
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        side=Side.BUY,
        qty=1.0,
        price=101.03,
        fee=0.0,
        slippage=0.0,
        metadata={},
    )

    violations = inspect_fill(ts=fill.ts, fill=fill, bar=bar, tolerance_bps=5.0)

    assert violations == []


def test_fill_audit_flags_large_out_of_range_fill() -> None:
    bar = Bar(
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=10.0,
    )
    fill = Fill(
        order_id="o1",
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        side=Side.BUY,
        qty=1.0,
        price=102.0,
        fee=0.0,
        slippage=0.0,
        metadata={},
    )

    violations = inspect_fill(ts=fill.ts, fill=fill, bar=bar, tolerance_bps=5.0)

    assert len(violations) == 1
    assert violations[0]["type"] == "fill_outside_bar"


def test_position_audit_short_position_is_not_violation() -> None:
    position = Position(
        symbol="AAA",
        state=PositionState.OPEN,
        side=Side.SELL,
        qty=1.0,
        avg_entry_price=100.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        mae_price=100.0,
        mfe_price=100.0,
        opened_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        closed_ts=None,
    )

    violations = inspect_position("AAA", position)

    assert violations == []


def test_position_audit_missing_side_for_open_position_is_violation() -> None:
    position = Position(
        symbol="AAA",
        state=PositionState.OPEN,
        side=None,
        qty=1.0,
        avg_entry_price=100.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        mae_price=100.0,
        mfe_price=100.0,
        opened_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        closed_ts=None,
    )

    violations = inspect_position("AAA", position)

    assert len(violations) == 1
    assert violations[0]["type"] == "missing_side_for_open_position"
