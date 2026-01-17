from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Order
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel


def _bar(*, ts: pd.Timestamp, symbol: str) -> Bar:
    return Bar(
        ts=ts,
        symbol=symbol,
        open=100,
        high=110,
        low=90,
        close=100,
        volume=1000,
    )


def _order(*, ts: pd.Timestamp, symbol: str, order_type: OrderType) -> Order:
    return Order(
        id="order-1",
        ts_submitted=ts,
        symbol=symbol,
        side=Side.BUY,
        qty=1.0,
        order_type=order_type,
        limit_price=None,
        state=OrderState.NEW,
        metadata={},
    )


def test_fee_and_slippage_applied_with_delay_and_worst_case_fill() -> None:
    fee_model = FeeModel(maker_fee_bps=0.0, taker_fee_bps=10.0)
    slippage_model = SlippageModel(k=1.0)
    model = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=1,
    )

    t0 = pd.Timestamp("2024-01-01T00:00:00Z")
    t1 = pd.Timestamp("2024-01-01T01:00:00Z")
    bar = _bar(ts=t0, symbol="BTC")
    order = _order(ts=t0, symbol="BTC", order_type=OrderType.MARKET)

    updated_orders, fills = model.process(ts=t0, bars_by_symbol={"BTC": bar}, open_orders=[order])
    assert len(fills) == 0
    assert updated_orders[0].state == OrderState.SUBMITTED

    updated_orders, fills = model.process(ts=t1, bars_by_symbol={"BTC": bar}, open_orders=updated_orders)
    assert len(fills) == 1
    fill = fills[0]
    assert fill.price >= 110
    assert fill.fee > 0
    assert fill.slippage >= 0
    assert updated_orders[0].state == OrderState.FILLED


def test_missing_bar_preserves_delay_and_prevents_fill() -> None:
    fee_model = FeeModel(maker_fee_bps=0.0, taker_fee_bps=1.0)
    slippage_model = SlippageModel(k=1.0)
    model = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=1,
    )

    t0 = pd.Timestamp("2024-01-02T00:00:00Z")
    t1 = pd.Timestamp("2024-01-02T01:00:00Z")
    t2 = pd.Timestamp("2024-01-02T02:00:00Z")
    order = _order(ts=t0, symbol="ETH", order_type=OrderType.MARKET)

    updated_orders, fills = model.process(ts=t0, bars_by_symbol={}, open_orders=[order])
    assert len(fills) == 0
    assert updated_orders[0].metadata["delay_remaining"] == 1

    bar = _bar(ts=t1, symbol="ETH")
    updated_orders, fills = model.process(ts=t1, bars_by_symbol={"ETH": bar}, open_orders=updated_orders)
    assert len(fills) == 0
    assert updated_orders[0].metadata["delay_remaining"] == 0

    updated_orders, fills = model.process(ts=t2, bars_by_symbol={"ETH": bar}, open_orders=updated_orders)
    assert len(fills) == 1
    assert updated_orders[0].state == OrderState.FILLED


def test_limit_order_not_supported() -> None:
    fee_model = FeeModel(maker_fee_bps=0.0, taker_fee_bps=1.0)
    slippage_model = SlippageModel(k=1.0)
    model = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=0,
    )

    t0 = pd.Timestamp("2024-01-03T00:00:00Z")
    order = _order(ts=t0, symbol="BTC", order_type=OrderType.LIMIT)

    try:
        model.process(ts=t0, bars_by_symbol={"BTC": _bar(ts=t0, symbol="BTC")}, open_orders=[order])
    except NotImplementedError:
        assert True
    else:
        raise AssertionError("Expected NotImplementedError for limit orders.")
