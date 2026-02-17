from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Order
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.execution.spread import apply_spread


def _bar(*, ts: pd.Timestamp, symbol: str, close: float = 100.0, volume: float = 1000.0) -> Bar:
    return Bar(
        ts=ts,
        symbol=symbol,
        open=close,
        high=close + 5.0,
        low=close - 5.0,
        close=close,
        volume=volume,
    )


def _order(*, ts: pd.Timestamp, symbol: str, side: Side) -> Order:
    return Order(
        id=f"order-{symbol}-{side.value}",
        ts_submitted=ts,
        symbol=symbol,
        side=side,
        qty=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.NEW,
        metadata={},
    )


def test_apply_spread_none_mode_returns_price_unchanged() -> None:
    adjusted = apply_spread(
        mode="none",
        spread_bps=10.0,
        price=100.0,
        bar_high=105.0,
        bar_low=95.0,
        side="buy",
    )
    assert adjusted == 100.0


def test_apply_spread_fixed_bps_buy_and_sell() -> None:
    buy_px = apply_spread(
        mode="fixed_bps",
        spread_bps=100.0,
        price=100.0,
        bar_high=105.0,
        bar_low=95.0,
        side="buy",
    )
    sell_px = apply_spread(
        mode="fixed_bps",
        spread_bps=100.0,
        price=100.0,
        bar_high=105.0,
        bar_low=95.0,
        side="sell",
    )
    assert buy_px == pytest.approx(101.0)
    assert sell_px == pytest.approx(99.0)


def test_apply_spread_bar_range_proxy() -> None:
    buy_px = apply_spread(
        mode="bar_range_proxy",
        spread_bps=0.0,
        price=100.0,
        bar_high=105.0,
        bar_low=95.0,
        side="buy",
    )
    sell_px = apply_spread(
        mode="bar_range_proxy",
        spread_bps=0.0,
        price=100.0,
        bar_high=105.0,
        bar_low=95.0,
        side="sell",
    )
    assert buy_px == pytest.approx(102.5)
    assert sell_px == pytest.approx(97.5)
    assert buy_px > 100.0
    assert sell_px < 100.0


def test_apply_spread_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="spread_bps"):
        apply_spread(
            mode="fixed_bps",
            spread_bps=-1.0,
            price=100.0,
            bar_high=105.0,
            bar_low=95.0,
            side="buy",
        )

    with pytest.raises(ValueError, match="bar_high"):
        apply_spread(
            mode="bar_range_proxy",
            spread_bps=0.0,
            price=100.0,
            bar_high=95.0,
            bar_low=105.0,
            side="buy",
        )


def test_integration_smoke_spread_before_slippage_and_fee() -> None:
    ts = pd.Timestamp("2024-01-04T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC")
    order = _order(ts=ts, symbol="BTC", side=Side.BUY)

    model = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=10.0),
        slippage_model=SlippageModel(k=1.0),
        spread_mode="fixed_bps",
        spread_bps=100.0,
        delay_bars=0,
    )

    _, fills = model.process(ts=ts, bars_by_symbol={"BTC": bar}, open_orders=[order])
    assert len(fills) == 1

    fill = fills[0]
    base_price = bar.high
    spread_only_price = base_price * 1.01
    assert fill.price > spread_only_price
    assert fill.fee > 0.0
    assert fill.metadata["spread_mode"] == "fixed_bps"
    assert fill.metadata["spread_cost"] == pytest.approx(spread_only_price - base_price)
