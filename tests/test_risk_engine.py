from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import OrderType, Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine


def _bar(*, ts: pd.Timestamp, symbol: str, high: float, low: float, close: float) -> Bar:
    return Bar(
        ts=ts,
        symbol=symbol,
        open=low,
        high=high,
        low=low,
        close=close,
        volume=1.0,
    )


def _signal(*, ts: pd.Timestamp, symbol: str, side: Side | None) -> Signal:
    return Signal(
        ts=ts,
        symbol=symbol,
        side=side,
        signal_type="unit",
        confidence=1.0,
        metadata={},
    )


def test_signal_to_order_intent_approves_and_sizes() -> None:
    engine = RiskEngine(max_positions=5, risk_per_trade_pct=0.01)
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.order_type == OrderType.MARKET
    assert order_intent.limit_price is None
    assert order_intent.qty == pytest.approx(10.0)


def test_signal_to_order_intent_rejects_max_positions() -> None:
    engine = RiskEngine(max_positions=1, risk_per_trade_pct=0.01)
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=1,
        max_leverage=2.0,
    )

    assert order_intent is None
    assert reason.startswith("risk_rejected:")


def test_signal_to_order_intent_applies_notional_cap() -> None:
    engine = RiskEngine(max_positions=5, risk_per_trade_pct=0.5, max_notional_per_symbol=500)
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["cap_applied"] is True
    assert order_intent.metadata["notional_est"] <= 500 + 1e-9


def test_signal_to_order_intent_rejects_no_side() -> None:
    engine = RiskEngine(max_positions=5, risk_per_trade_pct=0.01)
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=None)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
    )

    assert order_intent is None
    assert "no_side" in reason


def test_signal_to_order_intent_scales_when_margin_insufficient() -> None:
    engine = RiskEngine(max_positions=5, risk_per_trade_pct=0.01)
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=100.5, low=100.0, close=100.0)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=50,
        open_positions=0,
        max_leverage=2.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is True
    assert order_intent.metadata["margin_required"] <= 50 + 1e-9
    assert order_intent.qty == pytest.approx(1.0)


def test_signal_to_order_intent_allows_when_margin_sufficient() -> None:
    engine = RiskEngine(max_positions=5, risk_per_trade_pct=0.01)
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = _bar(ts=ts, symbol="BTC", high=110, low=100, close=105)
    signal = _signal(ts=ts, symbol="BTC", side=Side.BUY)

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000,
        free_margin=10_000,
        open_positions=0,
        max_leverage=2.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is False
    assert order_intent.qty == pytest.approx(10.0)
