from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine


def _ts() -> pd.Timestamp:
    return pd.Timestamp("2024-01-01T00:00:00Z")


def _bar(ts: pd.Timestamp) -> Bar:
    return Bar(ts=ts, symbol="BTC", open=100.0, high=110.0, low=90.0, close=100.0, volume=1.0)


def _engine(stop_resolution: str = "strict") -> RiskEngine:
    return RiskEngine(
        max_positions=5,
        config={
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 0.01,
                "qty_rounding": "none",
                "stop": {},
                "stop_resolution": stop_resolution,
            }
        },
    )


def test_exit_signals_bypass_stop_resolution() -> None:
    ts = _ts()
    engine = _engine("strict")
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.SELL,
        signal_type="strategy_exit",
        confidence=1.0,
        metadata={},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=_bar(ts),
        equity=10_000.0,
        free_margin=0.0,
        open_positions=1,
        max_leverage=2.0,
        current_qty=2.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved:close_only"
    assert order_intent.qty == -2.0
    assert order_intent.metadata["stop_resolution_skipped"] is True
    assert order_intent.metadata["stop_resolution_skip_reason"] == "exit_signal"


def test_entry_missing_stop_in_strict_mode_is_rejected_not_crash() -> None:
    ts = _ts()
    engine = _engine("strict")
    signal = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={})

    result_one = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=_bar(ts),
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )
    result_two = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=_bar(ts),
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert result_one == result_two
    order_intent, reason = result_one
    assert order_intent is None
    assert reason.startswith("risk_rejected:stop_unresolvable:strict")
    assert "ENTRY_requires_explicit_stop_price" in reason
    assert "signal_type=unit" in reason


def test_allow_legacy_proxy_sets_used_legacy_stop_proxy_flag() -> None:
    ts = _ts()
    engine = _engine("allow_legacy_proxy")
    signal = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={})

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=_bar(ts),
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["used_legacy_stop_proxy"] is True
    assert "legacy" in str(order_intent.metadata["stop_source"])


def test_successful_entry_sizing_includes_stop_distance_and_source_metadata() -> None:
    ts = _ts()
    engine = _engine("strict")
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        signal_type="unit",
        confidence=1.0,
        metadata={"stop_price": 95.0},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=_bar(ts),
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert float(order_intent.metadata["stop_distance"]) > 0
    assert order_intent.metadata["stop_source"] == "explicit_stop_price"
    assert float(order_intent.metadata["risk_amount"]) > 0
