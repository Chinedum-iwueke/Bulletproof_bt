from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine


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


def test_allow_legacy_proxy_mode_approves_with_legacy_metadata() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
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
    assert order_intent.metadata["stop_source"] == "legacy_high_low_proxy"
    assert order_intent.metadata["used_legacy_stop_proxy"] is True
    assert order_intent.metadata["r_metrics_valid"] is False


def test_close_only_approves_and_uses_negative_current_qty() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    engine = _engine()
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.SELL,
        signal_type="coinflip_exit",
        confidence=1.0,
        metadata={"close_only": True},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=_bar(ts),
        equity=10_000.0,
        free_margin=0.0,
        open_positions=1,
        max_leverage=2.0,
        current_qty=3.5,
    )

    assert order_intent is not None
    assert reason == "risk_approved:close_only"
    assert order_intent.qty == -3.5


def test_qty_sign_invariant_rejects_mismatched_order_qty() -> None:
    assert not RiskEngine._qty_sign_invariant_ok(
        signal_side=Side.BUY,
        current_qty=0.0,
        flip=False,
        order_qty=-1.0,
        close_only=False,
    )


def test_exit_signal_suffix_bypasses_stop_resolution_in_strict_mode() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    engine = _engine("strict")
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.SELL,
        signal_type="h1_volfloor_donchian_exit",
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


def test_exit_signal_suffix_can_still_be_rejected_when_already_flat() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    engine = _engine("strict")
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.SELL,
        signal_type="h1_volfloor_donchian_exit",
        confidence=1.0,
        metadata={},
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

    assert order_intent is None
    assert reason == "risk_rejected:close_only_no_position"


def test_entry_signal_without_stop_still_rejects_in_strict_mode() -> None:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    engine = _engine("strict")
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

    assert order_intent is None
    assert reason.startswith("risk_rejected:stop_unresolvable:strict")
    assert "signal_type=unit" in reason
    assert "ENTRY_requires_explicit_stop_price" in reason
