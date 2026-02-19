from __future__ import annotations

import json

import pandas as pd
import pytest

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.reject_codes import RISK_FALLBACK_LEGACY_PROXY
from bt.risk.risk_engine import RiskEngine


def _bar() -> Bar:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    return Bar(ts=ts, symbol="BTC", open=100.0, high=106.0, low=95.0, close=100.0, volume=1.0)


def _signal(*, metadata: dict | None = None) -> Signal:
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    return Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="entry", confidence=1.0, metadata=metadata or {})


def _engine(*, stop_resolution: str, allow_legacy_proxy: bool) -> RiskEngine:
    return RiskEngine(
        max_positions=3,
        config={
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 0.01,
                "qty_rounding": "none",
                "stop": {},
                "stop_resolution": stop_resolution,
                "allow_legacy_proxy": allow_legacy_proxy,
            }
        },
    )


def _run(engine: RiskEngine, signal: Signal) -> tuple:
    bar = _bar()
    return engine.signal_to_order_intent(
        ts=bar.ts,
        signal=signal,
        bar=bar,
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )


def test_strict_mode_missing_stop_raises_actionable_error() -> None:
    engine = _engine(stop_resolution="strict", allow_legacy_proxy=False)

    with pytest.raises(ValueError, match=r"StrategyContractError") as excinfo:
        _run(engine, _signal())

    msg = str(excinfo.value)
    assert "stop_spec" in msg or "stop_price" in msg
    assert "risk.stop_resolution" in msg
    assert "strict" in msg
    assert "BTC" in msg


def test_safe_mode_missing_stop_without_allow_legacy_proxy_raises() -> None:
    engine = _engine(stop_resolution="safe", allow_legacy_proxy=False)

    with pytest.raises(ValueError, match=r"Safe mode is active but legacy proxy fallback is disabled") as excinfo:
        _run(engine, _signal())

    msg = str(excinfo.value)
    assert "risk.allow_legacy_proxy: true" in msg
    assert "stop_spec/stop_price" in msg


def test_safe_mode_missing_stop_with_allow_legacy_proxy_proceeds_and_labels_fallback() -> None:
    engine = _engine(stop_resolution="safe", allow_legacy_proxy=True)

    order_intent, reason = _run(engine, _signal())

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["used_legacy_stop_proxy"] is True
    assert order_intent.metadata["r_metrics_valid"] is False
    assert order_intent.metadata["stop_reason_code"] == RISK_FALLBACK_LEGACY_PROXY


def test_explicit_stop_in_strict_mode_proceeds_and_marks_valid_r() -> None:
    engine = _engine(stop_resolution="strict", allow_legacy_proxy=False)

    order_intent, reason = _run(engine, _signal(metadata={"stop_price": 98.0}))

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["used_legacy_stop_proxy"] is False
    assert order_intent.metadata["r_metrics_valid"] is True
    assert float(order_intent.metadata["stop_distance"]) > 0


def test_atr_stop_not_ready_behavior_is_consistent() -> None:
    engine = _engine(stop_resolution="strict", allow_legacy_proxy=False)
    signal = _signal(metadata={"stop_spec": {"kind": "atr", "atr_multiple": 2.0}})

    with pytest.raises(ValueError, match=r"ATR.*not found in ctx"):
        _run(engine, signal)


def test_metadata_sequence_is_deterministic_for_identical_inputs() -> None:
    engine = _engine(stop_resolution="safe", allow_legacy_proxy=True)

    first_order, _ = _run(engine, _signal())
    second_order, _ = _run(engine, _signal())
    assert first_order is not None and second_order is not None

    def canonicalize(metadata: dict) -> str:
        rounded: dict[str, object] = {}
        for key in sorted(metadata):
            value = metadata[key]
            if isinstance(value, float):
                rounded[key] = round(value, 8)
            else:
                rounded[key] = value
        return json.dumps(rounded, sort_keys=True, separators=(",", ":"))

    assert canonicalize(first_order.metadata) == canonicalize(second_order.metadata)
