from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.reject_codes import (
    INSUFFICIENT_FREE_MARGIN,
    MAX_NOTIONAL_EXCEEDED,
    MAX_NOTIONAL_PCT_EQUITY_EXCEEDED,
    MAX_POSITIONS_REACHED,
    MIN_STOP_DISTANCE_VIOLATION,
    STOP_FALLBACK_LEGACY_PROXY,
    STOP_UNRESOLVABLE_SAFE_NO_PROXY,
    STOP_UNRESOLVABLE_STRICT,
    is_known,
)
import bt.risk.reject_codes as reject_codes
from bt.risk.risk_engine import RiskEngine


def test_reject_codes_are_known_and_stable() -> None:
    required = [
        STOP_UNRESOLVABLE_STRICT,
        STOP_UNRESOLVABLE_SAFE_NO_PROXY,
        STOP_FALLBACK_LEGACY_PROXY,
        INSUFFICIENT_FREE_MARGIN,
        MAX_POSITIONS_REACHED,
        MAX_NOTIONAL_EXCEEDED,
        MAX_NOTIONAL_PCT_EQUITY_EXCEEDED,
        MIN_STOP_DISTANCE_VIOLATION,
    ]
    for code in required:
        assert is_known(code)

    values = [
        value
        for name, value in vars(reject_codes).items()
        if name.isupper() and isinstance(value, str) and not name.startswith("RISK_REJECT_") and name != "RISK_FALLBACK_LEGACY_PROXY"
    ]
    assert len(values) == len(set(values))


def test_risk_engine_emits_canonical_stop_unresolvable_code(tmp_path) -> None:
    del tmp_path
    engine = RiskEngine(
        max_positions=5,
        config={
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 0.01,
                "qty_rounding": "none",
                "stop": {},
                "stop_resolution": "strict",
                "allow_legacy_proxy": False,
            }
        },
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=110.0, low=100.0, close=105.0, volume=1.0)
    signal = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={})

    try:
        engine.signal_to_order_intent(
            ts=ts,
            signal=signal,
            bar=bar,
            equity=10_000.0,
            free_margin=10_000.0,
            open_positions=0,
            max_leverage=2.0,
            current_qty=0.0,
        )
        raise AssertionError("Expected stop unresolvable strict error")
    except ValueError as exc:
        assert str(exc).startswith(f"{STOP_UNRESOLVABLE_STRICT}:")


def test_risk_engine_emits_canonical_margin_rejection_code(tmp_path) -> None:
    del tmp_path
    engine = RiskEngine(
        max_positions=5,
        config={
            "risk": {
                "mode": "r_fixed",
                "r_per_trade": 0.02,
                "qty_rounding": "none",
                "stop_resolution": "safe",
                "allow_legacy_proxy": True,
                "stop": {"mode": "legacy_proxy"},
            }
        },
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=110.0, low=95.0, close=105.0, volume=1.0)
    signal = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={})

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000.0,
        free_margin=0.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )
    assert order_intent is None
    assert reason == INSUFFICIENT_FREE_MARGIN
