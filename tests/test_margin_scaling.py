from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine


def test_margin_scaling_applies_and_approves() -> None:
    engine = RiskEngine(
        max_positions=5,
                taker_fee_bps=10.0,
        slippage_k_proxy=0.001,
        margin_buffer_tier=2,
        config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none", "stop": {}}},
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=101.0, low=100.0, close=100.0, volume=1.0)
    signal = Signal(
        ts=ts,
        symbol="BTC",
        side=Side.BUY,
        signal_type="unit",
        confidence=1.0,
        metadata={"stop_price": 99.0},
    )

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000.0,
        free_margin=150.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is not None
    assert reason == "risk_approved"
    assert order_intent.metadata["scaled_by_margin"] is True

    expected_max_affordable_qty = 150.0 / (100.0 * (0.5 + 0.001 + 0.001 + 0.01))
    assert abs(order_intent.qty) == pytest.approx(expected_max_affordable_qty)
    assert order_intent.metadata["margin_required"] <= 150.0
