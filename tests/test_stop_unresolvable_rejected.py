from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine


def test_stop_unresolvable_signal_is_rejected() -> None:
    engine = RiskEngine(
        max_positions=5,
                config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none", "stop": {}, "stop_resolution": "strict", "allow_legacy_proxy": False}},
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=110.0, low=100.0, close=105.0, volume=1.0)
    signal = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={})

    with pytest.raises(ValueError, match=r"StrategyContractError") as excinfo:
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

    reason = str(excinfo.value)
    assert "signal_type=unit" in reason
    assert "stop_price" in reason
