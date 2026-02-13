from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.strategy.context_view import StrategyContextView
from bt.strategy.templates.client_strategy_template import ClientEmaTemplateStrategy


def test_client_strategy_template_smoke() -> None:
    strategy = ClientEmaTemplateStrategy(symbol="BTCUSDT", timeframe="15m", confidence=0.7)
    ts = pd.Timestamp("2025-01-01T00:00:00Z")
    bar = Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        volume=10.0,
    )
    ctx = StrategyContextView({"htf": {"15m": {"BTCUSDT": bar}}})

    signals = strategy.on_bars(ts, {"BTCUSDT": bar}, {"BTCUSDT"}, ctx)

    assert isinstance(signals, list)
    assert len(signals) == 1
