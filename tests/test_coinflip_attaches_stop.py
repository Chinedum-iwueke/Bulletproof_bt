from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.strategy.coinflip import CoinFlipStrategy


def test_coinflip_entry_signals_include_stop_fields() -> None:
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    bar = Bar(ts=ts, symbol="A", open=100.0, high=103.0, low=99.0, close=101.0, volume=1.0)
    strat = CoinFlipStrategy(seed=123, p_trade=1.0, cooldown_bars=0)

    signals = strat.on_bars(ts, {"A": bar}, {"A"}, {})

    assert len(signals) == 1
    metadata = signals[0].metadata
    assert "stop_price" in metadata
    assert metadata["stop_distance"] == 4.0
