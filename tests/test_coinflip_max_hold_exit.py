from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.coinflip import CoinFlipStrategy


def _bar(ts: pd.Timestamp, symbol: str) -> Bar:
    return Bar(ts=ts, symbol=symbol, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0)


def test_coinflip_emits_exit_after_max_hold() -> None:
    ts0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    ts1 = pd.Timestamp("2024-01-01 00:01:00", tz="UTC")
    ts2 = pd.Timestamp("2024-01-01 00:02:00", tz="UTC")
    strat = CoinFlipStrategy(seed=1, p_trade=1.0, cooldown_bars=0, max_hold_bars=2)

    entry = strat.on_bars(ts0, {"A": _bar(ts0, "A")}, {"A"}, {})
    assert len(entry) == 1
    entry_side = entry[0].side

    no_exit_yet = strat.on_bars(ts1, {"A": _bar(ts1, "A")}, {"A"}, {"positions": {"A": {"side": entry_side.value}}})
    assert no_exit_yet == []

    exit_signals = strat.on_bars(ts2, {"A": _bar(ts2, "A")}, {"A"}, {"positions": {"A": {"side": entry_side.value}}})
    assert len(exit_signals) == 1
    assert exit_signals[0].signal_type == "coinflip_exit"
    assert exit_signals[0].side == (Side.SELL if entry_side == Side.BUY else Side.BUY)
    assert "stop_price" not in exit_signals[0].metadata
    assert exit_signals[0].metadata["close_only"] is True
    assert exit_signals[0].metadata["exit_reason"] == "max_hold_bars"
