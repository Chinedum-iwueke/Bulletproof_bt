"""Tests for the coinflip strategy."""
from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.coinflip import CoinFlipStrategy


def _bar(ts: pd.Timestamp, symbol: str) -> Bar:
    return Bar(
        ts=ts,
        symbol=symbol,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1.0,
    )


def test_coinflip_emits_for_all_symbols_when_always_trade() -> None:
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    bars_by_symbol = {
        "A": _bar(ts, "A"),
        "B": _bar(ts, "B"),
    }
    tradeable = {"A", "B"}
    strat = CoinFlipStrategy(seed=123, p_trade=1.0, cooldown_bars=0)

    signals = strat.on_bars(ts, bars_by_symbol, tradeable)

    assert len(signals) == 2
    assert {signal.symbol for signal in signals} == {"A", "B"}
    assert all(signal.signal_type == "coinflip" for signal in signals)
    assert all(signal.confidence == 0.5 for signal in signals)
    assert all(signal.side in {Side.BUY, Side.SELL} for signal in signals)
    for signal in signals:
        assert signal.metadata["strategy"] == "coinflip"
        assert signal.metadata["seed"] == 123
        assert signal.metadata["p_trade"] == 1.0
        assert signal.metadata["cooldown_bars"] == 0


def test_coinflip_emits_none_when_never_trade() -> None:
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    bars_by_symbol = {"A": _bar(ts, "A"), "B": _bar(ts, "B")}
    tradeable = {"A", "B"}
    strat = CoinFlipStrategy(seed=123, p_trade=0.0, cooldown_bars=0)

    signals = strat.on_bars(ts, bars_by_symbol, tradeable)

    assert len(signals) == 0


def test_coinflip_cooldown_enforces_spacing() -> None:
    ts0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    ts1 = pd.Timestamp("2024-01-01 00:01:00", tz="UTC")
    ts2 = pd.Timestamp("2024-01-01 00:02:00", tz="UTC")
    bars_by_symbol_0 = {"A": _bar(ts0, "A")}
    bars_by_symbol_1 = {"A": _bar(ts1, "A")}
    bars_by_symbol_2 = {"A": _bar(ts2, "A")}
    tradeable = {"A"}
    strat = CoinFlipStrategy(seed=123, p_trade=1.0, cooldown_bars=2)

    signals_0 = strat.on_bars(ts0, bars_by_symbol_0, tradeable)
    signals_1 = strat.on_bars(ts1, bars_by_symbol_1, tradeable)
    signals_2 = strat.on_bars(ts2, bars_by_symbol_2, tradeable)

    assert len(signals_0) == 1
    assert len(signals_1) == 0
    assert len(signals_2) == 1
