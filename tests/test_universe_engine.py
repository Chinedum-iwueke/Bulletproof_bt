"""Tests for UniverseEngine."""
from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.universe.universe import UniverseEngine


def _bar(ts: pd.Timestamp, symbol: str, volume: float) -> Bar:
    return Bar(
        ts=ts,
        symbol=symbol,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=volume,
    )


def test_universe_requires_history() -> None:
    engine = UniverseEngine(min_history_bars=3, lookback_bars=2, lag_bars=0)
    t0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    t1 = pd.Timestamp("2024-01-01 00:01:00", tz="UTC")

    engine.update(_bar(t0, "AAA", 10.0))
    engine.update(_bar(t1, "AAA", 10.0))

    assert engine.tradeable_at(t1) == set()


def test_universe_volume_threshold() -> None:
    engine = UniverseEngine(
        min_history_bars=2,
        lookback_bars=2,
        min_avg_volume=50.0,
        lag_bars=0,
    )
    t0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    t1 = pd.Timestamp("2024-01-01 00:01:00", tz="UTC")

    for ts in (t0, t1):
        engine.update(_bar(ts, "AAA", 100.0))
        engine.update(_bar(ts, "BBB", 10.0))

    assert engine.tradeable_at(t1) == {"AAA"}


def test_universe_lag_prevents_peeking() -> None:
    engine = UniverseEngine(
        min_history_bars=2,
        lookback_bars=2,
        min_avg_volume=100.0,
        lag_bars=1,
    )
    t0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    t1 = pd.Timestamp("2024-01-01 00:01:00", tz="UTC")
    t2 = pd.Timestamp("2024-01-01 00:02:00", tz="UTC")
    t3 = pd.Timestamp("2024-01-01 00:03:00", tz="UTC")

    engine.update(_bar(t0, "AAA", 1.0))
    engine.update(_bar(t1, "AAA", 1.0))
    engine.update(_bar(t2, "AAA", 1000.0))

    assert engine.tradeable_at(t2) == set()

    engine.update(_bar(t3, "AAA", 1.0))
    assert engine.tradeable_at(t3) == {"AAA"}


def test_universe_handles_missing_bars() -> None:
    engine = UniverseEngine(
        min_history_bars=2,
        lookback_bars=2,
        min_avg_volume=5.0,
        lag_bars=0,
    )
    t0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    t1 = pd.Timestamp("2024-01-01 00:01:00", tz="UTC")
    t2 = pd.Timestamp("2024-01-01 00:02:00", tz="UTC")

    engine.update(_bar(t0, "AAA", 10.0))
    engine.update(_bar(t0, "BBB", 1.0))
    engine.update(_bar(t1, "AAA", 10.0))
    engine.update(_bar(t2, "AAA", 10.0))
    engine.update(_bar(t2, "BBB", 1.0))

    assert engine.tradeable_at(t1) == {"AAA"}
    assert engine.tradeable_at(t2) == {"AAA"}
