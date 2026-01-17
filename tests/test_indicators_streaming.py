"""Tests for streaming indicators."""
from __future__ import annotations

import pytest
import pandas as pd

from bt.core.types import Bar
from bt.indicators.atr import ATR
from bt.indicators.ema import EMA
from bt.indicators.vwap import VWAP


def _bar(ts: pd.Timestamp, *, open_: float, high: float, low: float, close: float, volume: float) -> Bar:
    return Bar(
        ts=ts,
        symbol="AAA",
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def test_ema_streaming_flat_series() -> None:
    ema = EMA(period=3)
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")

    for idx in range(2):
        bar = _bar(ts + pd.Timedelta(minutes=idx), open_=10, high=10, low=10, close=10, volume=1)
        ema.update(bar)
        assert ema.is_ready is False
        assert ema.value is None

    bar = _bar(ts + pd.Timedelta(minutes=2), open_=10, high=10, low=10, close=10, volume=1)
    ema.update(bar)
    assert ema.is_ready is True
    assert ema.value == pytest.approx(10.0)

    bar = _bar(ts + pd.Timedelta(minutes=3), open_=10, high=10, low=10, close=10, volume=1)
    ema.update(bar)
    assert ema.value == pytest.approx(10.0)


def test_atr_streaming_true_range() -> None:
    atr = ATR(period=3)
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")

    bars = [
        _bar(ts, open_=7, high=10, low=5, close=7, volume=1),
        _bar(ts + pd.Timedelta(minutes=1), open_=8, high=11, low=6, close=10, volume=1),
        _bar(ts + pd.Timedelta(minutes=2), open_=10, high=12, low=8, close=11, volume=1),
        _bar(ts + pd.Timedelta(minutes=3), open_=11, high=13, low=9, close=12, volume=1),
        _bar(ts + pd.Timedelta(minutes=4), open_=12, high=14, low=10, close=13, volume=1),
    ]

    for idx, bar in enumerate(bars[:3]):
        atr.update(bar)
        assert atr.is_ready is False
        assert atr.value is None

    atr.update(bars[3])
    assert atr.is_ready is True
    assert atr.value == pytest.approx(13 / 3)

    atr.update(bars[4])
    assert atr.value == pytest.approx((13 / 3 * 2 + 4) / 3)


def test_vwap_streaming_weighted_average() -> None:
    vwap = VWAP()
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")

    bar1 = _bar(ts, open_=9, high=10, low=8, close=9, volume=2)
    vwap.update(bar1)
    assert vwap.is_ready is True
    assert vwap.value == pytest.approx(9.0)

    bar2 = _bar(ts + pd.Timedelta(minutes=1), open_=11, high=12, low=10, close=11, volume=3)
    vwap.update(bar2)
    assert vwap.value == pytest.approx(10.2)
