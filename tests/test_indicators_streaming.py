"""Deterministic tests for streaming indicator library."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bt.core.types import Bar
from bt.indicators import make_indicator
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.bollinger import BollingerBands
from bt.indicators.donchian import DonchianChannel
from bt.indicators.macd import MACD
from bt.indicators.obv import OBV
from bt.indicators.rsi import RSI
from bt.indicators.sma import SMA


def _bar(ts: pd.Timestamp, close: float, volume: float = 100.0) -> Bar:
    return Bar(ts=ts, symbol="AAA", open=close - 0.5, high=close + 1.0, low=close - 1.0, close=close, volume=volume)


def _bars(n: int = 80) -> list[Bar]:
    ts0 = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    close = np.linspace(100, 120, n) + 2 * np.sin(np.arange(n) / 3)
    vol = 100 + (np.arange(n) % 7) * 20
    return [_bar(ts0 + pd.Timedelta(minutes=i), float(close[i]), float(vol[i])) for i in range(n)]


def test_no_value_before_warmup() -> None:
    checks = [SMA(5), RSI(14), MACD(), BollingerBands(20), DMIADX(14)]
    data = _bars(15)
    for ind in checks:
        for bar in data:
            ind.update(bar)
            if ind._bars_seen < ind.warmup_bars:  # noqa: SLF001
                assert ind.is_ready is False
                assert ind.value is None


def test_streaming_matches_vectorized_references() -> None:
    bars = _bars(120)
    df = pd.DataFrame({
        "close": [b.close for b in bars],
    })

    sma = SMA(10)
    rsi = RSI(14)
    macd = MACD(12, 26, 9)
    bb = BollingerBands(20, 2.0)
    for b in bars:
        sma.update(b); rsi.update(b); macd.update(b); bb.update(b)

    ref_sma = df["close"].rolling(10).mean().iloc[-1]
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    ref_rsi = 100 - (100 / (1 + (avg_gain / avg_loss)))
    ema_fast = df["close"].ewm(span=12, adjust=False, min_periods=12).mean()
    ema_slow = df["close"].ewm(span=26, adjust=False, min_periods=26).mean()
    ref_macd_line = ema_fast - ema_slow
    ref_signal = ref_macd_line.ewm(span=9, adjust=False, min_periods=9).mean()
    ref_mid = df["close"].rolling(20).mean().iloc[-1]

    assert sma.value == pytest.approx(float(ref_sma), rel=1e-6)
    assert rsi.value == pytest.approx(float(ref_rsi.iloc[-1]), rel=2e-2)
    assert macd.get("macd") == pytest.approx(float(ref_macd_line.iloc[-1]), rel=2e-2)
    assert macd.get("signal") == pytest.approx(float(ref_signal.iloc[-1]), rel=2e-2)
    assert bb.get("mid") == pytest.approx(float(ref_mid), rel=1e-6)


def test_missing_bars_not_filled() -> None:
    ind = DonchianChannel(3)
    ts = pd.Timestamp("2024-01-01", tz="UTC")
    bars = [
        _bar(ts, 100),
        _bar(ts + pd.Timedelta(minutes=1), 101),
        _bar(ts + pd.Timedelta(minutes=10), 99),
    ]
    for b in bars:
        ind.update(b)
    assert ind.is_ready is True
    assert ind.get("upper") == pytest.approx(max(b.high for b in bars[-3:]))


def test_determinism_same_inputs_same_outputs() -> None:
    bars = _bars(60)
    one = OBV()
    two = OBV()
    for b in bars:
        one.update(b); two.update(b)
    assert f"{one.value:.8f}" == f"{two.value:.8f}"


def test_registry_factory() -> None:
    rsi = make_indicator("rsi", period=14)
    assert isinstance(rsi, RSI)
    assert rsi.name == "rsi_14"

    macd = make_indicator("macd", fast=12, slow=26, signal=9)
    assert isinstance(macd, MACD)
