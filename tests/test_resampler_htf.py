from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler


def _utc_ts(ts: str | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(ts)
    if stamp.tz is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp


def _bar(ts: str | pd.Timestamp, close: float, symbol: str = "BTCUSDT") -> Bar:
    t = _utc_ts(ts)
    return Bar(
        ts=t,
        symbol=symbol,
        open=close - 0.1,
        high=close + 0.2,
        low=close - 0.3,
        close=close,
        volume=10.0,
    )


def test_htf_bar_only_emitted_on_close() -> None:
    r = TimeframeResampler(timeframes=["5m"], strict=True)

    input_closes = [100.0, 101.0, 102.0, 103.0, 104.0]
    for minute, close in zip(["00:00", "00:01", "00:02", "00:03", "00:04"], input_closes):
        bars = r.update(_bar(f"2025-01-01 {minute}:00", close=close))
        assert bars == []

    emitted = r.update(_bar("2025-01-01 00:05:00", close=106.0))
    assert len(emitted) == 1
    htf = emitted[0]

    assert htf.ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    assert htf.timeframe == "5m"
    assert htf.open == 99.9
    assert htf.close == 104.0
    assert htf.high == 104.2
    assert htf.low == 99.7
    assert htf.volume == 50.0
    assert htf.is_complete is True
    assert htf.n_bars == 5


def test_missing_1m_bars_do_not_get_filled_in_strict_mode() -> None:
    r = TimeframeResampler(timeframes=["5m"], strict=True)

    for minute in ["00:00", "00:01", "00:03", "00:04"]:
        r.update(_bar(f"2025-01-01 {minute}:00", close=100.0))

    emitted = r.update(_bar("2025-01-01 00:05:00", close=101.0))
    assert emitted == []
    assert r.latest_closed("BTCUSDT", "5m") is None


def test_gap_across_buckets_marks_bucket_incomplete() -> None:
    r = TimeframeResampler(timeframes=["5m"], strict=True)

    for minute in ["00:00", "00:01", "00:02", "00:03", "00:04"]:
        r.update(_bar(f"2025-01-01 {minute}:00", close=100.0))

    first_emit = r.update(_bar("2025-01-01 00:05:00", close=101.0))
    assert len(first_emit) == 1
    assert first_emit[0].ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")

    r.update(_bar("2025-01-01 00:07:00", close=102.0))
    r.update(_bar("2025-01-01 00:08:00", close=103.0))
    r.update(_bar("2025-01-01 00:09:00", close=104.0))

    second_emit = r.update(_bar("2025-01-01 00:10:00", close=105.0))
    assert second_emit == []
    latest = r.latest_closed("BTCUSDT", "5m")
    assert latest is not None
    assert latest.ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")


def test_multitimeframe_emission_correctness() -> None:
    r = TimeframeResampler(timeframes=["5m", "15m"], strict=True)

    emitted_5m = []
    emitted_15m = []

    start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    for idx in range(16):
        ts = start + pd.Timedelta(minutes=idx)
        out = r.update(_bar(ts, close=100.0 + idx))
        for bar in out:
            if bar.timeframe == "5m":
                emitted_5m.append(bar)
            elif bar.timeframe == "15m":
                emitted_15m.append(bar)

    assert [b.ts for b in emitted_5m] == [
        pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
        pd.Timestamp("2025-01-01 00:05:00", tz="UTC"),
        pd.Timestamp("2025-01-01 00:10:00", tz="UTC"),
    ]
    assert [b.ts for b in emitted_15m] == [
        pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
    ]
    assert len(emitted_5m) == 3
    assert len(emitted_15m) == 1
