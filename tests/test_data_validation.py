"""Tests for data validation."""
from __future__ import annotations

import pandas as pd
import pytest

from bt.core.types import Bar
from bt.data.feed import HistoricalDataFeed
from bt.data.validation import validate_bars_df, validate_no_gaps


def _bars_df(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_validate_rejects_naive_timestamps() -> None:
    df = _bars_df(
        [
            {
                "ts": pd.Timestamp("2020-01-01 00:00:00"),
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.5,
                "low": 0.5,
                "close": 1.2,
                "volume": 10.0,
            }
        ]
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        validate_bars_df(df)


def test_validate_rejects_duplicate_symbol_ts() -> None:
    ts = pd.Timestamp("2020-01-01 00:00:00", tz="UTC")
    df = _bars_df(
        [
            {
                "ts": ts,
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.5,
                "low": 0.5,
                "close": 1.2,
                "volume": 10.0,
            },
            {
                "ts": ts,
                "symbol": "AAA",
                "open": 1.1,
                "high": 1.6,
                "low": 0.6,
                "close": 1.3,
                "volume": 11.0,
            },
        ]
    )

    with pytest.raises(ValueError, match="Duplicate bars"):
        validate_bars_df(df)


def test_validate_rejects_bad_ohlc() -> None:
    ts = pd.Timestamp("2020-01-01 00:00:00", tz="UTC")
    df = _bars_df(
        [
            {
                "ts": ts,
                "symbol": "AAA",
                "open": 10.0,
                "high": 12.0,
                "low": 11.0,
                "close": 9.0,
                "volume": 10.0,
            }
        ]
    )

    with pytest.raises(ValueError, match="low must be"):
        validate_bars_df(df)


def test_validate_no_gaps_detects_missing_bar() -> None:
    df = _bars_df(
        [
            {
                "ts": pd.Timestamp("2020-01-01 00:00:00", tz="UTC"),
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.5,
                "low": 0.5,
                "close": 1.2,
                "volume": 10.0,
            },
            {
                "ts": pd.Timestamp("2020-01-01 00:02:00", tz="UTC"),
                "symbol": "AAA",
                "open": 1.1,
                "high": 1.6,
                "low": 0.6,
                "close": 1.3,
                "volume": 11.0,
            },
        ]
    )

    with pytest.raises(ValueError, match="Missing bars"):
        validate_no_gaps(df, freq="1min")


def test_historical_data_feed_emits_bars_by_global_timestamp() -> None:
    t0 = pd.Timestamp("2020-01-01 00:00:00", tz="UTC")
    t1 = pd.Timestamp("2020-01-01 00:01:00", tz="UTC")
    t2 = pd.Timestamp("2020-01-01 00:02:00", tz="UTC")
    df = _bars_df(
        [
            {
                "ts": t0,
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.5,
                "low": 0.5,
                "close": 1.2,
                "volume": 10.0,
            },
            {
                "ts": t1,
                "symbol": "AAA",
                "open": 1.1,
                "high": 1.6,
                "low": 0.6,
                "close": 1.3,
                "volume": 11.0,
            },
            {
                "ts": t0,
                "symbol": "BBB",
                "open": 2.0,
                "high": 2.5,
                "low": 1.5,
                "close": 2.2,
                "volume": 20.0,
            },
            {
                "ts": t2,
                "symbol": "BBB",
                "open": 2.1,
                "high": 2.6,
                "low": 1.6,
                "close": 2.3,
                "volume": 21.0,
            },
        ]
    )

    feed = HistoricalDataFeed(df)

    first = feed.next()
    assert first is not None
    assert {bar.symbol for bar in first} == {"AAA", "BBB"}
    assert all(isinstance(bar, Bar) for bar in first)
    assert {bar.ts for bar in first} == {t0}

    second = feed.next()
    assert second is not None
    assert [bar.symbol for bar in second] == ["AAA"]
    assert {bar.ts for bar in second} == {t1}

    third = feed.next()
    assert third is not None
    assert [bar.symbol for bar in third] == ["BBB"]
    assert {bar.ts for bar in third} == {t2}

    assert feed.next() is None
