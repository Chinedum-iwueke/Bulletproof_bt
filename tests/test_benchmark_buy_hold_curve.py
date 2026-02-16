from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from bt.benchmark import EquityPoint, compute_buy_hold_equity


@dataclass(frozen=True)
class _BarLike:
    ts: datetime
    symbol: str
    open: float
    close: float


def test_buy_hold_curve_basic_close() -> None:
    bars = [
        _BarLike(ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc), symbol="BTCUSDT", open=99.0, close=100.0),
        _BarLike(ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc), symbol="BTCUSDT", open=111.0, close=110.0),
        _BarLike(ts=datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc), symbol="BTCUSDT", open=88.0, close=90.0),
    ]

    result = compute_buy_hold_equity(bars=bars, initial_equity=1000, price_field="close")

    assert result == [
        EquityPoint(ts=bars[0].ts, equity=1000.0),
        EquityPoint(ts=bars[1].ts, equity=1100.0),
        EquityPoint(ts=bars[2].ts, equity=900.0),
    ]


def test_buy_hold_curve_uses_open_when_configured() -> None:
    bars = [
        _BarLike(ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc), symbol="ETHUSDT", open=50.0, close=100.0),
        _BarLike(ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc), symbol="ETHUSDT", open=55.0, close=110.0),
        _BarLike(ts=datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc), symbol="ETHUSDT", open=45.0, close=90.0),
    ]

    result = compute_buy_hold_equity(bars=bars, initial_equity=1000, price_field="open")

    assert result == [
        EquityPoint(ts=bars[0].ts, equity=1000.0),
        EquityPoint(ts=bars[1].ts, equity=1100.0),
        EquityPoint(ts=bars[2].ts, equity=900.0),
    ]


def test_raises_on_no_bars() -> None:
    with pytest.raises(ValueError, match="No bars provided"):
        compute_buy_hold_equity(bars=[], initial_equity=1000)


def test_raises_on_non_positive_price() -> None:
    bars = [
        _BarLike(ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc), symbol="BTCUSDT", open=100.0, close=0.0),
    ]

    with pytest.raises(ValueError, match="price") as exc_info:
        compute_buy_hold_equity(bars=bars, initial_equity=1000, price_field="close")

    assert "close" in str(exc_info.value)


def test_requires_tz_aware_utc_ts() -> None:
    bars = [
        _BarLike(ts=datetime(2024, 1, 1, 0, 0), symbol="BTCUSDT", open=100.0, close=100.0),
    ]

    with pytest.raises(ValueError, match="tz-aware UTC"):
        compute_buy_hold_equity(bars=bars, initial_equity=1000)
