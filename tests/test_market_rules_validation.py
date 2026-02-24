from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from bt.data.market_rules import parse_market_rules, validate_market_timestamp
from bt.data.symbol_source import SymbolDataSource


def _utc(ts: str) -> datetime:
    return pd.Timestamp(ts).tz_convert("UTC").to_pydatetime()


def test_fx_weekend_rejection() -> None:
    rules = parse_market_rules({"data": {"market": "fx_24x5", "allow_weekend_bars": False}})
    saturday = _utc("2024-01-06T12:00:00Z")

    with pytest.raises(ValueError, match=r"fx_24x5.*allow_weekend_bars"):
        validate_market_timestamp(
            market_rules=rules,
            symbol="EURUSD",
            ts_utc=saturday,
            path="/tmp/dataset/symbols/EURUSD.parquet",
        )


def test_fx_allow_weekend_override() -> None:
    rules = parse_market_rules({"data": {"market": "fx_24x5", "allow_weekend_bars": True}})
    saturday = _utc("2024-01-06T12:00:00Z")
    validate_market_timestamp(
        market_rules=rules,
        symbol="EURUSD",
        ts_utc=saturday,
        path="/tmp/dataset/symbols/EURUSD.parquet",
    )


def test_equity_session_rejects_outside_hours() -> None:
    rules = parse_market_rules(
        {
            "data": {
                "market": "equity_session",
                "equity_session": {
                    "timezone": "America/New_York",
                    "open_time": "09:30",
                    "close_time": "16:00",
                    "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
                },
            }
        }
    )
    # 20:00 UTC in winter -> 15:00 NY? use 23:00 UTC -> 18:00 NY outside
    outside = _utc("2024-01-02T23:00:00Z")
    with pytest.raises(ValueError, match=r"local=.*equity_session.*data\.equity_session"):
        validate_market_timestamp(
            market_rules=rules,
            symbol="AAPL",
            ts_utc=outside,
            path="/tmp/dataset/symbols/AAPL.parquet",
        )


def test_equity_session_allows_inside_hours() -> None:
    rules = parse_market_rules(
        {
            "data": {
                "market": "equity_session",
                "equity_session": {
                    "timezone": "America/New_York",
                    "open_time": "09:30",
                    "close_time": "16:00",
                    "trading_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
                },
            }
        }
    )
    inside = _utc("2024-01-02T15:00:00Z")  # 10:00 NY
    validate_market_timestamp(
        market_rules=rules,
        symbol="AAPL",
        ts_utc=inside,
        path="/tmp/dataset/symbols/AAPL.parquet",
    )


def test_symbol_data_source_streaming_enforces_market_rules(tmp_path: Path) -> None:
    path = tmp_path / "EURUSD.parquet"
    frame = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2024-01-06T12:00:00Z"),
                "open": 1.10,
                "high": 1.11,
                "low": 1.09,
                "close": 1.10,
                "volume": 1000.0,
                "symbol": "EURUSD",
            }
        ]
    )
    frame.to_parquet(path, index=False)

    rules = parse_market_rules({"data": {"market": "fx_24x5", "allow_weekend_bars": False}})
    source = SymbolDataSource("EURUSD", str(path), market_rules=rules)

    with pytest.raises(ValueError, match=r"fx_24x5.*allow_weekend_bars"):
        list(source)
