"""UTC timestamp helpers for research data."""
from __future__ import annotations

from datetime import timezone
from typing import Any

import pandas as pd


def utc_ts(value: Any) -> pd.Timestamp:
    """Return a timezone-aware UTC pandas Timestamp."""
    if value is None:
        raise ValueError("timestamp value cannot be None")
    if isinstance(value, str) and value.lower() == "now":
        return pd.Timestamp.now(tz="UTC")
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    return ts.tz_convert("UTC")


def utc_series(values: Any) -> pd.Series:
    """Normalize a scalar/list/Series of timestamps to datetime64[ns, UTC]."""
    parsed = pd.to_datetime(values, utc=True, format="mixed")
    if isinstance(parsed, pd.Series):
        return parsed
    return pd.Series(parsed)


def ms(ts: Any) -> int:
    """UTC timestamp in Unix milliseconds."""
    return int(utc_ts(ts).timestamp() * 1000)


def timeframe_to_pandas_freq(timeframe: str) -> str:
    if timeframe.endswith("m"):
        return f"{int(timeframe[:-1])}min"
    if timeframe.endswith("h"):
        return f"{int(timeframe[:-1])}h"
    if timeframe.endswith("d"):
        return f"{int(timeframe[:-1])}D"
    raise ValueError(f"unsupported timeframe: {timeframe}")


def timeframe_delta(timeframe: str) -> pd.Timedelta:
    return pd.Timedelta(timeframe_to_pandas_freq(timeframe))
