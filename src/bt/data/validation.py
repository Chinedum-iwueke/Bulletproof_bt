"""Strict validation for market data."""
from __future__ import annotations

import pandas as pd

from bt.data.schema import BAR_COLUMNS


def _ensure_utc_series(ts: pd.Series) -> None:
    if pd.api.types.is_datetime64tz_dtype(ts):
        if str(ts.dt.tz) != "UTC":
            raise ValueError("ts must be in UTC")
        return
    if pd.api.types.is_datetime64_dtype(ts):
        raise ValueError("ts must be timezone-aware UTC")
    if not ts.map(lambda value: isinstance(value, pd.Timestamp)).all():
        raise ValueError("ts must contain pandas Timestamps")
    if ts.map(lambda value: value.tz is None).any():
        raise ValueError("ts must be timezone-aware UTC")
    if ts.map(lambda value: str(value.tz) != "UTC").any():
        raise ValueError("ts must be in UTC")


def validate_bars_df(df: pd.DataFrame) -> None:
    missing = [col for col in BAR_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if df[list(BAR_COLUMNS)].isna().any().any():
        raise ValueError("Required columns must not contain NaNs")

    _ensure_utc_series(df["ts"])

    if df.duplicated(subset=["symbol", "ts"]).any():
        raise ValueError("Duplicate bars found for symbol/ts")

    symbols = df["symbol"]
    if not symbols.map(lambda value: isinstance(value, str)).all():
        raise ValueError("Symbol must be a string")
    if symbols.map(lambda value: value.strip() == "").any():
        raise ValueError("Symbol must be non-empty")

    for symbol, group in df.groupby("symbol", sort=False):
        ts = group["ts"]
        if not ts.is_monotonic_increasing:
            raise ValueError(f"Timestamps must be increasing for symbol {symbol}")
        if not ts.is_unique:
            raise ValueError(f"Duplicate timestamps for symbol {symbol}")

    lows = df["low"]
    highs = df["high"]
    opens = df["open"]
    closes = df["close"]
    min_oc = pd.concat([opens, closes], axis=1).min(axis=1)
    max_oc = pd.concat([opens, closes], axis=1).max(axis=1)

    if not (lows <= min_oc).all():
        raise ValueError("low must be <= min(open, close)")
    if not (highs >= max_oc).all():
        raise ValueError("high must be >= max(open, close)")
    if not (highs >= lows).all():
        raise ValueError("high must be >= low")
    if not (df["volume"] >= 0).all():
        raise ValueError("volume must be >= 0")


def validate_no_gaps(df: pd.DataFrame, freq: str) -> None:
    for symbol, group in df.groupby("symbol", sort=False):
        ts = group["ts"].sort_values()
        expected = pd.date_range(start=ts.iloc[0], end=ts.iloc[-1], freq=freq, tz="UTC")
        missing = expected.difference(ts)
        if not missing.empty:
            first_missing = missing[0]
            raise ValueError(
                "Missing bars for symbol "
                f"{symbol}: first missing {first_missing}, "
                f"count {len(missing)}"
            )
