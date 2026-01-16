"""Strict validation for market data."""
from __future__ import annotations

import pandas as pd

from bt.data.schema import REQUIRED_COLUMNS


def validate_bars(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    assert not missing, f"Missing columns: {missing}"
    assert df["timestamp"].is_monotonic_increasing, "Timestamps must be monotonic"
    assert not df.duplicated(subset=["timestamp", "symbol"]).any(), "Duplicate bars found"
    assert (df["high"] >= df[["open", "close"]].max(axis=1)).all()
    assert (df["low"] <= df[["open", "close"]].min(axis=1)).all()
    assert (df["high"] >= df["low"]).all()
