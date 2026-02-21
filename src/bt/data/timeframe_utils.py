"""Utilities for validated timeframe alignment checks."""
from __future__ import annotations

import pandas as pd

from bt.data.resample import normalize_timeframe


_TIMEFRAME_TO_FREQ: dict[str, str] = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


def is_timeframe_boundary(ts: pd.Timestamp, timeframe: str) -> bool:
    """Return ``True`` when ``ts`` is aligned to the given timeframe boundary."""
    normalized = normalize_timeframe(timeframe, key_path="timeframe")
    return ts == ts.floor(_TIMEFRAME_TO_FREQ[normalized])

