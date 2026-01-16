"""Schema definitions for market data."""
from __future__ import annotations

import pandas as pd

BAR_COLUMNS: tuple[str, ...] = (
    "ts",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

DTYPES: dict[str, str] = {
    "ts": "datetime64[ns, UTC]",
    "symbol": "string",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
}


_RENAME_MAP = {
    "timestamp": "ts",
    "time": "ts",
    "date": "ts",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "vol": "volume",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {col: _RENAME_MAP.get(col.lower(), col.lower()) for col in df.columns}
    return df.rename(columns=renamed)
