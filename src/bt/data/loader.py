"""Data loader for parquet/csv into in-memory format."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.data.schema import BAR_COLUMNS, DTYPES, normalize_columns
from bt.data.validation import validate_bars_df


def _ensure_utc(ts: pd.Series) -> None:
    if pd.api.types.is_datetime64tz_dtype(ts):
        if str(ts.dt.tz) != "UTC":
            raise ValueError("ts must be in UTC")
        return
    if pd.api.types.is_datetime64_dtype(ts):
        raise ValueError("ts must be timezone-aware UTC")
    raise ValueError("ts must be timezone-aware UTC")


def load_bars(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError("Unsupported file type")

    df = normalize_columns(df)
    if "ts" not in df.columns:
        raise ValueError("Missing ts column after normalization")

    df["ts"] = pd.to_datetime(df["ts"], errors="raise")
    _ensure_utc(df["ts"])

    for col in BAR_COLUMNS:
        if col == "ts":
            continue
        if col in df.columns and col in DTYPES:
            df[col] = df[col].astype(DTYPES[col])

    df = df.sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)
    validate_bars_df(df)
    return df
