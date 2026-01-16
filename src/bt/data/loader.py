"""Data loader for parquet/csv into in-memory format."""
from __future__ import annotations

import pandas as pd

from bt.data.validation import validate_bars


def load_bars(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    validate_bars(df)
    return df
