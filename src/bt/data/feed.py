"""Historical data feed emitting bars by timestamp."""
from __future__ import annotations

from typing import Iterable, Iterator

import pandas as pd

from bt.core.types import Bar


def iter_bars(df: pd.DataFrame) -> Iterator[Bar]:
    for _, row in df.iterrows():
        yield Bar(
            timestamp=row["timestamp"],
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        )
