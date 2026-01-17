"""Historical data feed emitting bars by timestamp."""
from __future__ import annotations

from collections import defaultdict

import pandas as pd

from bt.core.types import Bar


class HistoricalDataFeed:
    def __init__(self, bars: pd.DataFrame) -> None:
        self._bars = bars
        self._index = 0
        self._timestamps: list[pd.Timestamp] = sorted(bars["ts"].unique())
        self._rows_by_ts: dict[pd.Timestamp, list[pd.Series]] = defaultdict(list)
        for _, row in bars.iterrows():
            self._rows_by_ts[row["ts"]].append(row)

    def symbols(self) -> list[str]:
        return sorted(self._bars["symbol"].unique().tolist())

    def reset(self) -> None:
        self._index = 0

    def next(self) -> list[Bar] | None:
        if self._index >= len(self._timestamps):
            return None
        ts = self._timestamps[self._index]
        self._index += 1
        rows = self._rows_by_ts[ts]
        return [
            Bar(
                ts=row["ts"],
                symbol=row["symbol"],
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
            for row in rows
        ]
