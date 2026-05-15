"""Exchange adapter protocol."""
from __future__ import annotations

from typing import Protocol

import pandas as pd


class PerpExchangeAdapter(Protocol):
    exchange: str

    def fetch_usdt_perp_instruments(self) -> pd.DataFrame: ...

    def fetch_ohlcv(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame: ...

    def fetch_mark(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame: ...

    def fetch_index(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame: ...

    def fetch_funding(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame: ...

    def fetch_open_interest(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame: ...
