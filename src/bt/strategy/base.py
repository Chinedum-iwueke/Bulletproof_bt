"""Strategy interface."""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from bt.core.types import Bar, Signal


class Strategy(ABC):
    @abstractmethod
    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
    ) -> list[Signal]:
        """
        Called once per timestamp.
        bars_by_symbol includes only symbols that have a bar at ts (gaps preserved).
        tradeable is the universe membership at ts.
        Return a list of Signals (intent only).
        """
