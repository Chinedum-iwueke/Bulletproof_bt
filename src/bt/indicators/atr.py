"""ATR indicator placeholder."""
from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import Indicator


class ATR(Indicator):
    def __init__(self, period: int) -> None:
        self.period = period
        self._value = None

    def update(self, bar: Bar) -> float:
        raise NotImplementedError("ATR calculation not implemented yet.")
