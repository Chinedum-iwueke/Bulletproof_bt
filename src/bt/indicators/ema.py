"""EMA indicator placeholder."""
from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import Indicator


class EMA(Indicator):
    def __init__(self, period: int) -> None:
        self.period = period
        self._value = None

    def update(self, bar: Bar) -> float:
        raise NotImplementedError("EMA calculation not implemented yet.")
