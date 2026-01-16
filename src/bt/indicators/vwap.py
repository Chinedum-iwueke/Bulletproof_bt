"""VWAP indicator placeholder."""
from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import Indicator


class VWAP(Indicator):
    def __init__(self) -> None:
        self._value = None

    def update(self, bar: Bar) -> float:
        raise NotImplementedError("VWAP calculation not implemented yet.")
