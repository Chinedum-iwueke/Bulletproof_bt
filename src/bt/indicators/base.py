"""Indicator base class."""
from __future__ import annotations

from abc import ABC, abstractmethod

from bt.core.types import Bar


class Indicator(ABC):
    @abstractmethod
    def update(self, bar: Bar) -> float:
        """Update indicator with a new bar."""
