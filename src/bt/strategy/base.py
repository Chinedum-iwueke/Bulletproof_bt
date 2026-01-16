"""Strategy interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from bt.core.types import Bar, Signal


class Strategy(ABC):
    @abstractmethod
    def on_bar(self, bar: Bar) -> Iterable[Signal]:
        """Generate signals for the incoming bar."""
