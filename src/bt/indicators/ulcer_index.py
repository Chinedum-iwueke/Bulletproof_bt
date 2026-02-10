from __future__ import annotations

from collections import deque
from math import sqrt

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("ulcer_index")
class UlcerIndex(BaseIndicator):
    def __init__(self, period: int = 14) -> None:
        super().__init__(name=f"ulcer_{period}", warmup_bars=period)
        self._period = period
        self._close: deque[float] = deque(maxlen=period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._close.append(bar.close)

    def reset(self) -> None:
        self._bars_seen = 0
        self._close.clear()

    @property
    def value(self) -> float | None:
        if len(self._close) < self._period:
            return None
        peak = max(self._close)
        sq = [((100 * ((c / peak) - 1)) ** 2) for c in self._close]
        return sqrt(sum(sq) / self._period)
