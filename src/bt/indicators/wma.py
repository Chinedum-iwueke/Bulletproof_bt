from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("wma")
class WMA(BaseIndicator):
    def __init__(self, period: int) -> None:
        super().__init__(name=f"wma_{period}", warmup_bars=period)
        self._period = period
        self._weights = list(range(1, period + 1))
        self._den = sum(self._weights)
        self._vals: deque[float] = deque(maxlen=period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._vals.append(bar.close)

    def reset(self) -> None:
        self._bars_seen = 0
        self._vals.clear()

    @property
    def value(self) -> float | None:
        if len(self._vals) < self._period:
            return None
        return sum(v * w for v, w in zip(self._vals, self._weights)) / self._den
