from __future__ import annotations

from collections import deque
from math import log, sqrt

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("historical_vol")
class HistoricalVolatility(BaseIndicator):
    def __init__(self, period: int = 20, annualization: int | None = 252) -> None:
        super().__init__(name=f"historical_vol_{period}", warmup_bars=period + 1)
        self._period = period
        self._ann = annualization
        self._returns: deque[float] = deque(maxlen=period)
        self._prev: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev is not None and self._prev > 0 and bar.close > 0:
            self._returns.append(log(bar.close / self._prev))
        self._prev = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._returns.clear(); self._prev = None

    @property
    def value(self) -> float | None:
        if len(self._returns) < self._period:
            return None
        mean = sum(self._returns) / self._period
        var = sum((r - mean) ** 2 for r in self._returns) / self._period
        vol = sqrt(var)
        if self._ann is not None:
            vol *= sqrt(self._ann)
        return vol
