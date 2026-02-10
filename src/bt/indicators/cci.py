from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators._helpers import typical_price
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("cci")
class CCI(BaseIndicator):
    def __init__(self, period: int = 20) -> None:
        super().__init__(name=f"cci_{period}", warmup_bars=period)
        self._period = period
        self._tp: deque[float] = deque(maxlen=period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._tp.append(typical_price(bar.high, bar.low, bar.close))

    def reset(self) -> None:
        self._bars_seen = 0
        self._tp.clear()

    @property
    def value(self) -> float | None:
        if len(self._tp) < self._period:
            return None
        sma = sum(self._tp) / self._period
        md = sum(abs(x - sma) for x in self._tp) / self._period
        return safe_div((self._tp[-1] - sma), (0.015 * md), default=0.0)
