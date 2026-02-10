from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("vwma")
class VWMA(BaseIndicator):
    def __init__(self, period: int) -> None:
        super().__init__(name=f"vwma_{period}", warmup_bars=period)
        self._period = period
        self._pv: deque[float] = deque(maxlen=period)
        self._v: deque[float] = deque(maxlen=period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._pv.append(bar.close * bar.volume)
        self._v.append(bar.volume)

    def reset(self) -> None:
        self._bars_seen = 0
        self._pv.clear(); self._v.clear()

    @property
    def value(self) -> float | None:
        if len(self._v) < self._period:
            return None
        return safe_div(sum(self._pv), sum(self._v), default=0.0)
