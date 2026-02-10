from __future__ import annotations

from collections import deque
from math import log10

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("choppiness")
class ChoppinessIndex(BaseIndicator):
    def __init__(self, period: int = 14) -> None:
        super().__init__(name=f"choppiness_{period}", warmup_bars=period + 1)
        self._period = period
        self._trs: deque[float] = deque(maxlen=period)
        self._h: deque[float] = deque(maxlen=period)
        self._l: deque[float] = deque(maxlen=period)
        self._prev: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._h.append(bar.high); self._l.append(bar.low)
        if self._prev is not None:
            tr = max(bar.high - bar.low, abs(bar.high - self._prev), abs(bar.low - self._prev))
            self._trs.append(tr)
        self._prev = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._trs.clear(); self._h.clear(); self._l.clear(); self._prev = None

    @property
    def value(self) -> float | None:
        if len(self._trs) < self._period:
            return None
        total_tr = sum(self._trs)
        span = max(self._h) - min(self._l)
        if span <= 0:
            return 100.0
        return 100 * safe_div(log10(safe_div(total_tr, span, 1.0)), log10(self._period), default=0.0)
