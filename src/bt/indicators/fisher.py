from __future__ import annotations

from collections import deque
from math import log

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, clamp, safe_div
from bt.indicators.registry import register


@register("fisher")
class FisherTransform(BaseIndicator):
    def __init__(self, period: int = 10) -> None:
        super().__init__(name=f"fisher_{period}", warmup_bars=period)
        self._period = period
        self._h: deque[float] = deque(maxlen=period)
        self._l: deque[float] = deque(maxlen=period)
        self._x = 0.0
        self._fisher: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._h.append(bar.high); self._l.append(bar.low)
        if len(self._h) < self._period:
            return
        hh, ll = max(self._h), min(self._l)
        price = (bar.high + bar.low) / 2
        n = 2 * (safe_div(price - ll, hh - ll) - 0.5)
        self._x = clamp((0.33 * n) + (0.67 * self._x), -0.999, 0.999)
        self._fisher = 0.5 * log((1 + self._x) / (1 - self._x))

    def reset(self) -> None:
        self._bars_seen = 0
        self._h.clear(); self._l.clear(); self._x = 0.0; self._fisher = None

    @property
    def value(self) -> float | None:
        return self._fisher
