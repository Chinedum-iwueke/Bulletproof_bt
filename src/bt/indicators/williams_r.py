from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("williams_r")
class WilliamsR(BaseIndicator):
    def __init__(self, period: int = 14) -> None:
        super().__init__(name=f"williams_r_{period}", warmup_bars=period)
        self._h: deque[float] = deque(maxlen=period)
        self._l: deque[float] = deque(maxlen=period)
        self._c: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._h.append(bar.high); self._l.append(bar.low); self._c = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._h.clear(); self._l.clear(); self._c = None

    @property
    def value(self) -> float | None:
        if len(self._h) < self._h.maxlen or self._c is None:
            return None
        hh, ll = max(self._h), min(self._l)
        return -100 * safe_div(hh - self._c, hh - ll)
