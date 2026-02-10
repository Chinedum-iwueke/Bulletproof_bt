from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("ultimate_oscillator")
class UltimateOscillator(BaseIndicator):
    def __init__(self, p1: int = 7, p2: int = 14, p3: int = 28) -> None:
        super().__init__(name="ultimate_oscillator", warmup_bars=p3 + 1)
        self._p = (p1, p2, p3)
        self._bp: deque[float] = deque(maxlen=p3)
        self._tr: deque[float] = deque(maxlen=p3)
        self._prev_close: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev_close is None:
            self._prev_close = bar.close
            return
        low = min(bar.low, self._prev_close)
        high = max(bar.high, self._prev_close)
        self._bp.append(bar.close - low)
        self._tr.append(high - low)
        self._prev_close = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._bp.clear(); self._tr.clear(); self._prev_close = None

    @property
    def value(self) -> float | None:
        p1, p2, p3 = self._p
        if len(self._bp) < p3:
            return None
        avgs = []
        for p in (p1, p2, p3):
            bp = sum(list(self._bp)[-p:])
            tr = sum(list(self._tr)[-p:])
            avgs.append(safe_div(bp, tr))
        return 100 * safe_div((4 * avgs[0]) + (2 * avgs[1]) + avgs[2], 7)
