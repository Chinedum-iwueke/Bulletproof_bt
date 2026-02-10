from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("kama")
class KAMA(BaseIndicator):
    def __init__(self, period: int = 10, fast: int = 2, slow: int = 30) -> None:
        super().__init__(name=f"kama_{period}", warmup_bars=period + 1)
        self._period = period
        self._fast_sc = 2 / (fast + 1)
        self._slow_sc = 2 / (slow + 1)
        self._vals: deque[float] = deque(maxlen=period + 1)
        self._kama: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._vals.append(bar.close)
        if len(self._vals) <= self._period:
            return
        change = abs(self._vals[-1] - self._vals[0])
        volatility = sum(abs(self._vals[i] - self._vals[i - 1]) for i in range(1, len(self._vals)))
        er = safe_div(change, volatility)
        sc = (er * (self._fast_sc - self._slow_sc) + self._slow_sc) ** 2
        if self._kama is None:
            self._kama = self._vals[-1]
        else:
            self._kama += sc * (self._vals[-1] - self._kama)

    def reset(self) -> None:
        self._bars_seen = 0
        self._vals.clear(); self._kama = None

    @property
    def value(self) -> float | None:
        return self._kama
