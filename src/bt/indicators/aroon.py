from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator
from bt.indicators.registry import register


@register("aroon")
class Aroon(MultiValueIndicator):
    def __init__(self, period: int = 25) -> None:
        super().__init__(name=f"aroon_{period}", warmup_bars=period, primary_key="up")
        self._period = period
        self._h: deque[float] = deque(maxlen=period)
        self._l: deque[float] = deque(maxlen=period)
        self._values = {"up": None, "down": None, "oscillator": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._h.append(bar.high); self._l.append(bar.low)
        if len(self._h) < self._period:
            return
        h_idx = list(self._h).index(max(self._h))
        l_idx = list(self._l).index(min(self._l))
        up = 100 * (self._period - 1 - h_idx) / (self._period - 1)
        down = 100 * (self._period - 1 - l_idx) / (self._period - 1)
        self._values = {"up": up, "down": down, "oscillator": up - down}

    def reset(self) -> None:
        self._bars_seen = 0
        self._h.clear(); self._l.clear()
        self._values = {"up": None, "down": None, "oscillator": None}
