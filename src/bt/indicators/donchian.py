from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator
from bt.indicators.registry import register


@register("donchian")
class DonchianChannel(MultiValueIndicator):
    def __init__(self, period: int = 20) -> None:
        super().__init__(name=f"donchian_{period}", warmup_bars=period, primary_key="mid")
        self._h: deque[float] = deque(maxlen=period)
        self._l: deque[float] = deque(maxlen=period)
        self._values = {"upper": None, "lower": None, "mid": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._h.append(bar.high); self._l.append(bar.low)
        if len(self._h) < self._h.maxlen:
            return
        upper, lower = max(self._h), min(self._l)
        self._values = {"upper": upper, "lower": lower, "mid": (upper + lower) / 2}

    def reset(self) -> None:
        self._bars_seen = 0
        self._h.clear(); self._l.clear()
        self._values = {"upper": None, "lower": None, "mid": None}
