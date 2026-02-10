from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator, safe_div
from bt.indicators.registry import register


@register("stochastic")
class Stochastic(MultiValueIndicator):
    def __init__(self, period: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> None:
        super().__init__(name=f"stoch_{period}", warmup_bars=period + smooth_k + smooth_d - 2, primary_key="k")
        self._period = period
        self._highs: deque[float] = deque(maxlen=period)
        self._lows: deque[float] = deque(maxlen=period)
        self._k_smooth: deque[float] = deque(maxlen=smooth_k)
        self._d_smooth: deque[float] = deque(maxlen=smooth_d)
        self._values = {"k": None, "d": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._highs.append(bar.high)
        self._lows.append(bar.low)
        if len(self._highs) < self._period:
            return
        h, l = max(self._highs), min(self._lows)
        raw_k = 100 * safe_div(bar.close - l, h - l)
        self._k_smooth.append(raw_k)
        if len(self._k_smooth) == self._k_smooth.maxlen:
            k = sum(self._k_smooth) / len(self._k_smooth)
            self._d_smooth.append(k)
            self._values["k"] = k
            if len(self._d_smooth) == self._d_smooth.maxlen:
                self._values["d"] = sum(self._d_smooth) / len(self._d_smooth)

    def reset(self) -> None:
        self._bars_seen = 0
        self._highs.clear(); self._lows.clear(); self._k_smooth.clear(); self._d_smooth.clear()
        self._values = {"k": None, "d": None}
