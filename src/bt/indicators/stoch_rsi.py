from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator, safe_div
from bt.indicators.registry import register
from bt.indicators.rsi import RSI


@register("stoch_rsi")
class StochRSI(MultiValueIndicator):
    def __init__(self, period: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> None:
        super().__init__(name=f"stochrsi_{period}", warmup_bars=(period * 2) + smooth_k + smooth_d, primary_key="k")
        self._rsi = RSI(period)
        self._period = period
        self._rsi_window: deque[float] = deque(maxlen=period)
        self._k_window: deque[float] = deque(maxlen=smooth_k)
        self._d_window: deque[float] = deque(maxlen=smooth_d)
        self._values = {"k": None, "d": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._rsi.update(bar)
        if self._rsi.value is None:
            return
        self._rsi_window.append(self._rsi.value)
        if len(self._rsi_window) < self._period:
            return
        lo, hi = min(self._rsi_window), max(self._rsi_window)
        raw = 100 * safe_div(self._rsi.value - lo, hi - lo)
        self._k_window.append(raw)
        if len(self._k_window) == self._k_window.maxlen:
            k = sum(self._k_window) / len(self._k_window)
            self._values["k"] = k
            self._d_window.append(k)
            if len(self._d_window) == self._d_window.maxlen:
                self._values["d"] = sum(self._d_window) / len(self._d_window)

    def reset(self) -> None:
        self._bars_seen = 0
        self._rsi.reset(); self._rsi_window.clear(); self._k_window.clear(); self._d_window.clear()
        self._values = {"k": None, "d": None}
