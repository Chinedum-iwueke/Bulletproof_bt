from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.atr import ATR
from bt.indicators.base import MultiValueIndicator
from bt.indicators.registry import register


@register("supertrend")
class Supertrend(MultiValueIndicator):
    def __init__(self, period: int = 10, multiplier: float = 3.0) -> None:
        super().__init__(name=f"supertrend_{period}", warmup_bars=period + 1, primary_key="supertrend")
        self._mult = multiplier
        self._atr = ATR(period)
        self._final_upper: float | None = None
        self._final_lower: float | None = None
        self._values = {"supertrend": None, "direction": None, "upper": None, "lower": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._atr.update(bar)
        if self._atr.value is None:
            return
        hl2 = (bar.high + bar.low) / 2
        basic_upper = hl2 + self._mult * self._atr.value
        basic_lower = hl2 - self._mult * self._atr.value
        if self._final_upper is None:
            self._final_upper, self._final_lower = basic_upper, basic_lower
            return
        self._final_upper = basic_upper if bar.close > self._final_upper else min(basic_upper, self._final_upper)
        self._final_lower = basic_lower if bar.close < self._final_lower else max(basic_lower, self._final_lower)
        if self._values["supertrend"] is None:
            st = self._final_lower if bar.close >= self._final_lower else self._final_upper
        elif self._values["supertrend"] == self._final_upper and bar.close > self._final_upper:
            st = self._final_lower
        elif self._values["supertrend"] == self._final_lower and bar.close < self._final_lower:
            st = self._final_upper
        else:
            st = self._values["supertrend"]
        self._values = {
            "supertrend": st,
            "direction": 1.0 if st == self._final_lower else -1.0,
            "upper": self._final_upper,
            "lower": self._final_lower,
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._atr.reset(); self._final_upper = self._final_lower = None
        self._values = {"supertrend": None, "direction": None, "upper": None, "lower": None}
