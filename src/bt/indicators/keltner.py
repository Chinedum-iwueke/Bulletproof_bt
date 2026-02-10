from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.atr import ATR
from bt.indicators.base import MultiValueIndicator
from bt.indicators.ema import EMA
from bt.indicators.registry import register


@register("keltner")
class KeltnerChannel(MultiValueIndicator):
    def __init__(self, period: int = 20, atr_mult: float = 2.0) -> None:
        super().__init__(name=f"keltner_{period}", warmup_bars=period + 1, primary_key="mid")
        self._mult = atr_mult
        self._ema = EMA(period)
        self._atr = ATR(period)
        self._values = {"mid": None, "upper": None, "lower": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._ema.update(bar)
        self._atr.update(bar)
        if self._ema.value is None or self._atr.value is None:
            return
        self._values = {
            "mid": self._ema.value,
            "upper": self._ema.value + (self._mult * self._atr.value),
            "lower": self._ema.value - (self._mult * self._atr.value),
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._ema.reset(); self._atr.reset()
        self._values = {"mid": None, "upper": None, "lower": None}
