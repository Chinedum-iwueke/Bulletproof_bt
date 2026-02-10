from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import WilderRMA
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("rsi")
class RSI(BaseIndicator):
    def __init__(self, period: int = 14) -> None:
        super().__init__(name=f"rsi_{period}", warmup_bars=period + 1)
        self._prev: float | None = None
        self._up = WilderRMA(period)
        self._down = WilderRMA(period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev is None:
            self._prev = bar.close
            return
        delta = bar.close - self._prev
        self._prev = bar.close
        self._up.update(max(delta, 0.0))
        self._down.update(max(-delta, 0.0))

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev = None
        self._up.reset(); self._down.reset()

    @property
    def value(self) -> float | None:
        if self._up.value is None or self._down.value is None:
            return None
        rs = safe_div(self._up.value, self._down.value, default=float("inf"))
        return 100 - (100 / (1 + rs))
