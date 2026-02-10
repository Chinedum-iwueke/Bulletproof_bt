from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("obv")
class OBV(BaseIndicator):
    def __init__(self) -> None:
        super().__init__(name="obv", warmup_bars=2)
        self._prev: float | None = None
        self._obv = 0.0

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev is None:
            self._prev = bar.close
            return
        if bar.close > self._prev:
            self._obv += bar.volume
        elif bar.close < self._prev:
            self._obv -= bar.volume
        self._prev = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev = None
        self._obv = 0.0

    @property
    def value(self) -> float | None:
        return self._obv if self._bars_seen > 1 else None
