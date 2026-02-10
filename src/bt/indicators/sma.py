from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, RollingMean
from bt.indicators.registry import register


@register("sma")
class SMA(BaseIndicator):
    def __init__(self, period: int) -> None:
        super().__init__(name=f"sma_{period}", warmup_bars=period)
        self._roll = RollingMean(period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._roll.update(bar.close)

    def reset(self) -> None:
        self._bars_seen = 0
        self._roll.reset()

    @property
    def value(self) -> float | None:
        return self._roll.mean
