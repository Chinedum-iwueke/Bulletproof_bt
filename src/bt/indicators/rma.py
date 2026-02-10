from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import WilderRMA
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("rma")
class RMA(BaseIndicator):
    def __init__(self, period: int) -> None:
        super().__init__(name=f"rma_{period}", warmup_bars=period)
        self._rma = WilderRMA(period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._rma.update(bar.close)

    def reset(self) -> None:
        self._bars_seen = 0
        self._rma.reset()

    @property
    def value(self) -> float | None:
        return self._rma.value
