from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("dema")
class DEMA(BaseIndicator):
    def __init__(self, period: int) -> None:
        super().__init__(name=f"dema_{period}", warmup_bars=period * 2)
        self._ema1 = StreamingEMA(period)
        self._ema2 = StreamingEMA(period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        e1 = self._ema1.update(bar.close)
        if e1 is not None:
            self._ema2.update(e1)

    def reset(self) -> None:
        self._bars_seen = 0
        self._ema1.reset()
        self._ema2.reset()

    @property
    def value(self) -> float | None:
        if self._ema1.value is None or self._ema2.value is None:
            return None
        return 2 * self._ema1.value - self._ema2.value
