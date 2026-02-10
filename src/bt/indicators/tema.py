from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("tema")
class TEMA(BaseIndicator):
    def __init__(self, period: int) -> None:
        super().__init__(name=f"tema_{period}", warmup_bars=period * 3)
        self._ema1 = StreamingEMA(period)
        self._ema2 = StreamingEMA(period)
        self._ema3 = StreamingEMA(period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        e1 = self._ema1.update(bar.close)
        if e1 is not None:
            e2 = self._ema2.update(e1)
            if e2 is not None:
                self._ema3.update(e2)

    def reset(self) -> None:
        self._bars_seen = 0
        self._ema1.reset(); self._ema2.reset(); self._ema3.reset()

    @property
    def value(self) -> float | None:
        if None in (self._ema1.value, self._ema2.value, self._ema3.value):
            return None
        return 3 * self._ema1.value - 3 * self._ema2.value + self._ema3.value
