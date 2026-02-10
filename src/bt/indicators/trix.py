from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("trix")
class TRIX(BaseIndicator):
    def __init__(self, period: int = 15) -> None:
        super().__init__(name=f"trix_{period}", warmup_bars=period * 3 + 1)
        self._e1 = StreamingEMA(period)
        self._e2 = StreamingEMA(period)
        self._e3 = StreamingEMA(period)
        self._prev: float | None = None
        self._value: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        e1 = self._e1.update(bar.close)
        if e1 is None:
            return
        e2 = self._e2.update(e1)
        if e2 is None:
            return
        e3 = self._e3.update(e2)
        if e3 is None:
            return
        if self._prev is not None:
            self._value = 100 * safe_div(e3 - self._prev, self._prev)
        self._prev = e3

    def reset(self) -> None:
        self._bars_seen = 0
        self._e1.reset(); self._e2.reset(); self._e3.reset()
        self._prev = None; self._value = None

    @property
    def value(self) -> float | None:
        return self._value
