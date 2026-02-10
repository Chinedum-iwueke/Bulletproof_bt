from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("tsi")
class TSI(BaseIndicator):
    def __init__(self, long_period: int = 25, short_period: int = 13) -> None:
        super().__init__(name="tsi", warmup_bars=long_period + short_period + 1)
        self._prev: float | None = None
        self._pc1 = StreamingEMA(long_period)
        self._pc2 = StreamingEMA(short_period)
        self._apc1 = StreamingEMA(long_period)
        self._apc2 = StreamingEMA(short_period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev is None:
            self._prev = bar.close
            return
        pc = bar.close - self._prev
        self._prev = bar.close
        s1 = self._pc1.update(pc)
        a1 = self._apc1.update(abs(pc))
        if s1 is not None:
            self._pc2.update(s1)
        if a1 is not None:
            self._apc2.update(a1)

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev = None
        for e in (self._pc1, self._pc2, self._apc1, self._apc2):
            e.reset()

    @property
    def value(self) -> float | None:
        if self._pc2.value is None or self._apc2.value is None:
            return None
        return 100 * safe_div(self._pc2.value, self._apc2.value)
