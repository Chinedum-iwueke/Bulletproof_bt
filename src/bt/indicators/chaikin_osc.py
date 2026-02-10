from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.adl import ADL
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("chaikin_osc")
class ChaikinOscillator(BaseIndicator):
    def __init__(self, fast: int = 3, slow: int = 10) -> None:
        super().__init__(name="chaikin_osc", warmup_bars=slow)
        self._adl = ADL()
        self._fast = StreamingEMA(fast)
        self._slow = StreamingEMA(slow)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._adl.update(bar)
        if self._adl.value is None:
            return
        self._fast.update(self._adl.value)
        self._slow.update(self._adl.value)

    def reset(self) -> None:
        self._bars_seen = 0
        self._adl.reset(); self._fast.reset(); self._slow.reset()

    @property
    def value(self) -> float | None:
        if self._fast.value is None or self._slow.value is None:
            return None
        return self._fast.value - self._slow.value
