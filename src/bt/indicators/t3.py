from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("t3")
class T3(BaseIndicator):
    def __init__(self, period: int, vfactor: float = 0.7) -> None:
        super().__init__(name=f"t3_{period}", warmup_bars=period * 6)
        self._v = vfactor
        self._e = [StreamingEMA(period) for _ in range(6)]

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        x: float | None = bar.close
        for ema in self._e:
            if x is None:
                break
            x = ema.update(x)

    def reset(self) -> None:
        self._bars_seen = 0
        for ema in self._e:
            ema.reset()

    @property
    def value(self) -> float | None:
        vals = [ema.value for ema in self._e]
        if any(v is None for v in vals):
            return None
        e3, e4, e5, e6 = vals[2], vals[3], vals[4], vals[5]
        v = self._v
        c1 = -v**3
        c2 = 3 * v**2 + 3 * v**3
        c3 = -6 * v**2 - 3 * v - 3 * v**3
        c4 = 1 + 3 * v + v**3 + 3 * v**2
        return c1 * e6 + c2 * e5 + c3 * e4 + c4 * e3
