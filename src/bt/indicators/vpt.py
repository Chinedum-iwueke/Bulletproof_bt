from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("vpt")
class VPT(BaseIndicator):
    def __init__(self) -> None:
        super().__init__(name="vpt", warmup_bars=2)
        self._prev: float | None = None
        self._vpt = 0.0

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev is None:
            self._prev = bar.close
            return
        self._vpt += bar.volume * safe_div(bar.close - self._prev, self._prev)
        self._prev = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev = None
        self._vpt = 0.0

    @property
    def value(self) -> float | None:
        return self._vpt if self._bars_seen > 1 else None
