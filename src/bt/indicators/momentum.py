from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("momentum")
class Momentum(BaseIndicator):
    def __init__(self, period: int = 10) -> None:
        super().__init__(name=f"mom_{period}", warmup_bars=period + 1)
        self._vals: deque[float] = deque(maxlen=period + 1)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._vals.append(bar.close)

    def reset(self) -> None:
        self._bars_seen = 0
        self._vals.clear()

    @property
    def value(self) -> float | None:
        if len(self._vals) < self._vals.maxlen:
            return None
        return self._vals[-1] - self._vals[0]
