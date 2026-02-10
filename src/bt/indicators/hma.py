from __future__ import annotations

from collections import deque
from math import sqrt

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


def _wma(vals: deque[float]) -> float:
    n = len(vals)
    weights = list(range(1, n + 1))
    den = sum(weights)
    return sum(v * w for v, w in zip(vals, weights)) / den


@register("hma")
class HMA(BaseIndicator):
    def __init__(self, period: int) -> None:
        half = max(1, period // 2)
        root = max(1, int(sqrt(period)))
        super().__init__(name=f"hma_{period}", warmup_bars=period + root)
        self._p = period
        self._half = half
        self._root = root
        self._vals = deque(maxlen=period)
        self._derived = deque(maxlen=root)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._vals.append(bar.close)
        if len(self._vals) >= self._half:
            w_half = _wma(deque(list(self._vals)[-self._half:]))
            w_full = _wma(self._vals)
            self._derived.append(2 * w_half - w_full)

    def reset(self) -> None:
        self._bars_seen = 0
        self._vals.clear(); self._derived.clear()

    @property
    def value(self) -> float | None:
        if len(self._derived) < self._root:
            return None
        return _wma(self._derived)
