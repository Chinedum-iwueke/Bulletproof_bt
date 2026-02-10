from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators._helpers import typical_price
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("mfi")
class MFI(BaseIndicator):
    def __init__(self, period: int = 14) -> None:
        super().__init__(name=f"mfi_{period}", warmup_bars=period + 1)
        self._period = period
        self._prev_tp: float | None = None
        self._pos: deque[float] = deque(maxlen=period)
        self._neg: deque[float] = deque(maxlen=period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        tp = typical_price(bar.high, bar.low, bar.close)
        raw = tp * bar.volume
        if self._prev_tp is not None:
            self._pos.append(raw if tp > self._prev_tp else 0.0)
            self._neg.append(raw if tp < self._prev_tp else 0.0)
        self._prev_tp = tp

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev_tp = None
        self._pos.clear(); self._neg.clear()

    @property
    def value(self) -> float | None:
        if len(self._pos) < self._period:
            return None
        ratio = safe_div(sum(self._pos), sum(self._neg), default=float("inf"))
        return 100 - (100 / (1 + ratio))
