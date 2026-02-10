"""True Range indicator."""
from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("true_range")
class TrueRange(BaseIndicator):
    def __init__(self) -> None:
        super().__init__(name="true_range", warmup_bars=2)
        self._prev_close: float | None = None
        self._value: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev_close is None:
            self._prev_close = bar.close
            self._value = None
            return
        self._value = max(
            bar.high - bar.low,
            abs(bar.high - self._prev_close),
            abs(bar.low - self._prev_close),
        )
        self._prev_close = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev_close = None
        self._value = None

    @property
    def value(self) -> float | None:
        return self._value
