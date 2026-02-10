from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("force_index")
class ForceIndex(BaseIndicator):
    def __init__(self, period: int = 13) -> None:
        super().__init__(name=f"force_index_{period}", warmup_bars=period + 1)
        self._prev: float | None = None
        self._ema = StreamingEMA(period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev is None:
            self._prev = bar.close
            return
        raw = (bar.close - self._prev) * bar.volume
        self._prev = bar.close
        self._ema.update(raw)

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev = None
        self._ema.reset()

    @property
    def value(self) -> float | None:
        return self._ema.value
