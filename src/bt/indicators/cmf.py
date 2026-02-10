from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators._helpers import money_flow_multiplier
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("cmf")
class CMF(BaseIndicator):
    def __init__(self, period: int = 20) -> None:
        super().__init__(name=f"cmf_{period}", warmup_bars=period)
        self._mfv: deque[float] = deque(maxlen=period)
        self._vol: deque[float] = deque(maxlen=period)

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        mfm = money_flow_multiplier(bar.high, bar.low, bar.close)
        self._mfv.append(mfm * bar.volume)
        self._vol.append(bar.volume)

    def reset(self) -> None:
        self._bars_seen = 0
        self._mfv.clear(); self._vol.clear()

    @property
    def value(self) -> float | None:
        if len(self._mfv) < self._mfv.maxlen:
            return None
        return safe_div(sum(self._mfv), sum(self._vol), default=0.0)
