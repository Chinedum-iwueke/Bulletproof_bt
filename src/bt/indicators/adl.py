from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import money_flow_multiplier
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("adl")
class ADL(BaseIndicator):
    def __init__(self) -> None:
        super().__init__(name="adl", warmup_bars=1)
        self._adl = 0.0

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._adl += money_flow_multiplier(bar.high, bar.low, bar.close) * bar.volume

    def reset(self) -> None:
        self._bars_seen = 0
        self._adl = 0.0

    @property
    def value(self) -> float | None:
        return self._adl if self._bars_seen > 0 else None
