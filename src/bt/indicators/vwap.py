"""Volume weighted average price indicator."""
from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("vwap")
class VWAP(BaseIndicator):
    """Streaming VWAP indicator."""

    def __init__(self) -> None:
        super().__init__(name="vwap", warmup_bars=1)
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        typical_price = (bar.high + bar.low + bar.close) / 3
        self._cum_pv += typical_price * bar.volume
        self._cum_vol += bar.volume

    def reset(self) -> None:
        self._bars_seen = 0
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    @property
    def is_ready(self) -> bool:
        return self._cum_vol > 0

    @property
    def value(self) -> float | None:
        if self._cum_vol <= 0:
            return None
        return safe_div(self._cum_pv, self._cum_vol, default=0.0)
