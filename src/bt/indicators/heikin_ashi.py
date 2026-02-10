from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator
from bt.indicators.registry import register


@register("heikin_ashi")
class HeikinAshi(MultiValueIndicator):
    def __init__(self) -> None:
        super().__init__(name="heikin_ashi", warmup_bars=1, primary_key="ha_close")
        self._ha_open: float | None = None
        self._ha_close: float | None = None
        self._values = {"ha_open": None, "ha_high": None, "ha_low": None, "ha_close": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        ha_close = (bar.open + bar.high + bar.low + bar.close) / 4
        ha_open = (bar.open + bar.close) / 2 if self._ha_open is None else (self._ha_open + (self._ha_close or ha_close)) / 2
        ha_high = max(bar.high, ha_open, ha_close)
        ha_low = min(bar.low, ha_open, ha_close)
        self._ha_open, self._ha_close = ha_open, ha_close
        self._values = {"ha_open": ha_open, "ha_high": ha_high, "ha_low": ha_low, "ha_close": ha_close}

    def reset(self) -> None:
        self._bars_seen = 0
        self._ha_open = self._ha_close = None
        self._values = {"ha_open": None, "ha_high": None, "ha_low": None, "ha_close": None}
