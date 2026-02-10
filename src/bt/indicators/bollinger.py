from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator, RollingMean, RollingStd, safe_div
from bt.indicators.registry import register


@register("bollinger")
class BollingerBands(MultiValueIndicator):
    def __init__(self, period: int = 20, num_std: float = 2.0) -> None:
        super().__init__(name=f"bollinger_{period}", warmup_bars=period, primary_key="mid")
        self._num_std = num_std
        self._mean = RollingMean(period)
        self._std = RollingStd(period)
        self._values = {"mid": None, "upper": None, "lower": None, "bandwidth": None, "percent_b": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._mean.update(bar.close)
        std = self._std.update(bar.close)
        mid = self._mean.mean
        if mid is None or std is None:
            return
        upper = mid + self._num_std * std
        lower = mid - self._num_std * std
        self._values = {
            "mid": mid,
            "upper": upper,
            "lower": lower,
            "bandwidth": safe_div(upper - lower, mid),
            "percent_b": safe_div(bar.close - lower, upper - lower),
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._mean.reset(); self._std.reset()
        self._values = {"mid": None, "upper": None, "lower": None, "bandwidth": None, "percent_b": None}
