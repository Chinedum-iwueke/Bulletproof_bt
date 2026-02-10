from __future__ import annotations

from math import log

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator, RollingMean, RollingStd, safe_div
from bt.indicators.registry import register


@register("candle_features")
class CandleFeatures(MultiValueIndicator):
    def __init__(self, z_period: int = 20) -> None:
        super().__init__(name="candle_features", warmup_bars=z_period + 1, primary_key="body")
        self._ret_mean = RollingMean(z_period)
        self._ret_std = RollingStd(z_period)
        self._rng_mean = RollingMean(z_period)
        self._rng_std = RollingStd(z_period)
        self._vol_mean = RollingMean(z_period)
        self._vol_std = RollingStd(z_period)
        self._prev_close: float | None = None
        self._values = {
            "body": None, "range": None, "upper_wick": None, "lower_wick": None,
            "body_ratio": None, "gap": None, "cpr": None,
            "return_z": None, "range_z": None, "volume_z": None,
        }

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        body = abs(bar.close - bar.open)
        rng = bar.high - bar.low
        upper = bar.high - max(bar.open, bar.close)
        lower = min(bar.open, bar.close) - bar.low
        gap = None if self._prev_close is None else bar.open - self._prev_close
        ret = 0.0 if self._prev_close is None or self._prev_close <= 0 or bar.close <= 0 else log(bar.close / self._prev_close)
        self._prev_close = bar.close

        self._ret_mean.update(ret); self._ret_std.update(ret)
        self._rng_mean.update(rng); self._rng_std.update(rng)
        self._vol_mean.update(bar.volume); self._vol_std.update(bar.volume)

        ret_z = None if self._ret_mean.mean is None or self._ret_std.std is None else safe_div(ret - self._ret_mean.mean, self._ret_std.std)
        rng_z = None if self._rng_mean.mean is None or self._rng_std.std is None else safe_div(rng - self._rng_mean.mean, self._rng_std.std)
        vol_z = None if self._vol_mean.mean is None or self._vol_std.std is None else safe_div(bar.volume - self._vol_mean.mean, self._vol_std.std)

        self._values = {
            "body": body,
            "range": rng,
            "upper_wick": upper,
            "lower_wick": lower,
            "body_ratio": safe_div(body, rng),
            "gap": gap,
            "cpr": safe_div(bar.close - bar.low, rng),
            "return_z": ret_z,
            "range_z": rng_z,
            "volume_z": vol_z,
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev_close = None
        for x in (self._ret_mean, self._ret_std, self._rng_mean, self._rng_std, self._vol_mean, self._vol_std):
            x.reset()
        self._values = {
            "body": None, "range": None, "upper_wick": None, "lower_wick": None,
            "body_ratio": None, "gap": None, "cpr": None,
            "return_z": None, "range_z": None, "volume_z": None,
        }
