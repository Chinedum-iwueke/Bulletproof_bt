from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator
from bt.indicators.registry import register


@register("parabolic_sar")
class ParabolicSAR(BaseIndicator):
    def __init__(self, af_step: float = 0.02, af_max: float = 0.2) -> None:
        super().__init__(name="parabolic_sar", warmup_bars=2)
        self._step = af_step
        self._max = af_max
        self._trend_up = True
        self._af = af_step
        self._ep: float | None = None
        self._sar: float | None = None
        self._prev: Bar | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev is None:
            self._prev = bar
            return
        if self._sar is None:
            self._trend_up = bar.close >= self._prev.close
            self._sar = self._prev.low if self._trend_up else self._prev.high
            self._ep = max(bar.high, self._prev.high) if self._trend_up else min(bar.low, self._prev.low)
            self._prev = bar
            return
        sar = self._sar + self._af * ((self._ep or self._sar) - self._sar)
        if self._trend_up:
            sar = min(sar, self._prev.low, bar.low)
            if bar.low < sar:
                self._trend_up = False
                sar = self._ep
                self._ep = bar.low
                self._af = self._step
            else:
                if bar.high > (self._ep or bar.high):
                    self._ep = bar.high
                    self._af = min(self._af + self._step, self._max)
        else:
            sar = max(sar, self._prev.high, bar.high)
            if bar.high > sar:
                self._trend_up = True
                sar = self._ep
                self._ep = bar.high
                self._af = self._step
            else:
                if bar.low < (self._ep or bar.low):
                    self._ep = bar.low
                    self._af = min(self._af + self._step, self._max)
        self._sar = sar
        self._prev = bar

    def reset(self) -> None:
        self._bars_seen = 0
        self._trend_up = True
        self._af = self._step
        self._ep = self._sar = None
        self._prev = None

    @property
    def value(self) -> float | None:
        return self._sar
