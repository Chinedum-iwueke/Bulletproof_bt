from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import WilderRMA
from bt.indicators.base import MultiValueIndicator, safe_div
from bt.indicators.registry import register


@register("dmi_adx")
@register("adx")
class DMIADX(MultiValueIndicator):
    def __init__(self, period: int = 14) -> None:
        super().__init__(name=f"adx_{period}", warmup_bars=(period * 2), primary_key="adx")
        self._period = period
        self._prev_h: float | None = None
        self._prev_l: float | None = None
        self._prev_c: float | None = None
        self._tr = WilderRMA(period)
        self._plus = WilderRMA(period)
        self._minus = WilderRMA(period)
        self._dx = WilderRMA(period)
        self._values = {"plus_di": None, "minus_di": None, "adx": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev_c is None:
            self._prev_h, self._prev_l, self._prev_c = bar.high, bar.low, bar.close
            return
        up_move = bar.high - self._prev_h
        down_move = self._prev_l - bar.low
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0
        tr = max(bar.high - bar.low, abs(bar.high - self._prev_c), abs(bar.low - self._prev_c))
        trv = self._tr.update(tr)
        pv = self._plus.update(plus_dm)
        mv = self._minus.update(minus_dm)
        self._prev_h, self._prev_l, self._prev_c = bar.high, bar.low, bar.close
        if None in (trv, pv, mv):
            return
        plus_di = 100 * safe_div(pv, trv)
        minus_di = 100 * safe_div(mv, trv)
        dx = 100 * safe_div(abs(plus_di - minus_di), plus_di + minus_di)
        adx = self._dx.update(dx)
        self._values = {"plus_di": plus_di, "minus_di": minus_di, "adx": adx}

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev_h = self._prev_l = self._prev_c = None
        for r in (self._tr, self._plus, self._minus, self._dx):
            r.reset()
        self._values = {"plus_di": None, "minus_di": None, "adx": None}
