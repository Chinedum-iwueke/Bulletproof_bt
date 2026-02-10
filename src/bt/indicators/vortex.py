from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator, safe_div
from bt.indicators.registry import register


@register("vortex")
class Vortex(MultiValueIndicator):
    def __init__(self, period: int = 14) -> None:
        super().__init__(name=f"vortex_{period}", warmup_bars=period + 1, primary_key="vi_plus")
        self._period = period
        self._prev_h: float | None = None
        self._prev_l: float | None = None
        self._prev_c: float | None = None
        self._vm_plus: deque[float] = deque(maxlen=period)
        self._vm_minus: deque[float] = deque(maxlen=period)
        self._tr: deque[float] = deque(maxlen=period)
        self._values = {"vi_plus": None, "vi_minus": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev_c is None:
            self._prev_h, self._prev_l, self._prev_c = bar.high, bar.low, bar.close
            return
        self._vm_plus.append(abs(bar.high - self._prev_l))
        self._vm_minus.append(abs(bar.low - self._prev_h))
        self._tr.append(max(bar.high - bar.low, abs(bar.high - self._prev_c), abs(bar.low - self._prev_c)))
        self._prev_h, self._prev_l, self._prev_c = bar.high, bar.low, bar.close
        if len(self._tr) < self._period:
            return
        tr_sum = sum(self._tr)
        self._values = {
            "vi_plus": safe_div(sum(self._vm_plus), tr_sum),
            "vi_minus": safe_div(sum(self._vm_minus), tr_sum),
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev_h = self._prev_l = self._prev_c = None
        self._vm_plus.clear(); self._vm_minus.clear(); self._tr.clear()
        self._values = {"vi_plus": None, "vi_minus": None}
