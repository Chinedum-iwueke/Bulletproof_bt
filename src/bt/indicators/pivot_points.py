from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator
from bt.indicators.registry import register


@register("pivot_points")
class PivotPoints(MultiValueIndicator):
    """Classic daily pivots using previous UTC day's OHLC."""

    def __init__(self) -> None:
        super().__init__(name="pivot_points", warmup_bars=2, primary_key="pivot")
        self._session_day: pd.Timestamp | None = None
        self._curr_h: float | None = None
        self._curr_l: float | None = None
        self._curr_c: float | None = None
        self._values = {"pivot": None, "r1": None, "s1": None, "r2": None, "s2": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        day = bar.ts.normalize()
        if self._session_day is None:
            self._session_day = day
            self._curr_h, self._curr_l, self._curr_c = bar.high, bar.low, bar.close
            return
        if day != self._session_day:
            assert self._curr_h is not None and self._curr_l is not None and self._curr_c is not None
            p = (self._curr_h + self._curr_l + self._curr_c) / 3
            self._values = {
                "pivot": p,
                "r1": (2 * p) - self._curr_l,
                "s1": (2 * p) - self._curr_h,
                "r2": p + (self._curr_h - self._curr_l),
                "s2": p - (self._curr_h - self._curr_l),
            }
            self._session_day = day
            self._curr_h, self._curr_l, self._curr_c = bar.high, bar.low, bar.close
        else:
            self._curr_h = max(self._curr_h or bar.high, bar.high)
            self._curr_l = min(self._curr_l or bar.low, bar.low)
            self._curr_c = bar.close

    def reset(self) -> None:
        self._bars_seen = 0
        self._session_day = None
        self._curr_h = self._curr_l = self._curr_c = None
        self._values = {"pivot": None, "r1": None, "s1": None, "r2": None, "s2": None}
