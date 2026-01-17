"""Average true range indicator."""
from __future__ import annotations

from collections import deque

from bt.core.types import Bar


class ATR:
    """Streaming ATR indicator using Wilder's smoothing."""

    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.name = f"atr_{period}"
        self._period = period
        self._prev_close: float | None = None
        self._trs: deque[float] = deque(maxlen=period)
        self._atr: float | None = None
        self._bars_seen = 0

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev_close is None:
            self._prev_close = bar.close
            return

        tr = max(
            bar.high - bar.low,
            abs(bar.high - self._prev_close),
            abs(bar.low - self._prev_close),
        )
        self._prev_close = bar.close

        if self._atr is None:
            self._trs.append(tr)
            if len(self._trs) == self._period:
                self._atr = sum(self._trs) / self._period
            return

        self._atr = ((self._atr * (self._period - 1)) + tr) / self._period

    @property
    def is_ready(self) -> bool:
        return self._atr is not None and self._bars_seen >= self._period + 1

    @property
    def value(self) -> float | None:
        return self._atr
