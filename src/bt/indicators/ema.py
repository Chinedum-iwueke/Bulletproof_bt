"""Exponential moving average indicator."""
from __future__ import annotations

from bt.core.types import Bar


class EMA:
    """Streaming EMA indicator."""

    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.name = f"ema_{period}"
        self._period = period
        self._alpha = 2 / (period + 1)
        self._count = 0
        self._sum = 0.0
        self._ema: float | None = None

    def update(self, bar: Bar) -> None:
        price = bar.close
        self._count += 1
        if self._count <= self._period:
            self._sum += price
            if self._count == self._period:
                self._ema = self._sum / self._period
            return

        if self._ema is None:
            self._ema = price
        else:
            self._ema = (self._alpha * price) + ((1 - self._alpha) * self._ema)

    @property
    def is_ready(self) -> bool:
        return self._ema is not None and self._count >= self._period

    @property
    def value(self) -> float | None:
        return self._ema
