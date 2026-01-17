"""Volume weighted average price indicator."""
from __future__ import annotations

from bt.core.types import Bar


class VWAP:
    """Streaming VWAP indicator.

    TODO: add session reset support for daily/regular trading hours.
    """

    def __init__(self) -> None:
        self.name = "vwap"
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    def update(self, bar: Bar) -> None:
        typical_price = (bar.high + bar.low + bar.close) / 3
        self._cum_pv += typical_price * bar.volume
        self._cum_vol += bar.volume

    @property
    def is_ready(self) -> bool:
        return self._cum_vol > 0

    @property
    def value(self) -> float | None:
        if self._cum_vol <= 0:
            return None
        return self._cum_pv / self._cum_vol
