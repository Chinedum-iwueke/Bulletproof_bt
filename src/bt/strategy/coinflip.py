"""Coinflip strategy placeholder."""
from __future__ import annotations

from typing import Iterable

from bt.core.types import Bar, Signal
from bt.strategy.base import Strategy


class CoinflipStrategy(Strategy):
    def on_bar(self, bar: Bar) -> Iterable[Signal]:
        raise NotImplementedError("Coinflip strategy not implemented yet.")
