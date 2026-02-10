"""Coinflip strategy implementation."""
from __future__ import annotations

import random
from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.strategy.base import Strategy
from bt.strategy import register_strategy


@register_strategy("coinflip")
class CoinFlipStrategy(Strategy):
    def __init__(self, *, seed: int = 42, p_trade: float = 0.2, cooldown_bars: int = 0):
        self._seed = seed
        self._p_trade = p_trade
        self._cooldown_bars = cooldown_bars
        self._rng = random.Random(seed)
        self._bars_since_signal: dict[str, int] = {}

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        signals: list[Signal] = []
        for symbol in tradeable:
            bar = bars_by_symbol.get(symbol)
            if bar is None:
                continue

            if symbol in self._bars_since_signal:
                self._bars_since_signal[symbol] += 1
            else:
                self._bars_since_signal[symbol] = self._cooldown_bars

            if self._cooldown_bars > 0 and self._bars_since_signal[symbol] < self._cooldown_bars:
                continue

            if self._rng.random() >= self._p_trade:
                continue

            side = Side.BUY if self._rng.random() < 0.5 else Side.SELL
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    signal_type="coinflip",
                    confidence=0.5,
                    metadata={
                        "strategy": "coinflip",
                        "seed": self._seed,
                        "p_trade": self._p_trade,
                        "cooldown_bars": self._cooldown_bars,
                    },
                )
            )
            self._bars_since_signal[symbol] = 0

        return signals
