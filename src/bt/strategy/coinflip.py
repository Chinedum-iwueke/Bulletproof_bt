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
    def __init__(
        self,
        *,
        seed: int = 42,
        p_trade: float = 0.2,
        cooldown_bars: int = 0,
        stop_atr_multiple: float = 2.0,
        max_hold_bars: int = 60,
    ):
        self._seed = seed
        self._p_trade = p_trade
        self._cooldown_bars = cooldown_bars
        self._stop_atr_multiple = stop_atr_multiple
        self._max_hold_bars = max_hold_bars
        self._rng = random.Random(seed)
        self._bars_since_signal: dict[str, int] = {}
        self._bars_in_position: dict[str, int] = {}

    @staticmethod
    def _ctx_position_side(ctx: Mapping[str, Any], symbol: str) -> Side | None:
        positions = ctx.get("positions")
        if not isinstance(positions, Mapping):
            return None
        position = positions.get(symbol)
        if not isinstance(position, Mapping):
            return None
        side = position.get("side")
        if isinstance(side, Side):
            return side
        if isinstance(side, str):
            if side.lower() == "buy":
                return Side.BUY
            if side.lower() == "sell":
                return Side.SELL
        return None

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

            current_side = self._ctx_position_side(ctx, symbol)
            if current_side is not None:
                bars_in_pos = self._bars_in_position.get(symbol, 0) + 1
                self._bars_in_position[symbol] = bars_in_pos
                if bars_in_pos >= self._max_hold_bars:
                    exit_side = Side.SELL if current_side == Side.BUY else Side.BUY
                    signals.append(
                        Signal(
                            ts=ts,
                            symbol=symbol,
                            side=exit_side,
                            signal_type="coinflip_exit",
                            confidence=1.0,
                            metadata={
                                "strategy": "coinflip",
                                "seed": self._seed,
                                "p_trade": self._p_trade,
                                "cooldown_bars": self._cooldown_bars,
                                "max_hold_bars": self._max_hold_bars,
                                "exit_reason": "max_hold_bars",
                                "close_only": True,
                            },
                        )
                    )
                    self._bars_in_position[symbol] = 0
                continue

            self._bars_in_position.pop(symbol, None)

            if symbol in self._bars_since_signal:
                self._bars_since_signal[symbol] += 1
            else:
                self._bars_since_signal[symbol] = self._cooldown_bars

            if self._cooldown_bars > 0 and self._bars_since_signal[symbol] < self._cooldown_bars:
                continue

            if self._rng.random() >= self._p_trade:
                continue

            side = Side.BUY if self._rng.random() < 0.5 else Side.SELL
            bar_range = bar.high - bar.low
            stop_distance = max(bar_range, 1e-8)
            stop_price = bar.close - stop_distance if side == Side.BUY else bar.close + stop_distance
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
                        "stop_atr_multiple": self._stop_atr_multiple,
                        "max_hold_bars": self._max_hold_bars,
                        "stop_price": stop_price,
                        "stop_distance": stop_distance,
                    },
                )
            )
            self._bars_since_signal[symbol] = 0

        return signals
