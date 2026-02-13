"""Client strategy template: copy this file, rename class, register strategy."""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.strategy.base import Strategy
from bt.strategy.context_view import StrategyContextView


class ClientEmaTemplateStrategy(Strategy):
    """Minimal signal-only example strategy for client customization."""

    def __init__(
        self,
        *,
        symbol: str = "BTCUSDT",
        timeframe: str = "15m",
        confidence: float = 0.6,
    ) -> None:
        # Pull configurable params from constructor kwargs (strategy config).
        self._symbol = symbol
        self._timeframe = timeframe
        self._confidence = confidence
        self._last_direction: Side | None = None

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        """Emit intent-only signals; no portfolio/execution access."""
        if self._symbol not in tradeable:
            return []

        bar = bars_by_symbol.get(self._symbol)
        if bar is None:
            return []

        # Optional HTF source: prefer configured HTF bar if adapter provides it.
        raw_ctx: Mapping[str, Any] = ctx.raw if isinstance(ctx, StrategyContextView) else ctx
        htf_ctx = raw_ctx.get("htf", {})
        if isinstance(htf_ctx, Mapping):
            tf_bars = htf_ctx.get(self._timeframe, {})
            if isinstance(tf_bars, Mapping):
                htf_bar = tf_bars.get(self._symbol)
                if isinstance(htf_bar, Bar):
                    bar = htf_bar

        direction = Side.BUY if bar.close > bar.open else Side.SELL
        if direction == self._last_direction:
            return []

        self._last_direction = direction
        return [
            Signal(
                ts=ts,
                symbol=self._symbol,
                side=direction,
                signal_type="client_template",
                confidence=self._confidence,
                metadata={"strategy": "client_template", "timeframe": self._timeframe},
            )
        ]


# Registration example (keep commented until you copy/rename this class):
# from bt.strategy import register_strategy
# register_strategy("client_ema_template")(ClientEmaTemplateStrategy)
