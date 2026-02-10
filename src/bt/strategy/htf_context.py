"""Strategy adapter to enrich engine context with strict HTF bars."""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from bt.core.types import Bar, Signal
from bt.data.resample import HTFBar, TimeframeResampler
from bt.strategy.base import Strategy


class HTFContextStrategyAdapter(Strategy):
    """Wrap a strategy and inject closed HTF bars into ctx['htf'][timeframe][symbol]."""

    def __init__(self, *, inner: Strategy, resampler: TimeframeResampler) -> None:
        self._inner = inner
        self._resampler = resampler
        self._latest_closed: dict[str, dict[str, HTFBar]] = {}

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        emitted_index: dict[str, dict[str, HTFBar]] = {}
        for bar in bars_by_symbol.values():
            emitted = self._resampler.update(bar)
            for htf_bar in emitted:
                by_tf = self._latest_closed.setdefault(htf_bar.timeframe, {})
                by_tf[htf_bar.symbol] = htf_bar
                emitted_index.setdefault(htf_bar.timeframe, {})[htf_bar.symbol] = htf_bar

        new_ctx = dict(ctx)
        new_ctx["htf"] = emitted_index
        return self._inner.on_bars(ts, bars_by_symbol, tradeable, new_ctx)
