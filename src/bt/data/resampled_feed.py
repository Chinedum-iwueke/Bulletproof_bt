"""Feed wrappers for deterministic timeframe resampling."""
from __future__ import annotations

from typing import Any

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler, normalize_timeframe


class ResampledDataFeed:
    """Wrap a base 1m feed and emit only closed bars for a target timeframe."""

    def __init__(self, *, inner_feed: Any, timeframe: str, strict: bool = True) -> None:
        self._inner_feed = inner_feed
        self._timeframe = normalize_timeframe(timeframe, key_path="data.engine_timeframe")
        self._resampler = TimeframeResampler(timeframes=[self._timeframe], strict=bool(strict))

    def symbols(self) -> list[str]:
        if hasattr(self._inner_feed, "symbols"):
            return list(self._inner_feed.symbols())
        return []

    def reset(self) -> None:
        if hasattr(self._inner_feed, "reset"):
            self._inner_feed.reset()
        self._resampler.reset()

    def next(self) -> dict[str, Bar] | None:
        while True:
            bars = self._inner_feed.next()
            if bars is None:
                return None

            if isinstance(bars, dict):
                bars_list = list(bars.values())
            else:
                bars_list = list(bars)

            emitted_by_symbol: dict[str, Bar] = {}
            for bar in bars_list:
                emitted = self._resampler.update(bar)
                for htf_bar in emitted:
                    emitted_by_symbol[htf_bar.symbol] = Bar(
                        ts=htf_bar.ts,
                        symbol=htf_bar.symbol,
                        open=htf_bar.open,
                        high=htf_bar.high,
                        low=htf_bar.low,
                        close=htf_bar.close,
                        volume=htf_bar.volume,
                    )

            if emitted_by_symbol:
                return emitted_by_symbol


class EntryTimeframeGate:
    """Strategy adapter that filters entry signals by timestamp boundary."""

    def __init__(self, *, inner: Any, entry_timeframe: str) -> None:
        self._inner = inner
        self._entry_timeframe = normalize_timeframe(entry_timeframe, key_path="data.entry_timeframe")

    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        from bt.risk.risk_engine import RiskEngine
        from bt.data.timeframe_utils import is_timeframe_boundary

        allow_entries = is_timeframe_boundary(ts, self._entry_timeframe)
        emitted = self._inner.on_bars(ts, bars_by_symbol, tradeable, ctx)
        if allow_entries:
            return emitted

        filtered = []
        for signal in emitted:
            if RiskEngine._is_exit_signal(signal):
                filtered.append(signal)
        return filtered
