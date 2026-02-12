"""Streaming, bounded-memory multi-symbol historical feed."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

from bt.core.types import Bar
from bt.data.dataset import DatasetManifest
from bt.data.symbol_source import RowTuple, SymbolDataSource


class StreamingHistoricalDataFeed:
    """Memory-safe multi-symbol historical feed."""

    def __init__(
        self,
        dataset_dir: str,
        manifest: DatasetManifest,
        *,
        config: dict[str, Any] | None = None,
    ) -> None:
        self._dataset_dir = Path(dataset_dir)
        self._manifest = manifest
        self._config = config or {}
        self._symbols = list(manifest.symbols)

        self._iter_by_symbol: dict[str, Iterator[RowTuple]] = {}
        self._buf_by_symbol: dict[str, RowTuple] = {}
        self._next_ts = None
        self.reset()

    def symbols(self) -> list[str]:
        return list(self._symbols)

    def reset(self) -> None:
        self._iter_by_symbol = {}
        self._buf_by_symbol = {}
        data_cfg = self._config.get("data", {}) if isinstance(self._config, dict) else {}
        if not isinstance(data_cfg, dict):
            raise ValueError("config.data must be a mapping when provided")

        date_range = data_cfg.get("date_range")
        row_limit = data_cfg.get("row_limit_per_symbol")
        chunksize = int(data_cfg.get("chunksize", 50_000))

        for symbol in self._symbols:
            rel_path = self._manifest.files_by_symbol.get(symbol)
            if rel_path is None:
                raise ValueError(f"manifest is missing file path for symbol '{symbol}'")
            abs_path = self._dataset_dir / rel_path
            if not abs_path.is_file():
                raise ValueError(f"Referenced data file for symbol '{symbol}' does not exist: {abs_path}")

            source = SymbolDataSource(
                symbol=symbol,
                path=str(abs_path),
                date_range=date_range,
                row_limit=row_limit,
                chunksize=chunksize,
            )
            iterator = iter(source)
            self._iter_by_symbol[symbol] = iterator
            try:
                self._buf_by_symbol[symbol] = next(iterator)
            except StopIteration:
                self._iter_by_symbol.pop(symbol, None)

        self._next_ts = self._compute_next_ts()

    def _compute_next_ts(self):
        if not self._buf_by_symbol:
            return None
        return min(row[0] for row in self._buf_by_symbol.values())

    def peek_time(self):
        return self._next_ts

    def next(self) -> dict[str, Bar] | None:
        if self._next_ts is None:
            return None

        current_ts = self._next_ts
        bars_by_symbol: dict[str, Bar] = {}
        symbols_to_advance: list[str] = []

        for symbol in self._symbols:
            buffered = self._buf_by_symbol.get(symbol)
            if buffered is None or buffered[0] != current_ts:
                continue

            ts, o, h, l, c, v = buffered
            bars_by_symbol[symbol] = Bar(
                ts=ts,
                symbol=symbol,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=v,
            )
            symbols_to_advance.append(symbol)

        for symbol in symbols_to_advance:
            iterator = self._iter_by_symbol[symbol]
            try:
                self._buf_by_symbol[symbol] = next(iterator)
            except StopIteration:
                self._buf_by_symbol.pop(symbol, None)
                self._iter_by_symbol.pop(symbol, None)

        self._next_ts = self._compute_next_ts()
        return bars_by_symbol

    def __iter__(self) -> StreamingHistoricalDataFeed:
        return self

    def __next__(self) -> dict[str, Bar]:
        bars = self.next()
        if bars is None:
            raise StopIteration
        return bars
