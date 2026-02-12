"""Streaming historical feed that merges per-symbol sources in timestamp order."""
from __future__ import annotations

import heapq
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from bt.core.types import Bar
from bt.data.dataset import DatasetManifest
from bt.data.symbol_source import SymbolDataSource


@dataclass(order=True)
class _HeapItem:
    ts: datetime
    symbol_order: int
    symbol: str
    row: tuple[datetime, float, float, float, float, float]


class StreamingHistoricalDataFeed:
    """Merge per-symbol row streams into timestamp batches with bounded memory."""

    def __init__(self, dataset_dir: str, manifest: DatasetManifest, config: dict[str, Any]) -> None:
        self._dataset_dir = Path(dataset_dir)
        self._manifest = manifest
        self._config = config
        self._symbols = list(manifest.symbols)
        self.reset()

    def symbols(self) -> list[str]:
        return list(self._symbols)

    def reset(self) -> None:
        self._iterators: dict[str, Iterator[tuple[datetime, float, float, float, float, float]]] = {}
        self._heap: list[_HeapItem] = []

        date_range, row_limit, chunksize = self._streaming_options()
        for symbol_order, symbol in enumerate(self._symbols):
            rel_path = self._manifest.files_by_symbol[symbol]
            source = SymbolDataSource(
                symbol,
                str(self._dataset_dir / rel_path),
                date_range=date_range,
                row_limit=row_limit,
                chunksize=chunksize,
            )
            iterator = iter(source)
            self._iterators[symbol] = iterator
            self._push_next(symbol, symbol_order)

    def next(self) -> list[Bar] | None:
        if not self._heap:
            return None

        first = heapq.heappop(self._heap)
        current_ts = first.ts
        items = [first]

        while self._heap and self._heap[0].ts == current_ts:
            items.append(heapq.heappop(self._heap))

        bars: list[Bar] = []
        for item in items:
            ts, open_, high, low, close, volume = item.row
            bars.append(
                Bar(
                    ts=pd.Timestamp(ts),
                    symbol=item.symbol,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )
            )
            self._push_next(item.symbol, item.symbol_order)

        return bars

    def _push_next(self, symbol: str, symbol_order: int) -> None:
        iterator = self._iterators[symbol]
        try:
            row = next(iterator)
        except StopIteration:
            return

        heapq.heappush(
            self._heap,
            _HeapItem(ts=row[0], symbol_order=symbol_order, symbol=symbol, row=row),
        )

    def _streaming_options(self) -> tuple[tuple[datetime, datetime] | None, int | None, int]:
        data_cfg: dict[str, Any] = {}
        if isinstance(self._config, dict) and isinstance(self._config.get("data"), dict):
            data_cfg = self._config["data"]

        date_range = data_cfg.get("date_range")
        normalized_range: tuple[datetime, datetime] | None = None
        if date_range is not None:
            if not isinstance(date_range, (tuple, list)) or len(date_range) != 2:
                raise ValueError("data.date_range must be a [start, end] pair")
            start = pd.Timestamp(date_range[0]).tz_convert("UTC") if pd.Timestamp(date_range[0]).tzinfo else pd.Timestamp(date_range[0], tz="UTC")
            end = pd.Timestamp(date_range[1]).tz_convert("UTC") if pd.Timestamp(date_range[1]).tzinfo else pd.Timestamp(date_range[1], tz="UTC")
            normalized_range = (start.to_pydatetime(), end.to_pydatetime())

        row_limit = data_cfg.get("row_limit_per_symbol")
        if row_limit is not None:
            if not isinstance(row_limit, int) or row_limit < 0:
                raise ValueError("data.row_limit_per_symbol must be an integer >= 0")

        chunksize = data_cfg.get("chunksize", 200_000)
        if not isinstance(chunksize, int) or chunksize <= 0:
            raise ValueError("data.chunksize must be a positive integer")

        return normalized_range, row_limit, chunksize
