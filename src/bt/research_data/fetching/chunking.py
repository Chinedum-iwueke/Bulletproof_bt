"""Chunk planning for exchange-safe data fetches."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

import pandas as pd

from bt.research_data.time import timeframe_delta, utc_ts


@dataclass(frozen=True)
class FetchChunk:
    start: pd.Timestamp
    end: pd.Timestamp


KLINE_MAX_CANDLES = 1500
FUNDING_PAGE_SPAN = pd.Timedelta(days=180)
OI_PAGE_SPAN = pd.Timedelta(hours=40)


def dataset_chunk_size(dataset: str, timeframe: str) -> pd.Timedelta:
    if dataset in {"ohlcv", "mark", "index"}:
        return timeframe_delta(timeframe) * KLINE_MAX_CANDLES
    if dataset == "funding":
        return FUNDING_PAGE_SPAN
    if dataset == "oi":
        return OI_PAGE_SPAN
    raise ValueError(f"unsupported dataset: {dataset}")


def iter_chunks(start: object, end: object, dataset: str, timeframe: str) -> Iterator[FetchChunk]:
    cursor = utc_ts(start)
    end_ts = utc_ts(end)
    size = dataset_chunk_size(dataset, timeframe)
    while cursor < end_ts:
        chunk_end = min(cursor + size, end_ts)
        yield FetchChunk(cursor, chunk_end)
        cursor = chunk_end


def overlap_for_dataset(dataset: str) -> pd.Timedelta:
    if dataset == "funding":
        return pd.Timedelta(days=14)
    if dataset in {"ohlcv", "mark", "index", "oi"}:
        return pd.Timedelta(days=3)
    raise ValueError(f"unsupported dataset: {dataset}")
