"""Fetch scheduler for backfills and incremental updates."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator

import pandas as pd

from bt.research_data.config import DEFAULT_START_TS, RAW_DATASETS, STABLE_USDT_PERP_SYMBOLS
from bt.research_data.fetching.chunking import FetchChunk, iter_chunks, overlap_for_dataset
from bt.research_data.fetching.fetch_jobs import FetchJob
from bt.research_data.fetching.state import FetchKey, FetchStateStore
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts


@dataclass(frozen=True)
class FetchPlan:
    jobs: tuple[FetchJob, ...]


class ChunkScheduler:
    def __init__(self, store: ResearchDataStore | None = None, state_store: FetchStateStore | None = None) -> None:
        self.store = store or ResearchDataStore()
        self.state_store = state_store or FetchStateStore(self.store)

    def plan_backfill(
        self,
        exchange: str,
        symbol: str,
        dataset: str,
        timeframe: str,
        start: object,
        end: object,
        resume: bool = True,
    ) -> FetchPlan:
        start_ts = utc_ts(start)
        end_ts = utc_ts(end)
        if resume:
            state = self.state_store.get(FetchKey(exchange, symbol, dataset, timeframe))
            if state is not None and pd.notna(state["last_successful_ts"]):
                start_ts = max(start_ts, utc_ts(state["last_successful_ts"]))
            else:
                local_last = self._local_last_ts(exchange, symbol, dataset, timeframe)
                if local_last is not None:
                    start_ts = max(start_ts, local_last)
        jobs = tuple(
            FetchJob(exchange, symbol, dataset, timeframe, chunk)
            for chunk in iter_chunks(start_ts, end_ts, dataset, timeframe)
        )
        return FetchPlan(jobs)

    def _local_last_ts(self, exchange: str, symbol: str, dataset: str, timeframe: str) -> pd.Timestamp | None:
        raw_timeframe = "5m" if dataset == "oi" else timeframe
        path = self.store.raw_path(exchange, symbol, dataset, raw_timeframe)
        df = self.store.read(path)
        if df.empty or "ts" not in df.columns:
            return None
        ts = pd.to_datetime(df["ts"], utc=True, errors="coerce").dropna()
        if ts.empty:
            return None
        return ts.max()

    def plan_update(
        self,
        exchange: str,
        symbol: str,
        dataset: str,
        timeframe: str,
        end: object,
        default_start: object = DEFAULT_START_TS,
    ) -> FetchPlan:
        state = self.state_store.get(FetchKey(exchange, symbol, dataset, timeframe))
        if state is not None and pd.notna(state["last_successful_ts"]):
            start_ts = utc_ts(state["last_successful_ts"]) - overlap_for_dataset(dataset)
        else:
            start_ts = utc_ts(default_start)
        start_ts = max(start_ts, utc_ts(default_start))
        return self.plan_backfill(exchange, symbol, dataset, timeframe, start_ts, end, resume=False)

    def symbols_from_local_state(self, exchange: str) -> list[str]:
        symbols: set[str] = set()
        state = self.state_store.read()
        if not state.empty:
            symbols.update(state.loc[state["exchange"].eq(exchange), "symbol"].astype(str))
        raw_root = self.store.root / "raw" / exchange
        if raw_root.exists():
            symbols.update(path.name for path in raw_root.iterdir() if path.is_dir())
        stable_path = self.store.manifest_path("stable_universe")
        stable = self.store.read(stable_path)
        if not stable.empty and "symbol" in stable.columns:
            symbols.update(stable.loc[stable.get("available", True).astype(bool), "symbol"].astype(str))
        if not symbols:
            symbols.update(STABLE_USDT_PERP_SYMBOLS)
        return sorted(symbols)

    def jobs_for_all(
        self,
        exchange: str,
        end: object,
        datasets: Iterable[str] = RAW_DATASETS,
        timeframe: str = "1m",
    ) -> Iterator[FetchJob]:
        for symbol in self.symbols_from_local_state(exchange):
            for dataset in datasets:
                yield from self.plan_update(exchange, symbol, dataset, timeframe, end).jobs
