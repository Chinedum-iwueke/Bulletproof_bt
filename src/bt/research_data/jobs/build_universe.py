"""Build universe manifests."""
from __future__ import annotations

import pandas as pd

from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.instruments import write_instrument_manifest
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts
from bt.research_data.universe import build_volatile_universe_from_ohlcv


def build_volatile_universe(
    exchange: str,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    rebalance_freq: str,
    lookback: str,
    top_gainers: int,
    top_losers: int,
    min_age_days: int,
    min_median_dollar_volume_7d: float,
    store: ResearchDataStore | None = None,
) -> pd.DataFrame:
    store = store or ResearchDataStore()
    adapter = get_adapter(exchange)
    instruments = adapter.fetch_usdt_perp_instruments()
    write_instrument_manifest(store, instruments)
    frames = []
    native_col = "native_symbol" if "native_symbol" in instruments.columns else "symbol"
    for symbol in instruments[native_col].astype(str):
        path = store.raw_path(exchange, symbol, "ohlcv", "1m")
        if path.exists():
            frames.append(store.read(path))
    bars = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    membership = build_volatile_universe_from_ohlcv(
        bars=bars,
        exchange=exchange,
        start=utc_ts(start),
        end=utc_ts(end),
        rebalance_freq=rebalance_freq,
        lookback=lookback,
        top_gainers=top_gainers,
        top_losers=top_losers,
        min_age_days=min_age_days,
        min_median_dollar_volume_7d=min_median_dollar_volume_7d,
    )
    store.upsert_parquet(
        store.manifest_path("volatile_universe_membership"),
        membership,
        key=("exchange", "ts", "symbol", "universe"),
    )
    return membership
