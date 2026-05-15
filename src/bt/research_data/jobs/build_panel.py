"""Build canonical symbol panels."""
from __future__ import annotations

from bt.research_data.alignment import build_research_panel
from bt.research_data.schemas import SCHEMAS
from bt.research_data.storage import ResearchDataStore


def build_panels(exchange: str, symbols: list[str], timeframe: str = "1m", store: ResearchDataStore | None = None) -> None:
    store = store or ResearchDataStore()
    for symbol in symbols:
        ohlcv = store.read(store.raw_path(exchange, symbol, "ohlcv", timeframe))
        mark = store.read(store.raw_path(exchange, symbol, "mark", timeframe))
        index = store.read(store.raw_path(exchange, symbol, "index", timeframe))
        funding = store.read(store.raw_path(exchange, symbol, "funding", timeframe))
        oi = store.read(store.raw_path(exchange, symbol, "oi", "5m"))
        liquidations = store.read(store.canonical_path(exchange, symbol, timeframe, "liquidation_1m"))
        panel = build_research_panel(ohlcv, mark, index, funding, oi, liquidations)
        store.upsert_parquet(
            store.canonical_path(exchange, symbol, timeframe, "research_panel"),
            panel,
            key=SCHEMAS["research_panel"].key,
        )
        if not ohlcv.empty:
            store.upsert_parquet(
                store.canonical_path(exchange, symbol, timeframe, "ohlcv"),
                ohlcv,
                key=SCHEMAS["ohlcv"].key,
                columns=SCHEMAS["ohlcv"].columns,
            )
        perp_cols = [
            col
            for col in panel.columns
            if col
            in {
                "ts",
                "exchange",
                "symbol",
                "funding_rate",
                "funding_source_ts",
                "open_interest",
                "oi_source_ts",
                "oi_change_1",
                "oi_change_pct_1",
                "premium_mark_vs_index",
                "basis_close_vs_index",
            }
        ]
        if not panel.empty:
            store.upsert_parquet(
                store.canonical_path(exchange, symbol, timeframe, "perp_features"),
                panel[perp_cols],
                key=("exchange", "symbol", "ts"),
            )
