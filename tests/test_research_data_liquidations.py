from __future__ import annotations

import json

import pandas as pd

from bt.research_data.alignment import build_research_panel
from bt.research_data.live.binance_ws import normalize_binance_liquidation
from bt.research_data.live.bybit_ws import normalize_bybit_liquidation
from bt.research_data.live.liquidation_collector import (
    LiquidationEvent,
    aggregate_liquidation_frame,
    aggregate_liquidations,
    append_liquidation_event,
    read_liquidation_events,
)
from bt.research_data.live.okx_ws import normalize_okx_liquidation
from bt.research_data.storage import ResearchDataStore


def _event(event_id: str, side: str, ts: str = "2021-01-01 00:00:10") -> LiquidationEvent:
    return LiquidationEvent(
        ts=pd.Timestamp(ts, tz="UTC"),
        exchange="binance",
        native_symbol="BTCUSDT",
        canonical_symbol="BTC-USDT-PERP",
        side=side,
        price=100.0,
        qty=2.0,
        notional=200.0,
        event_id=event_id,
        raw="{}",
    )


def test_binance_liquidation_normalization() -> None:
    event = normalize_binance_liquidation(
        {
            "data": {
                "E": 1609459200500,
                "o": {"s": "BTCUSDT", "S": "SELL", "p": "100", "q": "2", "T": 1609459200000, "i": "abc"}
            }
        }
    )

    assert event is not None
    assert event.native_symbol == "BTCUSDT"
    assert event.canonical_symbol == "BTC-USDT-PERP"
    assert event.side == "sell"
    assert event.notional == 200.0


def test_bybit_liquidation_normalization() -> None:
    events = normalize_bybit_liquidation(
        {"topic": "allLiquidation.BTCUSDT", "data": [{"s": "BTCUSDT", "S": "Buy", "p": "100", "v": "3", "T": 1609459200000}]}
    )

    assert len(events) == 1
    assert events[0].canonical_symbol == "BTC-USDT-PERP"
    assert events[0].side == "buy"
    assert events[0].notional == 300.0


def test_okx_liquidation_normalization() -> None:
    events = normalize_okx_liquidation(
        {
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "details": [{"side": "sell", "bkPx": "100", "sz": "4", "ts": "1609459200000", "tradeId": "t1"}],
                }
            ]
        }
    )

    assert len(events) == 1
    assert events[0].native_symbol == "BTC-USDT-SWAP"
    assert events[0].canonical_symbol == "BTC-USDT-PERP"
    assert events[0].notional == 400.0


def test_append_only_jsonl_deduplicates_when_event_id_exists(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    first = append_liquidation_event(store, _event("e1", "buy"))
    second = append_liquidation_event(store, _event("e1", "buy"))
    path = store.raw_jsonl_path("binance", "BTCUSDT", "liquidations", "event")

    assert first is True
    assert second is False
    assert len(path.read_text().strip().splitlines()) == 1
    df = read_liquidation_events(path)
    assert len(df) == 1


def test_aggregate_liquidations_to_1m_buckets() -> None:
    events = pd.DataFrame(
        [
            json.loads(_event("b1", "buy", "2021-01-01 00:00:10").to_json()),
            json.loads(_event("s1", "sell", "2021-01-01 00:00:30").to_json()),
            json.loads(_event("b2", "buy", "2021-01-01 00:01:01").to_json()),
        ]
    )

    aggregated = aggregate_liquidation_frame(events, "1m")

    assert len(aggregated) == 2
    first = aggregated.iloc[0]
    assert first["liq_buy_qty"] == 2.0
    assert first["liq_sell_qty"] == 2.0
    assert first["liq_event_count"] == 2


def test_aggregate_liquidations_writes_canonical_parquet(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    append_liquidation_event(store, _event("e1", "buy"))

    aggregated = aggregate_liquidations("binance", "1m", store)
    saved = store.read(store.canonical_path("binance", "BTCUSDT", "1m", "liquidation_1m"))

    assert len(aggregated) == 1
    assert len(saved) == 1
    assert saved.loc[0, "liq_buy_notional"] == 200.0


def test_panel_includes_liquidations_only_when_available() -> None:
    ohlcv = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "canonical_symbol": "BTC-USDT-PERP",
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1.0,
                "quote_volume": 1.0,
                "trade_count": 1,
            }
        ]
    )
    no_liq = build_research_panel(ohlcv, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    liq = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                "exchange": "binance",
                "native_symbol": "BTCUSDT",
                "canonical_symbol": "BTC-USDT-PERP",
                "liq_buy_qty": 2.0,
                "liq_sell_qty": 0.0,
                "liq_buy_notional": 200.0,
                "liq_sell_notional": 0.0,
                "liq_event_count": 1,
            }
        ]
    )
    with_liq = build_research_panel(ohlcv, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), liq)

    assert "liq_buy_qty" not in no_liq.columns
    assert with_liq.loc[0, "liq_buy_qty"] == 2.0
