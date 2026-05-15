"""Binance liquidation websocket stream."""
from __future__ import annotations

import json
from typing import Iterable, Iterator

import pandas as pd

from bt.research_data.instruments import native_to_canonical_symbol
from bt.research_data.live.liquidation_collector import LiquidationEvent, WebSocketJSONClient

BINANCE_WS_BASE = "wss://fstream.binance.com/stream"


def liquidation_stream_url(symbols: Iterable[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@forceOrder" for symbol in symbols)
    return f"{BINANCE_WS_BASE}?streams={streams}"


def normalize_binance_liquidation(message: dict[str, object]) -> LiquidationEvent | None:
    data = message.get("data", message)
    if not isinstance(data, dict):
        return None
    order = data.get("o", data)
    if not isinstance(order, dict) or "s" not in order:
        return None
    native_symbol = str(order["s"])
    ts_value = pd.to_numeric(order.get("T") or data.get("E"), errors="coerce")
    price = float(order.get("p", 0.0))
    qty = float(order.get("q", 0.0))
    event_id = str(order.get("i") or f"{native_symbol}:{ts_value}:{order.get('S')}:{price}:{qty}")
    return LiquidationEvent(
        ts=pd.to_datetime(ts_value, unit="ms", utc=True),
        exchange="binance",
        native_symbol=native_symbol,
        canonical_symbol=native_to_canonical_symbol(native_symbol),
        side=str(order.get("S", "")).lower(),
        price=price,
        qty=qty,
        notional=price * qty,
        event_id=event_id,
        raw=json.dumps(message, separators=(",", ":"), sort_keys=True),
    )


def iter_liquidations(symbols: list[str]) -> Iterator[LiquidationEvent]:
    client = WebSocketJSONClient(liquidation_stream_url(symbols))
    for message in client.iter_messages():
        event = normalize_binance_liquidation(message)
        if event is not None:
            yield event
