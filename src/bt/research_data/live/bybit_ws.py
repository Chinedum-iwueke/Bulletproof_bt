"""Bybit liquidation websocket stream."""
from __future__ import annotations

import json
from typing import Iterator

import pandas as pd

from bt.research_data.instruments import native_to_canonical_symbol
from bt.research_data.live.liquidation_collector import LiquidationEvent, WebSocketJSONClient

BYBIT_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"


def normalize_bybit_liquidation(message: dict[str, object]) -> list[LiquidationEvent]:
    data = message.get("data", [])
    if isinstance(data, dict):
        rows = [data]
    elif isinstance(data, list):
        rows = [row for row in data if isinstance(row, dict)]
    else:
        rows = []
    events: list[LiquidationEvent] = []
    for row in rows:
        native_symbol = str(row.get("s") or row.get("symbol") or "")
        if not native_symbol:
            continue
        ts_value = pd.to_numeric(row.get("T") or row.get("updatedTime") or message.get("ts"), errors="coerce")
        price = float(row.get("p") or row.get("price") or 0.0)
        qty = float(row.get("v") or row.get("qty") or row.get("size") or 0.0)
        side = str(row.get("S") or row.get("side") or "").lower()
        event_id = str(row.get("id") or f"{native_symbol}:{ts_value}:{side}:{price}:{qty}")
        events.append(
            LiquidationEvent(
                ts=pd.to_datetime(ts_value, unit="ms", utc=True),
                exchange="bybit",
                native_symbol=native_symbol,
                canonical_symbol=native_to_canonical_symbol(native_symbol),
                side=side,
                price=price,
                qty=qty,
                notional=price * qty,
                event_id=event_id,
                raw=json.dumps(message, separators=(",", ":"), sort_keys=True),
            )
        )
    return events


def iter_liquidations(symbols: list[str]) -> Iterator[LiquidationEvent]:
    args = [f"allLiquidation.{symbol}" for symbol in symbols]
    client = WebSocketJSONClient(BYBIT_LINEAR_WS, subscribe_message={"op": "subscribe", "args": args})
    for message in client.iter_messages():
        if message.get("op") in {"subscribe", "pong"} or message.get("success") is True:
            continue
        yield from normalize_bybit_liquidation(message)
