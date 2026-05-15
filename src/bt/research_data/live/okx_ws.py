"""OKX liquidation websocket stream."""
from __future__ import annotations

import json
from typing import Iterator

import pandas as pd

from bt.research_data.instruments import native_to_canonical_symbol
from bt.research_data.live.liquidation_collector import LiquidationEvent, WebSocketJSONClient

OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"


def normalize_okx_liquidation(message: dict[str, object]) -> list[LiquidationEvent]:
    data = message.get("data", [])
    rows = data if isinstance(data, list) else []
    events: list[LiquidationEvent] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        details = row.get("details")
        detail_rows = details if isinstance(details, list) else [row]
        native_symbol = str(row.get("instId") or "")
        for detail in detail_rows:
            if not isinstance(detail, dict):
                continue
            symbol = str(detail.get("instId") or native_symbol)
            if not symbol:
                continue
            ts_value = pd.to_numeric(detail.get("ts") or row.get("ts"), errors="coerce")
            price = float(detail.get("bkPx") or detail.get("price") or detail.get("px") or 0.0)
            qty = float(detail.get("sz") or detail.get("qty") or 0.0)
            side = str(detail.get("side") or row.get("side") or "").lower()
            event_id = str(detail.get("tradeId") or f"{symbol}:{ts_value}:{side}:{price}:{qty}")
            events.append(
                LiquidationEvent(
                    ts=pd.to_datetime(ts_value, unit="ms", utc=True),
                    exchange="okx",
                    native_symbol=symbol,
                    canonical_symbol=native_to_canonical_symbol(symbol),
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
    args = [{"channel": "liquidation-orders", "instType": "SWAP", "instId": symbol} for symbol in symbols]
    client = WebSocketJSONClient(OKX_PUBLIC_WS, subscribe_message={"op": "subscribe", "args": args})
    for message in client.iter_messages():
        if "event" in message:
            continue
        yield from normalize_okx_liquidation(message)
