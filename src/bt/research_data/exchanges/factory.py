"""Exchange adapter factory."""
from __future__ import annotations

from bt.research_data.exchanges.binance import BinanceUSDMPerpAdapter
from bt.research_data.exchanges.bybit import BybitUSDTPerpAdapter
from bt.research_data.exchanges.okx import OKXUSDTPerpAdapter


def get_adapter(exchange: str):
    name = exchange.lower()
    if name == "binance":
        return BinanceUSDMPerpAdapter()
    if name == "bybit":
        return BybitUSDTPerpAdapter()
    if name == "okx":
        return OKXUSDTPerpAdapter()
    raise ValueError(f"unsupported exchange: {exchange}")
