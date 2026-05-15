"""OKX V5 USDT swap REST adapter."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bt.research_data.config import OKX_V5_BASE_URL
from bt.research_data.instruments import native_to_canonical_symbol, normalize_instrument_frame
from bt.research_data.schemas import FUNDING_COLUMNS, INDEX_COLUMNS, MARK_COLUMNS, OHLCV_COLUMNS, OI_COLUMNS
from bt.research_data.time import ms, utc_ts


@dataclass
class OKXV5Client:
    base_url: str = OKX_V5_BASE_URL
    timeout: float = 30.0
    retries: int = 5
    backoff_seconds: float = 0.5

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urllib.parse.urlencode(params or {})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "bulletproof-bt-research-data/1"})
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if str(payload.get("code", "0")) != "0":
                    raise RuntimeError(f"OKX error {payload.get('code')}: {payload.get('msg')}")
                return payload
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, RuntimeError) as exc:
                last_error = exc
                if isinstance(exc, urllib.error.HTTPError) and exc.code not in {429, 500, 502, 503, 504}:
                    raise
                time.sleep(self.backoff_seconds * (2**attempt))
        raise RuntimeError(f"OKX REST request failed after {self.retries} retries: {url}") from last_error


def normalize_okx_instruments(payload: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for item in payload.get("data", []):
        if item.get("instType") != "SWAP" or item.get("settleCcy") != "USDT":
            continue
        rows.append(
            {
                "exchange": "okx",
                "native_symbol": item.get("instId"),
                "canonical_symbol": native_to_canonical_symbol(str(item.get("instId", ""))),
                "base_asset": item.get("baseCcy") or str(item.get("instId", "")).split("-")[0],
                "quote_asset": item.get("quoteCcy") or "USDT",
                "settle_asset": item.get("settleCcy"),
                "contract_type": "PERPETUAL",
                "status": item.get("state"),
                "first_seen_ts": pd.NaT,
                "last_seen_ts": pd.Timestamp.now(tz="UTC"),
                "tick_size": item.get("tickSz"),
                "qty_step": item.get("lotSz"),
            }
        )
    return normalize_instrument_frame(rows)


class OKXUSDTPerpAdapter:
    exchange = "okx"

    def __init__(self, client: OKXV5Client | None = None) -> None:
        self.client = client or OKXV5Client()

    def fetch_usdt_perp_instruments(self) -> pd.DataFrame:
        payload = self.client.get("/api/v5/public/instruments", {"instType": "SWAP"})
        return normalize_okx_instruments(payload).sort_values("native_symbol").reset_index(drop=True)

    def _bar(self, timeframe: str) -> str:
        if timeframe.endswith("m"):
            return f"{int(timeframe[:-1])}m"
        if timeframe.endswith("h"):
            return f"{int(timeframe[:-1])}H"
        return timeframe

    def _fetch_candles(self, path: str, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str, inst_id: str | None = None) -> list[list[Any]]:
        rows: list[list[Any]] = []
        cursor_end = utc_ts(end)
        start_ts = utc_ts(start)
        while cursor_end > start_ts:
            payload = self.client.get(
                path,
                {
                    "instId": inst_id or symbol,
                    "bar": self._bar(timeframe),
                    "after": ms(cursor_end),
                    "limit": 100,
                },
            )
            chunk = payload.get("data", [])
            if not chunk:
                break
            rows.extend(chunk)
            oldest = min(pd.to_datetime(int(row[0]), unit="ms", utc=True) for row in chunk)
            if oldest <= start_ts or oldest >= cursor_end:
                break
            cursor_end = oldest
        return sorted([row for row in rows if start_ts <= pd.to_datetime(int(row[0]), unit="ms", utc=True) < utc_ts(end)], key=lambda row: int(row[0]))

    def fetch_ohlcv(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_candles("/api/v5/market/history-candles", symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row[0], unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "quote_volume": float(row[7]) if len(row) > 7 else pd.NA,
                    "trade_count": pd.NA,
                }
                for row in rows
            ],
            columns=OHLCV_COLUMNS,
        )
        return df.reset_index(drop=True)

    def fetch_mark(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_candles("/api/v5/market/history-mark-price-candles", symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row[0], unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "mark_open": float(row[1]),
                    "mark_high": float(row[2]),
                    "mark_low": float(row[3]),
                    "mark_close": float(row[4]),
                }
                for row in rows
            ],
            columns=MARK_COLUMNS,
        )
        return df.reset_index(drop=True)

    def fetch_index(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        index_symbol = "-".join(symbol.split("-")[:2])
        rows = self._fetch_candles("/api/v5/market/history-index-candles", symbol, start, end, timeframe, inst_id=index_symbol)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row[0], unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "index_open": float(row[1]),
                    "index_high": float(row[2]),
                    "index_low": float(row[3]),
                    "index_close": float(row[4]),
                }
                for row in rows
            ],
            columns=INDEX_COLUMNS,
        )
        return df.reset_index(drop=True)

    def fetch_funding(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        cursor_end = utc_ts(end)
        start_ts = utc_ts(start)
        while cursor_end > start_ts:
            payload = self.client.get("/api/v5/public/funding-rate-history", {"instId": symbol, "after": ms(cursor_end), "limit": 100})
            chunk = payload.get("data", [])
            if not chunk:
                break
            rows.extend(chunk)
            oldest = min(pd.to_datetime(int(row["fundingTime"]), unit="ms", utc=True) for row in chunk)
            if oldest <= start_ts or oldest >= cursor_end:
                break
            cursor_end = oldest
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row["fundingTime"], unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "funding_rate": float(row["fundingRate"]),
                    "mark_price_at_funding": pd.NA,
                }
                for row in rows
                if start_ts <= pd.to_datetime(int(row["fundingTime"]), unit="ms", utc=True) < utc_ts(end)
            ],
            columns=FUNDING_COLUMNS,
        ).sort_values("ts")
        return df.reset_index(drop=True)

    def fetch_open_interest(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        payload = self.client.get("/api/v5/public/open-interest", {"instType": "SWAP", "instId": symbol})
        rows = payload.get("data", [])
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row.get("ts"), unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "open_interest": float(row["oi"]),
                    "open_interest_value": float(row["oiCcy"]) if row.get("oiCcy") not in (None, "") else pd.NA,
                }
                for row in rows
            ],
            columns=OI_COLUMNS,
        ).sort_values("ts")
        return df[(df["ts"] >= utc_ts(start)) & (df["ts"] < utc_ts(end))].reset_index(drop=True)
