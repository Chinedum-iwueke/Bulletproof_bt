from __future__ import annotations

import pandas as pd

from bt.research_data.exchanges.binance import BinanceUSDMPerpAdapter
from bt.research_data.exchanges.bybit import BybitUSDTPerpAdapter, normalize_bybit_instruments
from bt.research_data.exchanges.okx import normalize_okx_instruments
from bt.research_data.instruments import native_to_canonical_symbol
from bt.research_data.schemas import INSTRUMENT_COLUMNS, OHLCV_COLUMNS


class BinanceInstrumentClient:
    def get(self, path, params=None):
        assert path == "/fapi/v1/exchangeInfo"
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "baseAsset": "BTC",
                    "marginAsset": "USDT",
                    "status": "TRADING",
                    "onboardDate": 1609459200000,
                    "deliveryDate": 4133404800000,
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                }
            ]
        }


class BinanceKlineClient:
    def get(self, path, params=None):
        return [[1609459200000, "1", "2", "0.5", "1.5", "10", 1609459259999, "15", 42]]


class BybitKlineClient:
    def get(self, path, params=None):
        return {
            "retCode": 0,
            "result": {
                "list": [
                    ["1609459200000", "1", "2", "0.5", "1.5", "10", "15"],
                    ["1609459260000", "2", "3", "2.5", "2.1", "10", "15"],
                    ["1609459320000", "2", "3", "1.9", "2.2", "10", "15"],
                ]
            },
        }


def test_binance_instrument_payload_normalizes_to_canonical_schema() -> None:
    df = BinanceUSDMPerpAdapter(BinanceInstrumentClient()).fetch_usdt_perp_instruments()

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "exchange"] == "binance"
    assert df.loc[0, "native_symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"
    assert df.loc[0, "settle_asset"] == "USDT"


def test_bybit_instrument_payload_normalizes_to_canonical_schema() -> None:
    df = normalize_bybit_instruments(
        {
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "LinearPerpetual",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "settleCoin": "USDT",
                        "status": "Trading",
                        "launchTime": "1609459200000",
                        "priceFilter": {"tickSize": "0.10"},
                        "lotSizeFilter": {"qtyStep": "0.001"},
                    }
                ]
            }
        }
    )

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "exchange"] == "bybit"
    assert df.loc[0, "native_symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"
    assert df.loc[0, "price_precision"] == 1
    assert df.loc[0, "qty_precision"] == 3


def test_okx_instrument_payload_normalizes_to_canonical_schema() -> None:
    df = normalize_okx_instruments(
        {
            "data": [
                {
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "settleCcy": "USDT",
                    "state": "live",
                    "tickSz": "0.1",
                    "lotSz": "0.01",
                }
            ]
        }
    )

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "exchange"] == "okx"
    assert df.loc[0, "native_symbol"] == "BTC-USDT-SWAP"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"
    assert df.loc[0, "price_precision"] == 1
    assert df.loc[0, "qty_precision"] == 2


def test_native_symbol_mapping_examples() -> None:
    assert native_to_canonical_symbol("BTCUSDT") == "BTC-USDT-PERP"
    assert native_to_canonical_symbol("BTC-USDT-SWAP") == "BTC-USDT-PERP"


def test_binance_ohlcv_includes_native_and_canonical_symbol() -> None:
    df = BinanceUSDMPerpAdapter(BinanceKlineClient()).fetch_ohlcv(
        "BTCUSDT",
        pd.Timestamp("2021-01-01", tz="UTC"),
        pd.Timestamp("2021-01-01 00:02", tz="UTC"),
    )

    assert tuple(df.columns) == OHLCV_COLUMNS
    assert df.loc[0, "symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"


def test_bybit_ohlcv_drops_invalid_exchange_candles() -> None:
    df = BybitUSDTPerpAdapter(BybitKlineClient()).fetch_ohlcv(
        "BTCUSDT",
        pd.Timestamp("2021-01-01", tz="UTC"),
        pd.Timestamp("2021-01-01 00:04", tz="UTC"),
    )

    assert tuple(df.columns) == OHLCV_COLUMNS
    assert df["ts"].tolist() == [
        pd.Timestamp("2021-01-01 00:00", tz="UTC"),
        pd.Timestamp("2021-01-01 00:02", tz="UTC"),
    ]
