from __future__ import annotations

import pandas as pd

from bt.research_data.jobs.coverage import coverage_for_frame, write_coverage_dashboard
from bt.research_data.jobs.daily_validation import run_daily_validation
from bt.research_data.storage import ResearchDataStore


def _ohlcv(symbol: str = "BTCUSDT") -> pd.DataFrame:
    ts = pd.to_datetime(
        ["2021-01-01 00:00", "2021-01-01 00:02", "2021-01-01 00:02"],
        utc=True,
    )
    return pd.DataFrame(
        {
            "ts": ts,
            "exchange": "binance",
            "symbol": symbol,
            "canonical_symbol": "BTC-USDT-PERP",
            "open": [1.0, 2.0, 2.0],
            "high": [1.5, 2.5, 2.5],
            "low": [0.5, 1.5, 1.5],
            "close": [1.2, 2.2, 2.2],
            "volume": [10.0, 20.0, 20.0],
        }
    )


def _panel() -> pd.DataFrame:
    ts = pd.date_range(pd.Timestamp("2021-01-01", tz="UTC"), periods=2, freq="1min")
    return pd.DataFrame(
        {
            "ts": ts,
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "canonical_symbol": "BTC-USDT-PERP",
            "open": [1.0, 2.0],
            "high": [1.5, 2.5],
            "low": [0.5, 1.5],
            "close": [1.2, 2.2],
            "volume": [10.0, 20.0],
            "mark_close": [1.2, 2.2],
            "index_close": [1.2, 2.2],
            "funding_source_ts": ts,
            "oi_source_ts": ts,
        }
    )


def test_coverage_calculates_missing_duplicates_and_gaps() -> None:
    row = coverage_for_frame(_ohlcv(), "binance", "BTCUSDT", "ohlcv", "1m")

    assert row["expected_rows"] == 3
    assert row["actual_rows"] == 2
    assert row["missing_rows"] == 1
    assert row["duplicate_rows"] == 1
    assert row["gap_count"] == 1
    assert row["largest_gap_minutes"] == 2.0
    assert row["status"] == "warning"


def test_daily_validation_catches_duplicate_timestamps(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    path = store.canonical_path("binance", "BTCUSDT", "1m", "research_panel")
    panel = _panel()
    panel = pd.concat([panel, panel.iloc[[0]]], ignore_index=True)
    store.write_atomic(panel, path)

    report = run_daily_validation(store, exchange="binance")

    assert "failed" in set(report["status"])
    assert report["message"].str.contains("duplicate timestamps").any()


def test_daily_validation_catches_future_source_timestamps(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    path = store.canonical_path("binance", "BTCUSDT", "1m", "research_panel")
    panel = _panel()
    panel.loc[0, "oi_source_ts"] = pd.Timestamp("2021-01-01 00:01", tz="UTC")
    store.write_atomic(panel, path)

    report = run_daily_validation(store, exchange="binance")

    assert report["message"].str.contains("oi_source_ts > ts").any()


def test_daily_validation_catches_bad_ohlcv(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    path = store.canonical_path("binance", "BTCUSDT", "1m", "research_panel")
    panel = _panel()
    panel.loc[0, "high"] = 1.0
    panel.loc[0, "close"] = 2.0
    store.write_atomic(panel, path)

    report = run_daily_validation(store, exchange="binance")

    assert report["message"].str.contains("OHLC").any()


def test_dashboard_writes_static_html(tmp_path) -> None:
    store = ResearchDataStore(tmp_path / "research_data")
    coverage = pd.DataFrame(
        [
            {
                "exchange": "binance",
                "native_symbol": "BTCUSDT",
                "canonical_symbol": "BTC-USDT-PERP",
                "dataset": "ohlcv",
                "timeframe": "1m",
                "start_ts": pd.Timestamp("2021-01-01", tz="UTC"),
                "end_ts": pd.Timestamp("2021-01-01", tz="UTC"),
                "expected_rows": 1,
                "actual_rows": 1,
                "missing_rows": 0,
                "duplicate_rows": 0,
                "gap_count": 0,
                "largest_gap_minutes": 0.0,
                "last_updated_at": pd.Timestamp("2021-01-01", tz="UTC"),
                "status": "ok",
            }
        ]
    )
    store.write_atomic(coverage, store.manifest_path("coverage"))

    path = write_coverage_dashboard(store)

    assert path.exists()
    assert "Research Data Coverage" in path.read_text()
