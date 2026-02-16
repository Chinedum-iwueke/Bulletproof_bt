from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.api import run_backtest


def _bars_frame(symbol: str, closes: list[float]) -> pd.DataFrame:
    ts_index = pd.date_range("2024-01-01", periods=len(closes), freq="min", tz="UTC")
    rows: list[dict[str, object]] = []
    for ts, close in zip(ts_index, closes, strict=True):
        rows.append(
            {
                "ts": ts,
                "symbol": symbol,
                "open": float(close),
                "high": float(close) + 1.0,
                "low": float(close) - 1.0,
                "close": float(close),
                "volume": 10.0,
            }
        )
    return pd.DataFrame(rows)


def _write_dataset(dataset_dir: Path, symbols: list[str]) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)

    if "BTCUSDT" in symbols:
        _bars_frame("BTCUSDT", [100.0, 110.0, 90.0]).to_parquet(dataset_dir / "BTCUSDT.parquet", index=False)
    if "ETHUSDT" in symbols:
        _bars_frame("ETHUSDT", [200.0, 220.0, 210.0]).to_parquet(dataset_dir / "ETHUSDT.parquet", index=False)

    manifest = {
        "format": "per_symbol_parquet",
        "symbols": symbols,
        "path": "{symbol}.parquet",
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def _write_config(path: Path, *, benchmark_symbol: str) -> None:
    cfg = {
        "initial_cash": 1000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 1,
        "risk": {
            "max_positions": 1,
            "risk_per_trade_pct": 0.001,
        },
        "strategy": {
            "name": "coinflip",
            "seed": 1,
            "p_trade": 0.0,
            "cooldown_bars": 0,
        },
        "benchmark": {
            "enabled": True,
            "symbol": benchmark_symbol,
            "price_field": "close",
        },
    }
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")


def test_benchmark_equity_artifact_written(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir, ["BTCUSDT", "ETHUSDT"])

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path, benchmark_symbol="BTCUSDT")

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out"),
        )
    )

    benchmark_path = run_dir / "benchmark_equity.csv"
    assert benchmark_path.exists()

    with benchmark_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 3
    equities = [float(row["equity"]) for row in rows]
    assert equities == [1000.0, 1100.0, 900.0]


def test_benchmark_missing_from_dataset_scope_raises_early(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir, ["ETHUSDT"])

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path, benchmark_symbol="BTCUSDT")

    with pytest.raises(ValueError, match=r"benchmark\.symbol=.*dataset_dir="):
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out"),
        )
