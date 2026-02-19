from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from bt.api import run_backtest
from bt.contracts.schema_versions import (
    BENCHMARK_METRICS_SCHEMA_VERSION,
    COMPARISON_SUMMARY_SCHEMA_VERSION,
)


def _write_legacy_manifest(dataset_dir: Path, symbols: list[str]) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.yaml").write_text(
        "format: per_symbol_parquet\n"
        f"symbols: [{', '.join(symbols)}]\n"
        'path: "symbols/{symbol}.parquet"\n',
        encoding="utf-8",
    )


def _write_symbol_parquet(
    dataset_dir: Path,
    symbol: str,
    rows: list[tuple[str, float, float, float, float, float]],
) -> None:
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    frame["symbol"] = symbol
    frame.to_parquet(symbols_dir / f"{symbol}.parquet", index=False)


def _write_config(config_path: Path, benchmark_symbol: str) -> None:
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
        "data": {"mode": "streaming"},
        "benchmark": {
            "enabled": True,
            "symbol": benchmark_symbol,
            "price_field": "close",
            "initial_equity": 1000.0,
        },
    }
    config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")


def _write_dataset(dataset_dir: Path, symbols: list[str]) -> None:
    _write_legacy_manifest(dataset_dir, symbols)
    if "BTCUSDT" in symbols:
        _write_symbol_parquet(
            dataset_dir,
            "BTCUSDT",
            [
                ("2024-01-01T00:00:00Z", 100.0, 101.0, 99.0, 100.0, 10.0),
                ("2024-01-01T00:01:00Z", 110.0, 111.0, 109.0, 110.0, 11.0),
                ("2024-01-01T00:02:00Z", 90.0, 91.0, 89.0, 90.0, 12.0),
            ],
        )
    if "ETHUSDT" in symbols:
        _write_symbol_parquet(
            dataset_dir,
            "ETHUSDT",
            [
                ("2024-01-01T00:00:00Z", 200.0, 201.0, 199.0, 200.0, 20.0),
                ("2024-01-01T00:01:00Z", 210.0, 211.0, 209.0, 210.0, 21.0),
                ("2024-01-01T00:02:00Z", 220.0, 221.0, 219.0, 220.0, 22.0),
            ],
        )


def _load_benchmark_equity(path: Path) -> list[tuple[str, float]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [(pd.Timestamp(row["ts"]).tz_convert("UTC").isoformat(), round(float(row["equity"]), 12)) for row in rows]


def _canonicalize_json(value: Any) -> Any:
    if isinstance(value, dict):
        ignored_keys = {"run_id", "timestamp", "created_at", "generated_at"}
        return {
            key: _canonicalize_json(value[key])
            for key in sorted(value)
            if key not in ignored_keys
        }
    if isinstance(value, list):
        return [_canonicalize_json(item) for item in value]
    if isinstance(value, float):
        return round(value, 12)
    return value


def _run_backtest(config_path: Path, dataset_dir: Path, out_dir: Path, run_name: str) -> Path:
    return Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(out_dir),
            run_name=run_name,
        )
    )


def test_benchmark_enabled_writes_all_artifacts(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir, ["BTCUSDT", "ETHUSDT"])

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path, benchmark_symbol="BTCUSDT")

    run_dir = _run_backtest(config_path, dataset_dir, tmp_path / "out", "benchmark-artifacts")

    benchmark_equity_path = run_dir / "benchmark_equity.csv"
    benchmark_metrics_path = run_dir / "benchmark_metrics.json"
    comparison_summary_path = run_dir / "comparison_summary.json"

    assert benchmark_equity_path.exists()
    assert benchmark_metrics_path.exists()
    assert comparison_summary_path.exists()

    equity_rows = _load_benchmark_equity(benchmark_equity_path)
    assert len(equity_rows) == 3

    initial_equity = 1000.0
    first_close = 100.0
    shares = initial_equity / first_close
    expected_equities = [round(shares * close, 12) for close in [100.0, 110.0, 90.0]]
    assert [equity for _, equity in equity_rows] == expected_equities

    benchmark_metrics = json.loads(benchmark_metrics_path.read_text(encoding="utf-8"))
    assert "total_return" in benchmark_metrics
    assert "max_drawdown" in benchmark_metrics
    assert benchmark_metrics["n_points"] == 3
    assert benchmark_metrics["schema_version"] == BENCHMARK_METRICS_SCHEMA_VERSION

    comparison_summary = json.loads(comparison_summary_path.read_text(encoding="utf-8"))
    assert set(comparison_summary.keys()) == {"strategy", "benchmark", "delta", "schema_version"}
    assert comparison_summary["schema_version"] == COMPARISON_SUMMARY_SCHEMA_VERSION
    assert comparison_summary["benchmark"]["total_return"] == pytest.approx(
        benchmark_metrics["total_return"]
    )


def test_benchmark_missing_symbol_raises_valueerror(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir, ["ETHUSDT"])

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path, benchmark_symbol="BTCUSDT")

    with pytest.raises(ValueError) as exc_info:
        _run_backtest(config_path, dataset_dir, tmp_path / "out", "benchmark-missing-symbol")

    message = str(exc_info.value)
    assert "benchmark.symbol" in message
    assert "BTCUSDT" in message
    assert "dataset_dir" in message


def test_benchmark_repeat_run_deterministic_outputs(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir, ["BTCUSDT", "ETHUSDT"])

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path, benchmark_symbol="BTCUSDT")

    run_a = _run_backtest(config_path, dataset_dir, tmp_path / "out_a", "repeat-a")
    run_b = _run_backtest(config_path, dataset_dir, tmp_path / "out_b", "repeat-b")

    equity_a = _load_benchmark_equity(run_a / "benchmark_equity.csv")
    equity_b = _load_benchmark_equity(run_b / "benchmark_equity.csv")
    assert equity_a == equity_b

    metrics_a = _canonicalize_json(json.loads((run_a / "benchmark_metrics.json").read_text(encoding="utf-8")))
    metrics_b = _canonicalize_json(json.loads((run_b / "benchmark_metrics.json").read_text(encoding="utf-8")))
    assert metrics_a == metrics_b

    summary_a = _canonicalize_json(json.loads((run_a / "comparison_summary.json").read_text(encoding="utf-8")))
    summary_b = _canonicalize_json(json.loads((run_b / "comparison_summary.json").read_text(encoding="utf-8")))
    assert summary_a == summary_b
