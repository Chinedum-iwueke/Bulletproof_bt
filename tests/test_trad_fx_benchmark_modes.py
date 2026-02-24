from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.api import run_backtest
from bt.benchmark.spec import BenchmarkSpec, parse_benchmark_spec
from bt.benchmark.tracker import BenchmarkTracker


def _write_manifest(dataset_dir: Path, symbols: list[str]) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.yaml").write_text(
        yaml.safe_dump({"format": "per_symbol_parquet", "symbols": symbols, "path": "symbols/{symbol}.parquet"}, sort_keys=False),
        encoding="utf-8",
    )


def _write_symbol(dataset_dir: Path, symbol: str, closes: list[float]) -> None:
    rows = []
    for i, close in enumerate(closes):
        ts = pd.Timestamp("2024-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
        rows.append({"ts": ts, "open": close, "high": close, "low": close, "close": close, "volume": 1.0, "symbol": symbol})
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(symbols_dir / f"{symbol}.parquet", index=False)


def _load_equity(path: Path) -> list[tuple[str, float]]:
    with path.open("r", encoding="utf-8", newline="") as h:
        rows = list(csv.DictReader(h))
    return [(pd.Timestamp(r["ts"]).tz_convert("UTC").isoformat(), float(r["equity"])) for r in rows]


def _run(tmp_path: Path, benchmark_cfg: dict, closes: list[float], symbol: str = "BTCUSDT") -> Path:
    dataset = tmp_path / "dataset"
    _write_manifest(dataset, [symbol])
    _write_symbol(dataset, symbol, closes)

    cfg = {
        "initial_cash": 1000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 1,
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
        "strategy": {"name": "coinflip", "seed": 1, "p_trade": 0.0, "cooldown_bars": 0},
        "data": {"mode": "streaming", "symbols_subset": [symbol]},
        "execution": {"profile": "tier2", "spread_mode": "none"},
        "benchmark": benchmark_cfg,
    }
    cfg_path = tmp_path / "engine.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    return Path(
        run_backtest(
            config_path=str(cfg_path),
            data_path=str(dataset),
            out_dir=str(tmp_path / "out"),
            run_name="bench-modes",
        )
    )


def test_flat_baseline_constant_equity(tmp_path: Path) -> None:
    run_dir = _run(tmp_path, {"enabled": True, "type": "flat", "initial_equity": 1000.0}, [100, 101, 99, 102])

    bench_eq = _load_equity(run_dir / "benchmark_equity.csv")
    assert len(bench_eq) == 4
    assert all(v == pytest.approx(1000.0) for _, v in bench_eq)

    metrics = json.loads((run_dir / "benchmark_metrics.json").read_text(encoding="utf-8"))
    assert metrics["total_return"] == pytest.approx(0.0)
    assert metrics["max_drawdown"] == pytest.approx(0.0)

    summary = json.loads((run_dir / "comparison_summary.json").read_text(encoding="utf-8"))
    assert summary["benchmark_return"] == pytest.approx(0.0)
    assert summary["alpha"] == pytest.approx(summary["strategy_return"])


def test_buy_hold_unchanged_legacy_behavior(tmp_path: Path) -> None:
    run_dir = _run(tmp_path, {"enabled": True, "symbol": "BTCUSDT", "price_field": "close", "initial_equity": 1000.0}, [100, 110, 90])
    bench_eq = _load_equity(run_dir / "benchmark_equity.csv")
    assert [v for _, v in bench_eq] == [1000.0, 1100.0, 900.0]


def test_baseline_strategy_ma_cross_deterministic(tmp_path: Path) -> None:
    closes = [10, 10, 10, 11, 12, 13, 14, 12, 11, 10]
    run_dir = _run(
        tmp_path,
        {
            "enabled": True,
            "type": "baseline_strategy",
            "symbol": "BTCUSDT",
            "baseline_strategy": {"name": "ma_cross", "params": {"fast": 2, "slow": 3}},
            "initial_equity": 1000.0,
        },
        closes,
    )

    bench_eq = _load_equity(run_dir / "benchmark_equity.csv")
    assert len(bench_eq) == len(closes)
    values = [v for _, v in bench_eq]
    assert max(values) != min(values)

    metrics = json.loads((run_dir / "benchmark_metrics.json").read_text(encoding="utf-8"))
    assert "total_return" in metrics
    assert metrics.get("benchmark_type") == "baseline_strategy"


def test_validation_guards() -> None:
    with pytest.raises(ValueError, match=r"benchmark\.baseline_strategy\.name"):
        parse_benchmark_spec({"benchmark": {"enabled": True, "type": "baseline_strategy", "baseline_strategy": {}}})

    with pytest.raises(ValueError, match=r"benchmark\.symbol"):
        parse_benchmark_spec({"benchmark": {"enabled": True, "type": "buy_hold"}})

    bad_tracker = BenchmarkTracker(
        BenchmarkSpec(
            enabled=True,
            mode="baseline_strategy",
            symbol="BTCUSDT",
            baseline_strategy_name="unknown",
            baseline_strategy_params={},
        )
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = type("Bar", (), {"symbol": "BTCUSDT", "ts": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0})()
    bad_tracker.on_tick(ts.to_pydatetime(), {"BTCUSDT": bar})
    with pytest.raises(ValueError, match=r"Supported names: \['ma_cross'\]"):
        bad_tracker.finalize(initial_equity=1000.0)
