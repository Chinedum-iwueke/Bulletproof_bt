from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import yaml

import importlib.util


def _load_run_grid():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "run_experiment_grid.py"
    spec = importlib.util.spec_from_file_location("run_experiment_grid", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.run_grid


run_grid = _load_run_grid()


def _write_manifest_dataset(dataset_dir: Path) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)

    ts_index = pd.date_range("2024-01-01", periods=30, freq="min", tz="UTC")
    rows: list[dict[str, object]] = []
    for symbol in ["AAA", "BBB"]:
        for idx, ts in enumerate(ts_index):
            base = 100 + idx
            rows.append(
                {
                    "ts": ts,
                    "symbol": symbol,
                    "open": float(base),
                    "high": float(base + 0.5),
                    "low": float(base - 0.5),
                    "close": float(base + 0.25),
                    "volume": float(1000 + idx),
                }
            )

    bars = pd.DataFrame(rows)
    bars.to_parquet(dataset_dir / "bars.parquet", index=False)

    manifest = {"version": 1, "format": "parquet", "files": ["bars.parquet"]}
    with (dataset_dir / "manifest.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(manifest, handle, sort_keys=False)


def test_experiment_grid_runner_deterministic_outputs(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_manifest_dataset(dataset_dir)

    base_config = {
        "signal_delay_bars": 1,
        "initial_cash": 100000.0,
        "max_leverage": 10.0,
        "max_positions": 1,
        "risk_per_trade_pct": 0.0001,
        "maker_fee_bps": 1.0,
        "taker_fee_bps": 2.0,
        "slippage_k": 0.01,
        "htf_timeframes": ["15m"],
        "strategy": {
            "name": "coinflip",
            "seed": 7,
            "p_trade": 0.0,
            "cooldown_bars": 0,
        },
    }
    base_cfg_path = tmp_path / "engine.yaml"
    with base_cfg_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(base_config, handle, sort_keys=False)

    experiment_cfg = {
        "version": 1,
        "name": "test_grid",
        "fixed": {
            "timeframe": "15m",
            "strategy": {"name": "coinflip"},
        },
        "grid": {
            "strategy.vol_floor_pct": [40, 50, 60, 70, 80],
            "strategy.adx_min": [18, 22, 25],
        },
        "run_naming": {
            "template": "adx{strategy.adx_min}_vol{strategy.vol_floor_pct}",
        },
    }
    exp_path = tmp_path / "experiment.yaml"
    with exp_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(experiment_cfg, handle, sort_keys=False)

    out_dir = tmp_path / "out"
    run_grid(base_cfg_path, exp_path, str(dataset_dir), out_dir)

    run_dirs = sorted((out_dir / "runs").iterdir())
    assert len(run_dirs) == 15

    assert run_dirs[0].name == "run_001__adx18_vol40"
    assert run_dirs[-1].name == "run_015__adx25_vol80"

    for run_dir in run_dirs:
        assert (run_dir / "config_used.yaml").exists()

    summary_csv = out_dir / "summary.csv"
    assert summary_csv.exists()
    with summary_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    assert len(rows) == 16

    summary_json = out_dir / "summary.json"
    assert summary_json.exists()
    with summary_json.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert len(payload["runs"]) == 15

    target_cfg_path = out_dir / "runs" / "run_009__adx22_vol70" / "config_used.yaml"
    with target_cfg_path.open("r", encoding="utf-8") as handle:
        target_cfg = yaml.safe_load(handle)
    assert target_cfg["strategy"]["adx_min"] == 22
    assert target_cfg["strategy"]["vol_floor_pct"] == 70

    grid_used_path = out_dir / "grid_used.yaml"
    assert grid_used_path.exists()
    with grid_used_path.open("r", encoding="utf-8") as handle:
        grid_used = yaml.safe_load(handle)
    assert grid_used["experiment"]["name"] == "test_grid"
    assert grid_used["resolved_grid_keys"] == ["strategy.adx_min", "strategy.vol_floor_pct"]
