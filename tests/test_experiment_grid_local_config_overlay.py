from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest
import yaml


def _load_run_grid_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "run_experiment_grid.py"
    spec = importlib.util.spec_from_file_location("run_experiment_grid", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_dataset(dataset_dir: Path) -> Path:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    ts_index = pd.date_range("2024-01-01", periods=5, freq="min", tz="UTC")
    rows: list[dict[str, object]] = []
    for i, ts in enumerate(ts_index):
        base = 100 + i
        rows.append(
            {
                "ts": ts,
                "symbol": "BTCUSDT",
                "open": float(base),
                "high": float(base + 1),
                "low": float(base - 1),
                "close": float(base + 0.5),
                "volume": float(1000 + i),
            }
        )
    bars = pd.DataFrame(rows)
    bars.to_parquet(dataset_dir / "bars.parquet", index=False)
    with (dataset_dir / "manifest.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump({"version": 1, "format": "parquet", "files": ["bars.parquet"]}, handle, sort_keys=False)
    return dataset_dir / "bars.parquet"


def _write_base_config(path: Path) -> None:
    cfg = {
        "execution_tier": 2,
        "initial_cash": 10000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 0,
        "risk": {"max_positions": 5, "risk_per_trade_pct": 0.001},
        "strategy": {"name": "coinflip", "p_trade": 0.0, "cooldown_bars": 0},
    }
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")


def _write_experiment(path: Path, *, fixed: dict | None = None, grid: dict | None = None) -> None:
    exp = {
        "version": 1,
        "fixed": fixed or {"strategy": {"name": "coinflip"}},
        "grid": grid or {"strategy.p_trade": [1.0], "strategy.cooldown_bars": [0]},
    }
    path.write_text(yaml.safe_dump(exp, sort_keys=False), encoding="utf-8")


def _load_single_run_cfg(out_dir: Path) -> dict:
    run_dirs = sorted((out_dir / "runs").iterdir())
    assert len(run_dirs) == 1
    cfg_path = run_dirs[0] / "config_used.yaml"
    assert cfg_path.exists()
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


def test_local_overlay_applied(tmp_path: Path) -> None:
    module = _load_run_grid_module()
    base_path = tmp_path / "engine.yaml"
    local_path = tmp_path / "engine.local.yaml"
    exp_path = tmp_path / "experiment.yaml"
    out_path = tmp_path / "out"

    _write_base_config(base_path)
    local_path.write_text(
        yaml.safe_dump({"risk": {"max_positions": 1}, "data": {"symbols_subset": ["BTCUSDT"]}}, sort_keys=False),
        encoding="utf-8",
    )
    _write_experiment(exp_path)
    data_path = _write_dataset(tmp_path / "dataset")

    exit_code = module.run_grid(base_path, exp_path, str(data_path), out_path, local_config_path=str(local_path))
    assert exit_code == 0

    run_cfg = _load_single_run_cfg(out_path)
    assert run_cfg["risk"]["max_positions"] == 1
    assert run_cfg["data"]["symbols_subset"] == ["BTCUSDT"]


def test_precedence_fixed_beats_local(tmp_path: Path) -> None:
    module = _load_run_grid_module()
    base_path = tmp_path / "engine.yaml"
    local_path = tmp_path / "engine.local.yaml"
    exp_path = tmp_path / "experiment.yaml"
    out_path = tmp_path / "out"

    _write_base_config(base_path)
    local_path.write_text(yaml.safe_dump({"risk": {"max_positions": 1}}, sort_keys=False), encoding="utf-8")
    _write_experiment(exp_path, fixed={"strategy": {"name": "coinflip"}, "risk": {"max_positions": 3}})
    data_path = _write_dataset(tmp_path / "dataset")

    exit_code = module.run_grid(base_path, exp_path, str(data_path), out_path, local_config_path=str(local_path))
    assert exit_code == 0

    run_cfg = _load_single_run_cfg(out_path)
    assert run_cfg["risk"]["max_positions"] == 3


def test_precedence_grid_beats_local_and_fixed(tmp_path: Path) -> None:
    module = _load_run_grid_module()
    base_path = tmp_path / "engine.yaml"
    local_path = tmp_path / "engine.local.yaml"
    exp_path = tmp_path / "experiment.yaml"
    out_path = tmp_path / "out"

    _write_base_config(base_path)
    local_path.write_text(yaml.safe_dump({"strategy": {"p_trade": 0.1}}, sort_keys=False), encoding="utf-8")
    _write_experiment(
        exp_path,
        fixed={"strategy": {"name": "coinflip", "p_trade": 0.2}},
        grid={"strategy.p_trade": [0.9], "strategy.cooldown_bars": [0]},
    )
    data_path = _write_dataset(tmp_path / "dataset")

    exit_code = module.run_grid(base_path, exp_path, str(data_path), out_path, local_config_path=str(local_path))
    assert exit_code == 0

    run_cfg = _load_single_run_cfg(out_path)
    assert run_cfg["strategy"]["p_trade"] == 0.9


def test_missing_local_config_path_raises_clear_error(tmp_path: Path) -> None:
    module = _load_run_grid_module()
    base_path = tmp_path / "engine.yaml"
    exp_path = tmp_path / "experiment.yaml"
    out_path = tmp_path / "out"

    _write_base_config(base_path)
    _write_experiment(exp_path)
    data_path = _write_dataset(tmp_path / "dataset")

    missing_path = "/nope/missing.yaml"
    with pytest.raises(ValueError, match=r"local-config") as exc_info:
        module.run_grid(
            base_path,
            exp_path,
            str(data_path),
            out_path,
            local_config_path=missing_path,
        )
    assert missing_path in str(exc_info.value)
