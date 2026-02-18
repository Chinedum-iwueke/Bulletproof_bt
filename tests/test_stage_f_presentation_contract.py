from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.api import run_backtest
from bt.logging.run_contract import validate_run_artifacts
from bt.logging.run_manifest import write_run_manifest
from bt.logging.summary import write_summary_txt


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


def _write_dataset(dataset_dir: Path) -> None:
    symbols = ["AAA", "BBB"]
    _write_legacy_manifest(dataset_dir, symbols)

    _write_symbol_parquet(
        dataset_dir,
        "AAA",
        [
            ("2024-01-01T00:00:00Z", 100.0, 101.0, 99.0, 100.0, 1.0),
            ("2024-01-01T00:01:00Z", 101.0, 102.0, 100.0, 101.0, 1.1),
            ("2024-01-01T00:02:00Z", 102.0, 103.0, 101.0, 102.0, 1.2),
        ],
    )
    _write_symbol_parquet(
        dataset_dir,
        "BBB",
        [
            ("2024-01-01T00:00:00Z", 200.0, 201.0, 199.0, 200.0, 2.0),
            ("2024-01-01T00:01:00Z", 199.0, 200.0, 198.0, 199.0, 2.1),
            ("2024-01-01T00:02:00Z", 198.0, 199.0, 197.0, 198.0, 2.2),
        ],
    )


def _write_config(path: Path, *, benchmark_enabled: bool = False) -> dict[str, Any]:
    config: dict[str, Any] = {
        "initial_cash": 1000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 1,
        "risk": {
            "max_positions": 1,
            "risk_per_trade_pct": 0.001,
        },
        "strategy": {
            "name": "coinflip",
            "seed": 7,
            "p_trade": 0.0,
            "cooldown_bars": 0,
        },
        "data": {"mode": "streaming"},
        "benchmark": {
            "enabled": benchmark_enabled,
            "symbol": "AAA",
            "price_field": "close",
            "initial_equity": 1000.0,
        },
    }
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config


def _list_files(run_dir: Path) -> list[str]:
    return sorted(path.name for path in run_dir.iterdir() if path.is_file())


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _canonicalize_json(d: dict[str, Any]) -> str:
    return json.dumps(d, sort_keys=True, indent=2)


def _strip_generated_line(summary_text: str) -> str:
    return "\n".join(line for line in summary_text.splitlines() if not line.startswith("Generated:"))


def _run_contract_backtest(
    tmp_path: Path,
    *,
    out_dir_name: str,
    run_name: str,
    benchmark_enabled: bool = False,
) -> tuple[Path, Path, dict[str, Any]]:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir)

    config_path = tmp_path / "engine.yaml"
    config = _write_config(config_path, benchmark_enabled=benchmark_enabled)

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / out_dir_name),
            run_name=run_name,
        )
    )
    validate_run_artifacts(run_dir)
    write_summary_txt(run_dir)
    write_run_manifest(run_dir, config=config, data_path=str(dataset_dir))
    return run_dir, dataset_dir, config


def _normalize_run_status(payload: dict[str, Any]) -> dict[str, Any]:
    ignored = {
        "created_at_utc",
        "created_at",
        "generated_at",
        "timestamp",
        "started_at_utc",
        "finished_at_utc",
        "run_started_at_utc",
        "run_finished_at_utc",
    }

    def _norm(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _norm(v) for k, v in value.items() if k not in ignored}
        if isinstance(value, list):
            return [_norm(item) for item in value]
        return value

    normalized = _norm(payload)
    assert isinstance(normalized, dict)
    return normalized


def _normalize_run_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized.pop("created_at_utc", None)
    normalized.pop("run_id", None)
    normalized.pop("run_dir", None)
    return normalized


def test_run_folder_contains_required_artifacts_end_to_end(tmp_path: Path) -> None:
    run_dir, _, _ = _run_contract_backtest(
        tmp_path,
        out_dir_name="out",
        run_name="presentation-contract",
    )

    required_artifacts = {
        "config_used.yaml",
        "performance.json",
        "equity.csv",
        "trades.csv",
        "fills.jsonl",
        "decisions.jsonl",
        "performance_by_bucket.csv",
        "run_manifest.json",
        "summary.txt",
    }

    actual_files = set(_list_files(run_dir))
    assert required_artifacts.issubset(actual_files)


def test_run_manifest_lists_run_dir_files(tmp_path: Path) -> None:
    run_dir, dataset_dir, config = _run_contract_backtest(
        tmp_path,
        out_dir_name="out",
        run_name="presentation-contract-manifest",
    )

    manifest = _load_json(run_dir / "run_manifest.json")

    assert manifest["schema_version"] == 1
    artifact_files = manifest["artifact_files"]
    assert artifact_files == sorted(artifact_files)
    assert set(artifact_files) == set(_list_files(run_dir))
    assert manifest["is_dataset_dir"] is dataset_dir.is_dir()
    assert manifest["benchmark_enabled"] is bool(config["benchmark"]["enabled"])


def test_summary_contains_sections(tmp_path: Path) -> None:
    run_dir, _, _ = _run_contract_backtest(
        tmp_path,
        out_dir_name="out",
        run_name="presentation-contract-summary",
    )

    summary = (run_dir / "summary.txt").read_text(encoding="utf-8")

    assert "TOP METRICS" in summary
    assert "WORST STREAK" in summary
    assert "COST DRAG" in summary
    assert "BENCHMARK COMPARISON" in summary
    assert "MOST IMPORTANT CONCLUSION" in summary


def test_repeat_run_deterministic_artifacts(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir)

    config_path = tmp_path / "engine.yaml"
    config = _write_config(config_path, benchmark_enabled=False)

    run_dir_1 = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out_1"),
            run_name="presentation-contract-repeat",
        )
    )
    run_dir_2 = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out_2"),
            run_name="presentation-contract-repeat",
        )
    )

    validate_run_artifacts(run_dir_1)
    validate_run_artifacts(run_dir_2)
    write_summary_txt(run_dir_1)
    write_summary_txt(run_dir_2)
    write_run_manifest(run_dir_1, config=config, data_path=str(dataset_dir))
    write_run_manifest(run_dir_2, config=config, data_path=str(dataset_dir))

    assert (run_dir_1 / "performance.json").read_bytes() == (run_dir_2 / "performance.json").read_bytes()

    run_status_1 = run_dir_1 / "run_status.json"
    run_status_2 = run_dir_2 / "run_status.json"
    if run_status_1.exists() and run_status_2.exists():
        normalized_1 = _normalize_run_status(_load_json(run_status_1))
        normalized_2 = _normalize_run_status(_load_json(run_status_2))
        assert _canonicalize_json(normalized_1) == _canonicalize_json(normalized_2)

    manifest_1 = _normalize_run_manifest(_load_json(run_dir_1 / "run_manifest.json"))
    manifest_2 = _normalize_run_manifest(_load_json(run_dir_2 / "run_manifest.json"))
    assert _canonicalize_json(manifest_1) == _canonicalize_json(manifest_2)

    summary_1 = _strip_generated_line((run_dir_1 / "summary.txt").read_text(encoding="utf-8"))
    summary_2 = _strip_generated_line((run_dir_2 / "summary.txt").read_text(encoding="utf-8"))
    assert summary_1 == summary_2

    assert (run_dir_1 / "equity.csv").read_bytes() == (run_dir_2 / "equity.csv").read_bytes()
    assert (run_dir_1 / "trades.csv").read_bytes() == (run_dir_2 / "trades.csv").read_bytes()
