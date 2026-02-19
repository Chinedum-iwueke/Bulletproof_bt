"""Run manifest artifact generation."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bt.logging.artifacts_manifest import (
    ARTIFACTS_MANIFEST_SCHEMA_VERSION,
    write_artifacts_manifest,
)
from bt.logging.formatting import write_json_deterministic

_MANIFEST_FILENAME = "run_manifest.json"
ARTIFACTS_MANIFEST_VERSION = ARTIFACTS_MANIFEST_SCHEMA_VERSION


def _artifact_files(run_dir: Path) -> list[str]:
    artifacts = sorted(path.name for path in run_dir.iterdir() if path.is_file())
    if _MANIFEST_FILENAME not in artifacts:
        artifacts.append(_MANIFEST_FILENAME)
        artifacts.sort()
    return artifacts


def _config_summary(config: dict[str, Any]) -> dict[str, Any]:
    strategy = None
    strategy_cfg = config.get("strategy")
    if isinstance(strategy_cfg, dict):
        strategy_name = strategy_cfg.get("name")
        if isinstance(strategy_name, str):
            strategy = strategy_name

    data_cfg = config.get("data")
    risk_cfg = config.get("risk")
    execution_cfg = config.get("execution")

    return {
        "strategy": strategy,
        "data_mode": data_cfg.get("mode", "unknown") if isinstance(data_cfg, dict) else "unknown",
        "risk_mode": risk_cfg.get("mode", "unknown") if isinstance(risk_cfg, dict) else "unknown",
        "execution_profile": (
            execution_cfg.get("profile", "unknown") if isinstance(execution_cfg, dict) else "unknown"
        ),
        "intrabar_mode": (
            execution_cfg.get("intrabar_mode", "unknown") if isinstance(execution_cfg, dict) else "unknown"
        ),
    }


def write_run_manifest(
    run_dir: Path,
    *,
    config: dict[str, Any],
    data_path: str,
) -> Path:
    """
    Writes run_manifest.json into run_dir and returns its path.

    Must be deterministic.
    """
    if not run_dir.exists():
        raise ValueError(f"run_dir does not exist: {run_dir}")
    if not run_dir.is_dir():
        raise ValueError(f"run_dir is not a directory: {run_dir}")
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary")

    created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    benchmark_cfg = config.get("benchmark")
    benchmark_enabled = benchmark_cfg.get("enabled", False) if isinstance(benchmark_cfg, dict) else False

    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_dir": str(run_dir),
        "created_at_utc": created_at_utc,
        "data_path": data_path,
        "is_dataset_dir": Path(data_path).is_dir(),
        "benchmark_enabled": bool(benchmark_enabled),
        "artifact_files": _artifact_files(run_dir),
        "config_summary": _config_summary(config),
    }

    manifest_path = run_dir / _MANIFEST_FILENAME
    write_json_deterministic(manifest_path, payload)
    return manifest_path
