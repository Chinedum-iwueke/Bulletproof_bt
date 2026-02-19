"""Run manifest artifact generation."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from bt.logging.formatting import write_json_deterministic


_MANIFEST_FILENAME = "run_manifest.json"
ARTIFACTS_MANIFEST_VERSION = 1


@dataclass(frozen=True)
class ArtifactEntry:
    name: str
    path: str
    kind: Literal["csv", "json", "jsonl", "yaml", "txt"]
    required: bool
    produced: bool
    conditional_on: str | None
    notes: str | None = None


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


def _artifact_definitions() -> list[ArtifactEntry]:
    return [
        ArtifactEntry("benchmark_equity.csv", "benchmark_equity.csv", "csv", False, False, "benchmark.enabled"),
        ArtifactEntry("benchmark_metrics.json", "benchmark_metrics.json", "json", False, False, "benchmark.enabled"),
        ArtifactEntry("comparison_summary.json", "comparison_summary.json", "json", False, False, "benchmark.enabled"),
        ArtifactEntry("config_used.yaml", "config_used.yaml", "yaml", True, False, None),
        ArtifactEntry("data_scope.json", "data_scope.json", "json", False, False, "data.scope_knobs_active"),
        ArtifactEntry("decisions.jsonl", "decisions.jsonl", "jsonl", True, False, None),
        ArtifactEntry("equity.csv", "equity.csv", "csv", True, False, None),
        ArtifactEntry("fills.jsonl", "fills.jsonl", "jsonl", True, False, None),
        ArtifactEntry("performance.json", "performance.json", "json", True, False, None),
        ArtifactEntry("performance_by_bucket.csv", "performance_by_bucket.csv", "csv", True, False, None),
        ArtifactEntry("run_status.json", "run_status.json", "json", True, False, None),
        ArtifactEntry(
            "summary.txt",
            "summary.txt",
            "txt",
            False,
            False,
            "stage_f.summary",
            "Produced when Stage F summary writer runs.",
        ),
        ArtifactEntry("trades.csv", "trades.csv", "csv", True, False, None),
    ]


def write_artifacts_manifest(
    run_dir: Path,
    *,
    config: dict[str, Any],
) -> Path:
    """
    Writes run_dir/artifacts_manifest.json deterministically.
    Returns the path.
    """
    if not run_dir.exists():
        raise ValueError(
            f"run_dir does not exist: {run_dir}. Ensure the run directory is created before writing artifacts_manifest.json."
        )
    if not run_dir.is_dir():
        raise ValueError(
            f"run_dir is not a directory: {run_dir}. Expected a writable run directory path."
        )
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary")

    benchmark_cfg = config.get("benchmark")
    benchmark_enabled = bool(benchmark_cfg.get("enabled", False)) if isinstance(benchmark_cfg, dict) else False

    artifacts: list[dict[str, Any]] = []
    for entry in sorted(_artifact_definitions(), key=lambda artifact: artifact.name):
        path = run_dir / entry.path
        artifacts.append(asdict(ArtifactEntry(**{**asdict(entry), "produced": path.exists()})))

    payload: dict[str, Any] = {
        "schema_version": ARTIFACTS_MANIFEST_VERSION,
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_dir": run_dir.name,
        "benchmark_enabled": benchmark_enabled,
        "artifacts": artifacts,
    }

    manifest_path = run_dir / "artifacts_manifest.json"
    try:
        write_json_deterministic(manifest_path, payload)
    except OSError as exc:
        raise ValueError(
            f"Unable to write artifacts_manifest.json in run_dir={run_dir}. Check that the directory exists and is writable: {exc}"
        ) from exc
    return manifest_path


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
