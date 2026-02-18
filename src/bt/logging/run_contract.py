"""Run artifact contract enforcement."""
from __future__ import annotations

from pathlib import Path

REQUIRED_ARTIFACTS = [
    "config_used.yaml",
    "performance.json",
    "equity.csv",
    "trades.csv",
    "fills.jsonl",
    "decisions.jsonl",
    "performance_by_bucket.csv",
]


def validate_run_artifacts(run_dir: Path) -> None:
    """
    Validate that all required artifacts exist in run_dir.

    Raises:
        ValueError if any required artifact is missing.
    """

    missing = sorted(
        artifact_name
        for artifact_name in REQUIRED_ARTIFACTS
        if not (run_dir / artifact_name).exists()
    )
    if missing:
        missing_lines = "\n".join(f"  - {artifact_name}" for artifact_name in missing)
        raise ValueError(
            "Run artifact validation failed.\n"
            f"Missing required artifacts in {run_dir}:\n"
            f"{missing_lines}"
        )
