from __future__ import annotations

import json
from pathlib import Path

from bt.experiments.status import detect_run_artifact_status
from bt.logging.run_contract import REQUIRED_ARTIFACTS


def test_strict_resume_does_not_skip_completed_run_with_missing_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")

    status = detect_run_artifact_status(run_dir, strict_resume=True)

    assert status.state == "INCOMPLETE"
    assert "missing artifacts" in status.message


def test_strict_resume_rejects_invalid_performance_metrics(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    for artifact in REQUIRED_ARTIFACTS:
        (run_dir / artifact).write_text("ok", encoding="utf-8")
    (run_dir / "performance.json").write_text(json.dumps({"metrics_valid": False}), encoding="utf-8")

    status = detect_run_artifact_status(run_dir, strict_resume=True)

    assert status.state == "INCOMPLETE"
    assert "metrics_valid=false" in status.message


def test_non_strict_resume_allows_legacy_completed_marker(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")

    assert detect_run_artifact_status(run_dir, strict_resume=False).state == "SUCCESS"
