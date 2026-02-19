from __future__ import annotations

import json
from pathlib import Path

from bt.logging.artifacts_manifest import ARTIFACTS_MANIFEST_SCHEMA_VERSION, write_artifacts_manifest


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_map(payload: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in payload["artifacts"]}


def test_artifacts_manifest_created_and_has_required_entries(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_001"
    run_dir.mkdir()

    for name in ["config_used.yaml", "performance.json", "equity.csv"]:
        (run_dir / name).write_text("x\n", encoding="utf-8")

    manifest_path = write_artifacts_manifest(run_dir, config={"benchmark": {"enabled": False}})

    assert manifest_path.exists()
    payload = _read(manifest_path)
    assert payload["schema_version"] == ARTIFACTS_MANIFEST_SCHEMA_VERSION

    artifacts = payload["artifacts"]
    names = [entry["name"] for entry in artifacts]
    assert names == sorted(names)

    artifact_map = _artifact_map(payload)
    assert "config_used.yaml" in artifact_map
    assert "performance.json" in artifact_map
    assert artifact_map["config_used.yaml"]["present"] is True
    assert artifact_map["performance.json"]["present"] is True
    assert artifact_map["trades.csv"]["present"] is False


def test_artifacts_manifest_benchmark_conditionals(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_002"
    run_dir.mkdir()
    for name in ["benchmark_equity.csv", "benchmark_metrics.json", "comparison_summary.json"]:
        (run_dir / name).write_text("x\n", encoding="utf-8")

    payload = _read(write_artifacts_manifest(run_dir, config={"benchmark": {"enabled": True}}))
    artifacts = _artifact_map(payload)

    assert payload["benchmark_enabled"] is True
    assert artifacts["benchmark_equity.csv"]["conditional_on"] == "benchmark.enabled"
    assert artifacts["benchmark_metrics.json"]["conditional_on"] == "benchmark.enabled"
    assert artifacts["comparison_summary.json"]["conditional_on"] == "benchmark.enabled"
    assert artifacts["benchmark_equity.csv"]["present"] is True
    assert artifacts["benchmark_metrics.json"]["present"] is True
    assert artifacts["comparison_summary.json"]["present"] is True


def test_artifacts_manifest_data_scope_flag(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_003"
    run_dir.mkdir()
    (run_dir / "data_scope.json").write_text("{}\n", encoding="utf-8")

    payload = _read(write_artifacts_manifest(run_dir, config={}))
    artifacts = _artifact_map(payload)

    assert payload["data_scope_active"] is True
    assert artifacts["data_scope.json"]["conditional_on"] == "data.scope_knobs_active"
    assert artifacts["data_scope.json"]["present"] is True
