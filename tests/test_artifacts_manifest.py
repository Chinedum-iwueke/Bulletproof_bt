from __future__ import annotations

import json
from pathlib import Path

from bt.logging.run_manifest import ARTIFACTS_MANIFEST_VERSION, write_artifacts_manifest


REQUIRED_NAMES = {
    "config_used.yaml",
    "performance.json",
    "equity.csv",
    "trades.csv",
    "fills.jsonl",
    "decisions.jsonl",
    "performance_by_bucket.csv",
    "summary.txt",
    "run_status.json",
    "data_scope.json",
    "benchmark_equity.csv",
    "benchmark_metrics.json",
    "comparison_summary.json",
}


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_map(payload: dict) -> dict[str, dict]:
    artifacts = payload["artifacts"]
    return {entry["name"]: entry for entry in artifacts}


def test_artifacts_manifest_written_and_lists_required(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_001"
    run_dir.mkdir()
    for name in [
        "config_used.yaml",
        "performance.json",
        "equity.csv",
        "trades.csv",
        "fills.jsonl",
        "decisions.jsonl",
        "performance_by_bucket.csv",
        "run_status.json",
        "summary.txt",
    ]:
        (run_dir / name).write_text("x\n", encoding="utf-8")

    manifest_path = write_artifacts_manifest(run_dir, config={"benchmark": {"enabled": False}})

    assert manifest_path.exists()
    payload = _read(manifest_path)
    assert payload["schema_version"] == ARTIFACTS_MANIFEST_VERSION

    names = [entry["name"] for entry in payload["artifacts"]]
    assert REQUIRED_NAMES.issubset(set(names))
    assert names == sorted(names)


def test_manifest_marks_produced_flags_correctly(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_002"
    run_dir.mkdir()
    for name in ["config_used.yaml", "performance.json", "trades.csv"]:
        (run_dir / name).write_text("x\n", encoding="utf-8")

    payload = _read(write_artifacts_manifest(run_dir, config={}))
    artifacts = _artifact_map(payload)

    assert artifacts["config_used.yaml"]["produced"] is True
    assert artifacts["performance.json"]["produced"] is True
    assert artifacts["trades.csv"]["produced"] is True
    assert artifacts["equity.csv"]["produced"] is False
    assert artifacts["decisions.jsonl"]["produced"] is False


def test_benchmark_conditionals_reflected(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_003"
    run_dir.mkdir()

    payload = _read(write_artifacts_manifest(run_dir, config={"benchmark": {"enabled": True}}))
    artifacts = _artifact_map(payload)

    assert payload["benchmark_enabled"] is True
    for name in ["benchmark_equity.csv", "benchmark_metrics.json", "comparison_summary.json"]:
        assert artifacts[name]["conditional_on"] == "benchmark.enabled"


def test_repeat_write_deterministic_except_timestamp(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_004"
    run_dir.mkdir()
    (run_dir / "config_used.yaml").write_text("x\n", encoding="utf-8")

    first = _read(write_artifacts_manifest(run_dir, config={"benchmark": {"enabled": False}}))
    second = _read(write_artifacts_manifest(run_dir, config={"benchmark": {"enabled": False}}))

    first.pop("created_utc")
    second.pop("created_utc")
    assert first == second
