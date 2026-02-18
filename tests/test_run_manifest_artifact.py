from __future__ import annotations

import json
from pathlib import Path

from bt.logging.run_manifest import write_run_manifest


def _read_manifest(manifest_path: Path) -> dict:
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def test_write_manifest_schema_keys_exist(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_a"
    run_dir.mkdir()
    (run_dir / "config_used.yaml").write_text("x: 1\n", encoding="utf-8")
    (run_dir / "metrics.json").write_text("{}\n", encoding="utf-8")

    manifest_path = write_run_manifest(
        run_dir,
        config={"benchmark": {"enabled": False}},
        data_path=str(tmp_path / "dataset"),
    )

    assert manifest_path.exists()
    payload = _read_manifest(manifest_path)

    required_keys = {
        "schema_version",
        "run_dir",
        "created_at_utc",
        "data_path",
        "is_dataset_dir",
        "benchmark_enabled",
        "artifact_files",
        "config_summary",
    }
    assert required_keys.issubset(payload.keys())
    assert payload["schema_version"] == 1


def test_manifest_artifact_files_sorted(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_b"
    run_dir.mkdir()

    for name in ["zeta.txt", "alpha.txt", "mid.json"]:
        (run_dir / name).write_text("x\n", encoding="utf-8")

    manifest_path = write_run_manifest(run_dir, config={}, data_path=str(tmp_path))
    payload = _read_manifest(manifest_path)

    expected = sorted(["zeta.txt", "alpha.txt", "mid.json", "run_manifest.json"])
    assert payload["artifact_files"] == expected


def test_manifest_config_summary_extraction(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_c"
    run_dir.mkdir()

    config = {
        "strategy": {"name": "volfloor_donchian"},
        "data": {"mode": "streaming"},
        "risk": {"mode": "equity_pct"},
        "execution": {"profile": "tier2", "intrabar_mode": "worst_case"},
    }

    manifest_path = write_run_manifest(run_dir, config=config, data_path=str(tmp_path))
    payload = _read_manifest(manifest_path)

    assert payload["config_summary"] == {
        "strategy": "volfloor_donchian",
        "data_mode": "streaming",
        "risk_mode": "equity_pct",
        "execution_profile": "tier2",
        "intrabar_mode": "worst_case",
    }


def test_manifest_deterministic_ignoring_created_at_utc(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_d"
    run_dir.mkdir()
    (run_dir / "a.txt").write_text("x\n", encoding="utf-8")

    config = {"execution": {"intrabar_mode": "midpoint"}, "benchmark": {"enabled": True}}

    first_path = write_run_manifest(run_dir, config=config, data_path=str(tmp_path))
    first_payload = _read_manifest(first_path)

    second_path = write_run_manifest(run_dir, config=config, data_path=str(tmp_path))
    second_payload = _read_manifest(second_path)

    first_payload.pop("created_at_utc")
    second_payload.pop("created_at_utc")

    assert first_payload == second_payload
