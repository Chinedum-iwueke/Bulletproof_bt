from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from orchestrator import research_daemon
from orchestrator.interpret_experiment_results import _state_discovery_summary, resolve_input_roots
from orchestrator.state_discovery.dataset_loader import load_discovery_datasets


def _write_trades_csv(root: Path) -> None:
    run_dir = root / "runs" / "run_1"
    run_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "run_id": "run_1",
                "r_net": 0.5,
                "r_gross": 0.6,
                "entry_state": "trend",
            }
        ]
    ).to_csv(run_dir / "trades.csv", index=False)


def test_daemon_post_pipeline_commands_use_phase_roots(tmp_path) -> None:
    payload = {
        "hypothesis": "research/hypotheses/demo.yaml",
        "name": "demo",
        "phase": "tier3",
        "outputs_root": str(tmp_path / "outputs"),
    }
    config = {
        "default_llm_provider": "none",
        "include_state_discovery_in_verdict": True,
        "state_discovery_output_dir": str(tmp_path / "state_findings"),
    }

    interp = research_daemon.build_interpret_command(tmp_path / "research.sqlite", payload, config)
    state = research_daemon.build_state_discovery_command(tmp_path / "research.sqlite", payload, config, mode="combined")

    assert str(tmp_path / "outputs" / "tier3" / "demo_parallel_stable") in interp
    assert str(tmp_path / "outputs" / "tier3" / "demo_parallel_vol") in interp
    assert str(tmp_path / "outputs" / "tier3" / "demo_parallel_stable") in state
    assert str(tmp_path / "outputs" / "tier3" / "demo_parallel_vol") in state
    assert str(tmp_path / "state_findings" / "tier3") in state
    assert "--phase" in state and "tier3" in state
    assert "--phase" in interp and "tier3" in interp


def test_daemon_interpreter_uses_phase_verdict_dir(tmp_path) -> None:
    payload = {
        "hypothesis": "research/hypotheses/demo.yaml",
        "name": "demo",
        "phase": "tier3",
        "outputs_root": str(tmp_path / "outputs"),
    }
    config = {"verdict_output_dir": str(tmp_path / "verdicts")}

    interp = research_daemon.build_interpret_command(tmp_path / "research.sqlite", payload, config)

    assert str(tmp_path / "verdicts" / "tier3") in interp


def test_interpreter_state_discovery_lookup_prefers_phase_and_falls_back_to_flat(tmp_path) -> None:
    phase_dir = tmp_path / "state_findings" / "tier3"
    phase_dir.mkdir(parents=True)
    (phase_dir / "demo_combined_state_findings.json").write_text('{"findings":[]}', encoding="utf-8")
    flat_dir = tmp_path / "legacy_state_findings"
    flat_dir.mkdir()
    (flat_dir / "legacy_combined_state_findings.json").write_text('{"findings":[]}', encoding="utf-8")

    phase_summary = _state_discovery_summary(
        SimpleNamespace(
            state_discovery_json=None,
            state_discovery_md=None,
            state_discovery_dir=str(tmp_path / "state_findings"),
            name="demo",
            phase="tier3",
        )
    )
    legacy_summary = _state_discovery_summary(
        SimpleNamespace(
            state_discovery_json=None,
            state_discovery_md=None,
            state_discovery_dir=str(flat_dir),
            name="legacy",
            phase="tier3",
        )
    )

    assert phase_summary and str(phase_dir / "demo_combined_state_findings.json") in phase_summary["source_json_paths"]
    assert legacy_summary and str(flat_dir / "legacy_combined_state_findings.json") in legacy_summary["source_json_paths"]


def test_state_discovery_loads_tier_aware_experiment_root(tmp_path) -> None:
    root = tmp_path / "outputs" / "tier2" / "demo_parallel_stable"
    _write_trades_csv(root)

    datasets = load_discovery_datasets(
        db_path=tmp_path / "research.sqlite",
        experiment_root=root,
        dataset_type="stable",
    )

    assert len(datasets) == 1
    assert datasets[0].experiment_root == root
    assert datasets[0].dataset_type is None


def test_state_discovery_explicit_root_falls_back_to_legacy(tmp_path) -> None:
    legacy = tmp_path / "outputs" / "demo_parallel_stable"
    _write_trades_csv(legacy)

    datasets = load_discovery_datasets(
        db_path=tmp_path / "research.sqlite",
        experiment_root=tmp_path / "outputs" / "tier2" / "demo_parallel_stable",
        dataset_type="stable",
    )

    assert len(datasets) == 1
    assert datasets[0].experiment_root == legacy


def test_interpreter_input_roots_fall_back_to_legacy_flat_outputs(tmp_path) -> None:
    stable_legacy = tmp_path / "outputs" / "demo_parallel_stable"
    vol_legacy = tmp_path / "outputs" / "demo_parallel_vol"
    stable_legacy.mkdir(parents=True)
    vol_legacy.mkdir(parents=True)

    stable_root, vol_root = resolve_input_roots(
        tmp_path / "outputs" / "tier2" / "demo_parallel_stable",
        tmp_path / "outputs" / "tier2" / "demo_parallel_vol",
    )

    assert stable_root == stable_legacy
    assert vol_root == vol_legacy
