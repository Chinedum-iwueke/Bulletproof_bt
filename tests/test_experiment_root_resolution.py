from __future__ import annotations

from bt.paths import (
    discover_experiment_roots,
    resolve_daemon_command_log_dir,
    resolve_experiment_root,
    resolve_existing_experiment_root,
    resolve_existing_experiment_root_from_path,
)


def test_tier2_stable_root_resolution() -> None:
    assert (
        resolve_experiment_root(
            outputs_root="outputs",
            phase="tier2",
            experiment_name="l1_h1_vol_floor_trend",
            variant="stable",
        ).as_posix()
        == "outputs/tier2/l1_h1_vol_floor_trend_parallel_stable"
    )


def test_tier3_vol_root_resolution() -> None:
    assert (
        resolve_experiment_root(
            outputs_root="outputs",
            phase="tier3",
            experiment_name="l1_h1_vol_floor_trend",
            variant="vol",
        ).as_posix()
        == "outputs/tier3/l1_h1_vol_floor_trend_parallel_vol"
    )


def test_read_only_resolution_falls_back_to_legacy_flat_output(tmp_path) -> None:
    legacy = tmp_path / "outputs" / "demo_parallel_stable"
    legacy.mkdir(parents=True)

    resolved = resolve_existing_experiment_root(
        outputs_root=tmp_path / "outputs",
        phase="tier2",
        experiment_name="demo",
        variant="stable",
    )

    assert resolved == legacy
    assert resolve_existing_experiment_root_from_path(tmp_path / "outputs" / "tier2" / "demo_parallel_stable") == legacy


def test_dashboard_style_discovery_includes_nested_and_legacy_outputs(tmp_path) -> None:
    legacy = tmp_path / "outputs" / "legacy_parallel_stable" / "runs"
    nested = tmp_path / "outputs" / "tier3" / "demo_parallel_vol" / "runs"
    legacy.mkdir(parents=True)
    nested.mkdir(parents=True)

    roots = discover_experiment_roots(tmp_path / "outputs")

    assert tmp_path / "outputs" / "legacy_parallel_stable" in roots
    assert tmp_path / "outputs" / "tier3" / "demo_parallel_vol" in roots


def test_daemon_command_logs_are_phase_scoped() -> None:
    assert (
        resolve_daemon_command_log_dir(outputs_root="outputs", phase="tier3", experiment_name="demo").as_posix()
        == "outputs/tier3/demo_daemon_command_logs"
    )
