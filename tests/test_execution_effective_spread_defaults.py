from __future__ import annotations

import pytest

from bt.execution.effective import build_effective_execution_snapshot, resolve_spread_settings
from bt.execution.profile import resolve_execution_profile


def test_resolve_spread_defaults_to_fixed_bps_for_tier2() -> None:
    cfg: dict[str, object] = {"execution": {"profile": "tier2"}}
    profile = resolve_execution_profile(cfg)
    mode, spread_bps = resolve_spread_settings(cfg, profile=profile)
    assert mode == "fixed_bps"
    assert spread_bps == pytest.approx(1.0)


def test_resolve_spread_defaults_to_none_for_tier1_zero_spread() -> None:
    cfg: dict[str, object] = {"execution": {"profile": "tier1"}}
    profile = resolve_execution_profile(cfg)
    mode, spread_bps = resolve_spread_settings(cfg, profile=profile)
    assert mode == "none"
    assert spread_bps is None


def test_explicit_spread_mode_none_overrides_profile_default() -> None:
    cfg: dict[str, object] = {"execution": {"profile": "tier3", "spread_mode": "none"}}
    profile = resolve_execution_profile(cfg)
    mode, spread_bps = resolve_spread_settings(cfg, profile=profile)
    assert mode == "none"
    assert spread_bps is None


def test_effective_snapshot_for_tier3_defaults_to_fixed_bps() -> None:
    payload = build_effective_execution_snapshot({"execution": {"profile": "tier3"}})
    assert payload["spread_mode"] == "fixed_bps"
    assert payload["spread_bps"] == pytest.approx(3.0)
