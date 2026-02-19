from __future__ import annotations

import pytest

from bt.core.config_resolver import resolve_config
from bt.execution.profile import resolve_execution_profile


def test_tier2_forbids_overrides() -> None:
    with pytest.raises(ValueError) as exc_info:
        resolve_execution_profile({"execution": {"profile": "tier2", "spread_bps": 1.0}})

    message = str(exc_info.value)
    assert "execution.profile" in message
    assert "spread_bps" in message


def test_custom_allows_overrides() -> None:
    profile = resolve_execution_profile(
        {
            "execution": {
                "profile": "custom",
                "maker_fee": 0.0,
                "taker_fee": 0.0006,
                "slippage_bps": 2.0,
                "delay_bars": 1,
                "spread_bps": 1.0,
            }
        }
    )

    assert profile.name == "custom"


def test_missing_profile_defaults_to_tier2() -> None:
    resolved = resolve_config({"execution": {}})
    assert resolved["execution"]["profile"] == "tier2"


def test_legacy_keys_without_profile_raise() -> None:
    with pytest.raises(ValueError, match="execution.profile=custom"):
        resolve_execution_profile({"signal_delay_bars": 1})


def test_preset_without_overrides_passes() -> None:
    profile = resolve_execution_profile({"execution": {"profile": "tier2"}})
    assert profile.name == "tier2"
