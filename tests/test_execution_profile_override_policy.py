from __future__ import annotations

import pytest

from bt.execution.profile import resolve_execution_profile


def test_tier_profile_forbids_overrides() -> None:
    with pytest.raises(ValueError) as exc_info:
        resolve_execution_profile({"execution": {"profile": "tier2", "taker_fee": 0.01}})

    message = str(exc_info.value)
    assert "execution.profile" in message
    assert "custom" in message
    assert "taker_fee" in message


def test_default_profile_tier2_forbids_overrides() -> None:
    with pytest.raises(ValueError, match="execution.profile=tier2"):
        resolve_execution_profile({"execution": {"taker_fee": 0.01}})


def test_custom_allows_overrides_when_complete() -> None:
    profile = resolve_execution_profile(
        {
            "execution": {
                "profile": "custom",
                "maker_fee": 0.0,
                "taker_fee": 0.001,
                "slippage_bps": 2.0,
                "delay_bars": 1,
                "spread_bps": 1.0,
            }
        }
    )

    assert profile.name == "custom"
    assert profile.maker_fee == 0.0
    assert profile.taker_fee == 0.001
    assert profile.slippage_bps == 2.0
    assert profile.delay_bars == 1
    assert profile.spread_bps == 1.0


def test_custom_missing_field_raises() -> None:
    with pytest.raises(ValueError) as exc_info:
        resolve_execution_profile(
            {
                "execution": {
                    "profile": "custom",
                    "maker_fee": 0.0,
                    "taker_fee": 0.001,
                    "slippage_bps": 2.0,
                    "delay_bars": 1,
                }
            }
        )

    assert "execution.spread_bps" in str(exc_info.value)


def test_custom_negative_values_raise() -> None:
    with pytest.raises(ValueError) as slippage_exc_info:
        resolve_execution_profile(
            {
                "execution": {
                    "profile": "custom",
                    "maker_fee": 0.0,
                    "taker_fee": 0.001,
                    "slippage_bps": -1.0,
                    "delay_bars": 1,
                    "spread_bps": 1.0,
                }
            }
        )
    assert "execution.slippage_bps" in str(slippage_exc_info.value)

    with pytest.raises(ValueError) as delay_exc_info:
        resolve_execution_profile(
            {
                "execution": {
                    "profile": "custom",
                    "maker_fee": 0.0,
                    "taker_fee": 0.001,
                    "slippage_bps": 2.0,
                    "delay_bars": -1,
                    "spread_bps": 1.0,
                }
            }
        )
    assert "execution.delay_bars" in str(delay_exc_info.value)
