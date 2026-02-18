from __future__ import annotations

import pytest

from bt.execution.profile import get_builtin_profile, resolve_execution_profile


def test_default_profile_is_tier2() -> None:
    profile = resolve_execution_profile({})
    assert profile.name == "tier2"
    assert profile.maker_fee == 0.0
    assert profile.taker_fee == 0.0006
    assert profile.slippage_bps == 2.0
    assert profile.delay_bars == 1
    assert profile.spread_bps == 1.0


def test_builtin_profile_values_exact() -> None:
    tier1 = get_builtin_profile("tier1")
    assert (tier1.maker_fee, tier1.taker_fee, tier1.slippage_bps, tier1.delay_bars, tier1.spread_bps) == (
        0.0,
        0.0004,
        0.5,
        0,
        0.0,
    )

    tier2 = get_builtin_profile("tier2")
    assert (tier2.maker_fee, tier2.taker_fee, tier2.slippage_bps, tier2.delay_bars, tier2.spread_bps) == (
        0.0,
        0.0006,
        2.0,
        1,
        1.0,
    )

    tier3 = get_builtin_profile("tier3")
    assert (tier3.maker_fee, tier3.taker_fee, tier3.slippage_bps, tier3.delay_bars, tier3.spread_bps) == (
        0.0,
        0.0008,
        5.0,
        1,
        3.0,
    )


def test_profile_override_conflict_raises() -> None:
    with pytest.raises(ValueError, match="taker_fee") as exc_info:
        resolve_execution_profile({"execution": {"profile": "tier2", "taker_fee": 0.01}})
    assert "profile" in str(exc_info.value)


def test_custom_requires_all_fields() -> None:
    with pytest.raises(ValueError, match="execution.spread_bps"):
        resolve_execution_profile(
            {
                "execution": {
                    "profile": "custom",
                    "maker_fee": 0.0,
                    "taker_fee": 0.001,
                    "slippage_bps": 2.5,
                    "delay_bars": 1,
                }
            }
        )


def test_invalid_ranges_raise() -> None:
    with pytest.raises(ValueError, match="execution.slippage_bps"):
        resolve_execution_profile(
            {
                "execution": {
                    "profile": "custom",
                    "maker_fee": 0.0,
                    "taker_fee": 0.001,
                    "slippage_bps": -1.0,
                    "delay_bars": 1,
                    "spread_bps": 0.0,
                }
            }
        )

    with pytest.raises(ValueError, match="execution.delay_bars"):
        resolve_execution_profile(
            {
                "execution": {
                    "profile": "custom",
                    "maker_fee": 0.0,
                    "taker_fee": 0.001,
                    "slippage_bps": 1.0,
                    "delay_bars": -1,
                    "spread_bps": 0.0,
                }
            }
        )
