from __future__ import annotations

import pytest

from bt.risk.spec import parse_risk_spec


def test_parse_risk_spec_valid_config() -> None:
    config = {
        "risk": {
            "mode": "r_fixed",
            "r_per_trade": 0.005,
            "min_stop_distance": 0.1,
            "max_leverage": 2.0,
        }
    }

    spec = parse_risk_spec(config)

    assert spec.mode == "r_fixed"
    assert spec.r_per_trade == 0.005
    assert spec.min_stop_distance == 0.1
    assert spec.max_leverage == 2.0
    assert spec.maintenance_free_margin_pct == 0.01


@pytest.mark.parametrize(
    "config",
    [
        {"risk": {"r_per_trade": 0.005}},
        {"risk": {"mode": "r_fixed"}},
    ],
)
def test_parse_risk_spec_missing_required_keys(config: dict[str, object]) -> None:
    with pytest.raises(ValueError, match=r"risk\.mode and risk\.r_per_trade are required"):
        parse_risk_spec(config)


@pytest.mark.parametrize(
    ("config", "expected_key", "expected_value"),
    [
        ({"risk": {"mode": "r_fixed", "r_per_trade": 0}}, "r_per_trade", "0"),
        ({"risk": {"mode": "r_fixed", "r_per_trade": -0.1}}, "r_per_trade", "-0.1"),
        ({"risk": {"mode": "bad_mode", "r_per_trade": 0.01}}, "mode", "bad_mode"),
        ({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "min_stop_distance": "nope"}}, "min_stop_distance", "nope"),
        ({"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "max_leverage": "x"}}, "max_leverage", "x"),
    ],
)
def test_parse_risk_spec_invalid_values_raise_actionable_messages(
    config: dict[str, object], expected_key: str, expected_value: str
) -> None:
    with pytest.raises(ValueError) as exc_info:
        parse_risk_spec(config)

    message = str(exc_info.value)
    assert f"risk.{expected_key}" in message
    assert expected_value in message


def test_parse_risk_spec_optional_defaults() -> None:
    config = {"risk": {"mode": "equity_pct", "r_per_trade": 0.01}}

    spec = parse_risk_spec(config)

    assert spec.min_stop_distance is None
    assert spec.max_leverage is None
    assert spec.maintenance_free_margin_pct == 0.01
