from __future__ import annotations

import pytest

from bt.core.config_resolver import resolve_config
from bt.risk.spec import parse_risk_spec


def test_resolve_config_keeps_canonical_r_per_trade_without_legacy_duplication() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.005}})

    assert resolved["risk"]["r_per_trade"] == 0.005
    assert "risk_per_trade_pct" not in resolved["risk"]
    assert "risk_per_trade_pct" not in resolved


def test_resolve_config_maps_legacy_risk_per_trade_pct_to_canonical_key() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "risk_per_trade_pct": 0.01}})

    assert resolved["risk"]["r_per_trade"] == 0.01
    # Legacy key is preserved only because the input explicitly provided it.
    assert resolved["risk"]["risk_per_trade_pct"] == 0.01


def test_parse_risk_spec_rejects_missing_risk_fraction() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed"}})

    with pytest.raises(ValueError, match=r"risk\.mode and risk\.r_per_trade are required"):
        parse_risk_spec(resolved)


def test_resolve_config_injects_default_stop_resolution() -> None:
    resolved = resolve_config({"risk": {"mode": "r_fixed", "r_per_trade": 0.01}})

    assert resolved["risk"]["stop_resolution"] == "strict"
