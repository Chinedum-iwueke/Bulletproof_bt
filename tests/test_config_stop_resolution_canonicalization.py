from __future__ import annotations

import pytest

from bt.core.config_resolver import resolve_config


def test_defaults_inject_safe_and_disallow_legacy_proxy() -> None:
    resolved = resolve_config({})

    assert resolved["risk"]["stop_resolution"] == "safe"
    assert resolved["risk"]["allow_legacy_proxy"] is False


def test_strict_with_allow_legacy_proxy_true_raises() -> None:
    with pytest.raises(ValueError, match=r"risk\.allow_legacy_proxy.*risk\.stop_resolution=strict"):
        resolve_config({"risk": {"stop_resolution": "strict", "allow_legacy_proxy": True}})


def test_legacy_allow_legacy_proxy_alias_normalizes() -> None:
    cfg = {"risk": {"stop_resolution": "allow_legacy_proxy"}}

    resolved_first = resolve_config(cfg)
    resolved_second = resolve_config(cfg)

    assert resolved_first["risk"]["stop_resolution"] == "safe"
    assert resolved_first["risk"]["allow_legacy_proxy"] is True
    assert resolved_first == resolved_second
    assert "warnings" in resolved_first
    assert any(
        "risk.stop_resolution=allow_legacy_proxy is deprecated" in warning
        and "use risk.stop_resolution=safe and risk.allow_legacy_proxy=true" in warning
        for warning in resolved_first["warnings"]
    )


def test_invalid_stop_resolution_value_raises() -> None:
    with pytest.raises(ValueError, match=r"risk\.stop_resolution") as excinfo:
        resolve_config({"risk": {"stop_resolution": "banana"}})

    message = str(excinfo.value)
    assert "safe" in message
    assert "strict" in message


def test_invalid_allow_legacy_proxy_type_raises() -> None:
    with pytest.raises(ValueError, match=r"risk\.allow_legacy_proxy") as excinfo:
        resolve_config({"risk": {"stop_resolution": "safe", "allow_legacy_proxy": "yes"}})

    assert "boolean" in str(excinfo.value)
