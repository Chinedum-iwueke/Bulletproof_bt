from __future__ import annotations

import json

from bt.config import deep_merge, load_yaml
from bt.core.config_resolver import resolve_config
from bt.validation.config_completeness import validate_resolved_config_completeness


def _resolve_with_runner_stack(*overrides: str) -> dict:
    config = deep_merge(load_yaml("configs/engine.yaml"), load_yaml("configs/fees.yaml"))
    config = deep_merge(config, load_yaml("configs/slippage.yaml"))
    for path in overrides:
        config = deep_merge(config, load_yaml(path))
    resolved = resolve_config(config)
    validate_resolved_config_completeness(resolved)
    return resolved


def _canonical_dump(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def test_engine_yaml_contains_all_supported_keys_and_resolves() -> None:
    resolved = _resolve_with_runner_stack()

    assert "risk" in resolved
    assert resolved["risk"]["stop_resolution"] == "safe"
    assert resolved["risk"]["allow_legacy_proxy"] is False
    assert isinstance(resolved["risk"]["stop_resolution"], str)
    assert isinstance(resolved["risk"]["allow_legacy_proxy"], bool)


def test_safe_client_pack_resolves_and_enables_explicit_fallback() -> None:
    resolved = _resolve_with_runner_stack("configs/examples/safe_client.yaml")

    assert resolved["risk"]["stop_resolution"] == "safe"
    assert resolved["risk"]["allow_legacy_proxy"] is True


def test_strict_research_pack_resolves_and_disables_fallback() -> None:
    resolved = _resolve_with_runner_stack("configs/examples/strict_research.yaml")

    assert resolved["risk"]["stop_resolution"] == "strict"
    assert resolved["risk"]["allow_legacy_proxy"] is False


def test_configs_resolve_deterministically() -> None:
    packs = [
        (),
        ("configs/examples/safe_client.yaml",),
        ("configs/examples/strict_research.yaml",),
    ]

    for pack in packs:
        first = _resolve_with_runner_stack(*pack)
        second = _resolve_with_runner_stack(*pack)
        assert _canonical_dump(first) == _canonical_dump(second)
