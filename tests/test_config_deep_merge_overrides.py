from __future__ import annotations

from bt.config import deep_merge, load_config_with_overrides


def test_deep_merge_recursion() -> None:
    base = {"risk": {"max_positions": 2, "nested": {"a": 1, "b": 2}}}
    override = {"risk": {"nested": {"b": 9, "c": 3}}}

    merged = deep_merge(base, override)

    assert merged["risk"]["max_positions"] == 2
    assert merged["risk"]["nested"] == {"a": 1, "b": 9, "c": 3}


def test_deep_merge_replaces_lists() -> None:
    base = {"symbols": ["AAA", "BBB"], "risk": {"tiers": [1, 2]}}
    override = {"symbols": ["CCC"], "risk": {"tiers": [9]}}

    merged = deep_merge(base, override)

    assert merged["symbols"] == ["CCC"]
    assert merged["risk"]["tiers"] == [9]


def test_override_precedence_order(tmp_path) -> None:
    base = tmp_path / "base.yaml"
    o1 = tmp_path / "o1.yaml"
    o2 = tmp_path / "o2.yaml"

    base.write_text("risk:\n  max_positions: 1\nstrategy:\n  p_trade: 0.1\n", encoding="utf-8")
    o1.write_text("risk:\n  max_positions: 2\n", encoding="utf-8")
    o2.write_text("risk:\n  max_positions: 3\n", encoding="utf-8")

    merged = load_config_with_overrides(str(base), [str(o1), str(o2)])

    assert merged["risk"]["max_positions"] == 3
