from pathlib import Path

import yaml


CONFIG_FILES = [
    Path("configs/engine.yaml"),
    Path("configs/experiments/h1_volfloor_donchian.yaml"),
]

LEGACY_KEYS = {"max_positions", "risk_per_trade_pct"}


def _find_legacy_keys(data: dict) -> list[str]:
    return sorted(key for key in LEGACY_KEYS if key in (data or {}))


def test_no_legacy_risk_keys_in_config() -> None:
    for path in CONFIG_FILES:
        data = yaml.safe_load(path.read_text()) or {}
        root_legacy = _find_legacy_keys(data)
        fixed_legacy = _find_legacy_keys((data.get("fixed") or {}))

        assert not root_legacy and not fixed_legacy, (
            f"Legacy risk keys in {path}: root={root_legacy}, fixed={fixed_legacy}"
        )
