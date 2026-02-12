from __future__ import annotations

import pytest

from bt.core.config_resolver import ConfigError, resolve_config


def test_config_resolver_rejects_conflicting_top_level_and_risk_values() -> None:
    cfg = {
        "max_positions": 2,
        "risk": {
            "max_positions": 1,
        },
    }

    with pytest.raises(ConfigError, match="Conflicting config values"):
        resolve_config(cfg)
