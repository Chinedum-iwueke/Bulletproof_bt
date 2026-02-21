from __future__ import annotations

import pytest

from bt.api import _apply_data_timeframe_override


def test_data_timeframe_override_rewrites_htf_resampler_timeframes() -> None:
    config = {
        "data": {"timeframe": "15m"},
        "htf_resampler": {"timeframes": ["5m", "1h"], "strict": False},
    }

    _apply_data_timeframe_override(config)

    assert config["htf_resampler"]["timeframes"] == ["15m"]
    assert config["htf_resampler"]["strict"] is False


def test_data_timeframe_override_creates_htf_resampler_when_missing() -> None:
    config = {"data": {"timeframe": "1h"}}

    _apply_data_timeframe_override(config)

    assert config["htf_resampler"] == {"timeframes": ["1h"], "strict": True}


def test_data_timeframe_override_validates_value() -> None:
    config = {"data": {"timeframe": "banana"}}

    with pytest.raises(ValueError, match=r"data\.timeframe"):
        _apply_data_timeframe_override(config)
