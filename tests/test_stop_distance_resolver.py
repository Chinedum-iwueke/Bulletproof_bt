from __future__ import annotations

from dataclasses import dataclass

import pytest

from bt.risk.stop_distance import resolve_stop_distance


@dataclass
class SignalStub:
    stop_price: float | None = None


@dataclass
class IndicatorStub:
    is_ready: bool
    value: float | None


def test_resolve_stop_distance_signal_stop_long() -> None:
    result = resolve_stop_distance(
        symbol="AAPL",
        side="long",
        entry_price=100.0,
        signal=SignalStub(stop_price=95.0),
        bars_by_symbol={},
        ctx={},
        config={},
    )

    assert result.stop_distance == 5.0
    assert result.source == "explicit_stop_price"


def test_resolve_stop_distance_signal_stop_invalid_side_long() -> None:
    with pytest.raises(ValueError, match=r"AAPL: invalid stop_price for long: stop=101\.0 entry=100\.0"):
        resolve_stop_distance(
            symbol="AAPL",
            side="long",
            entry_price=100.0,
            signal={"stop_price": 101.0},
            bars_by_symbol={},
            ctx={},
            config={},
        )


def test_resolve_stop_distance_atr_rule() -> None:
    config = {"risk": {"stop": {"mode": "atr", "atr_multiple": 2.0, "atr_indicator": "atr"}}}
    ctx = {"indicators": {"AAPL": {"atr": IndicatorStub(is_ready=True, value=1.5)}}}

    result = resolve_stop_distance(
        symbol="AAPL",
        side="short",
        entry_price=100.0,
        signal={},
        bars_by_symbol={},
        ctx=ctx,
        config=config,
    )

    assert result.stop_distance == 3.0
    assert result.source == "atr_multiple"


def test_resolve_stop_distance_atr_not_ready() -> None:
    config = {"risk": {"stop": {"mode": "atr", "atr_multiple": 2.0, "atr_indicator": "atr"}}}
    ctx = {"indicators": {"AAPL": {"atr": IndicatorStub(is_ready=False, value=None)}}}

    with pytest.raises(ValueError, match=r"ATR indicator 'atr' is not ready"):
        resolve_stop_distance(
            symbol="AAPL",
            side="long",
            entry_price=100.0,
            signal={},
            bars_by_symbol={},
            ctx=ctx,
            config=config,
        )


def test_resolve_stop_distance_missing_rules_actionable_error() -> None:
    with pytest.raises(
        ValueError,
        match=r"Provide signal\.stop_price or configure risk\.stop\.mode=atr",
    ):
        resolve_stop_distance(
            symbol="AAPL",
            side="long",
            entry_price=100.0,
            signal={},
            bars_by_symbol={},
            ctx={},
            config={},
        )
