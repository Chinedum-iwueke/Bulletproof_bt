from __future__ import annotations

from dataclasses import dataclass

import pytest

from bt.risk.contract import StopSpec
from bt.risk.reject_codes import RISK_FALLBACK_LEGACY_PROXY
from bt.risk.stop_distance import resolve_stop_distance
from bt.risk.stop_resolver import resolve_stop_from_spec


@dataclass
class BarStub:
    high: float
    low: float


@dataclass
class IndicatorStub:
    is_ready: bool
    value: float | None


def test_resolve_stop_from_spec_explicit_parity() -> None:
    bar = BarStub(high=105.0, low=95.0)
    spec = StopSpec(kind="explicit", stop_price=95.0)
    config: dict[str, dict[str, dict[str, float | str]]] = {}

    adapter_result = resolve_stop_from_spec(
        spec,
        symbol="AAPL",
        side="long",
        entry_price=100.0,
        bar=bar,
        ctx={},
        config=config,
    )
    truth = resolve_stop_distance(
        symbol="AAPL",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 95.0},
        bars_by_symbol={"AAPL": bar},
        ctx={},
        config=config,
    )

    assert adapter_result.stop_distance == pytest.approx(truth.stop_distance)
    assert adapter_result.stop_source == truth.source
    assert adapter_result.used_fallback is False
    assert adapter_result.reason_code == "resolved:explicit"


def test_resolve_stop_from_spec_atr_parity() -> None:
    bar = BarStub(high=105.0, low=95.0)
    spec = StopSpec(kind="atr", atr_multiple=2.0)
    ctx = {"indicators": {"AAPL": {"atr": IndicatorStub(is_ready=True, value=1.5)}}}

    adapter_result = resolve_stop_from_spec(
        spec,
        symbol="AAPL",
        side="short",
        entry_price=100.0,
        bar=bar,
        ctx=ctx,
        config={},
    )
    truth = resolve_stop_distance(
        symbol="AAPL",
        side="short",
        entry_price=100.0,
        signal={},
        bars_by_symbol={"AAPL": bar},
        ctx=ctx,
        config={"risk": {"stop": {"mode": "atr", "atr_multiple": 2.0}}},
    )

    assert adapter_result.stop_distance == pytest.approx(truth.stop_distance)
    assert adapter_result.stop_source == truth.source
    assert adapter_result.used_fallback is False
    assert adapter_result.reason_code == "resolved:atr"


def test_resolve_stop_from_spec_legacy_proxy_for_invalid_stopspec_raises() -> None:
    with pytest.raises(ValueError, match="missing required field 'stop_price'"):
        resolve_stop_from_spec(
            StopSpec(kind="explicit", stop_price=None),
            symbol="AAPL",
            side="long",
            entry_price=100.0,
            bar=BarStub(high=105.0, low=95.0),
            ctx={},
            config={"risk": {"stop": {"mode": "legacy_proxy"}}},
        )


@pytest.mark.parametrize("policy", ["wider", "tighter"])
def test_resolve_stop_from_spec_hybrid_not_implemented(policy: str) -> None:
    with pytest.raises(NotImplementedError, match="kind='hybrid'"):
        resolve_stop_from_spec(
            StopSpec(kind="hybrid", stop_price=95.0, atr_multiple=2.0, hybrid_policy=policy),
            symbol="AAPL",
            side="long",
            entry_price=100.0,
            bar=BarStub(high=105.0, low=95.0),
            ctx={"indicators": {"AAPL": {"atr": IndicatorStub(is_ready=True, value=1.0)}}},
            config={"risk": {"hybrid_policy": policy}},
        )


def test_fallback_reason_code_constant_is_stable() -> None:
    assert RISK_FALLBACK_LEGACY_PROXY == "risk_fallback:stop_legacy_proxy"
