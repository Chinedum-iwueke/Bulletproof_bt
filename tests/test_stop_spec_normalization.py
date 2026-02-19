import pandas as pd
import pytest

from bt.core.types import Signal
from bt.risk.stop_spec import normalize_stop_spec


def _signal(*, metadata: dict | None = None) -> Signal:
    return Signal(
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="BTC-USD",
        side=None,
        signal_type="entry",
        confidence=0.9,
        metadata=metadata or {},
    )


def test_returns_none_when_no_stop_fields_present() -> None:
    parsed = normalize_stop_spec(_signal(metadata={}), config={})
    assert parsed is None


def test_parses_legacy_signal_stop_price_as_explicit() -> None:
    signal = _signal(metadata={"stop_price": 95.25})

    parsed = normalize_stop_spec(signal, config={})

    assert parsed is not None
    assert parsed.kind == "explicit"
    assert parsed.stop_price == 95.25
    assert parsed.contract_version == 1
    assert parsed.raw_source == "signal.metadata.stop_price"


def test_parses_stop_spec_structural() -> None:
    signal = _signal(metadata={"stop_spec": {"kind": "structural", "stop_price": 123.4}})

    parsed = normalize_stop_spec(signal, config={})

    assert parsed is not None
    assert parsed.kind == "structural"
    assert parsed.stop_price == 123.4
    assert parsed.atr_multiple is None


def test_parses_stop_spec_atr() -> None:
    signal = _signal(metadata={"stop_spec": {"kind": "atr", "atr_multiple": 2.5}})

    parsed = normalize_stop_spec(signal, config={})

    assert parsed is not None
    assert parsed.kind == "atr"
    assert parsed.atr_multiple == 2.5
    assert parsed.stop_price is None


def test_parses_stop_spec_hybrid_with_optional_policy() -> None:
    signal = _signal(
        metadata={
            "stop_spec": {
                "kind": "hybrid",
                "stop_price": 100.0,
                "atr_multiple": 2.0,
                "hybrid_policy": "wider",
            }
        }
    )

    parsed = normalize_stop_spec(signal, config={})

    assert parsed is not None
    assert parsed.kind == "hybrid"
    assert parsed.hybrid_policy == "wider"


def test_precedence_stop_spec_beats_legacy_stop_price() -> None:
    signal = _signal(
        metadata={
            "stop_spec": {"kind": "atr", "atr_multiple": 3.0},
            "stop_price": 99.0,
        }
    )

    parsed = normalize_stop_spec(signal, config={})

    assert parsed is not None
    assert parsed.kind == "atr"
    assert parsed.atr_multiple == 3.0


def test_invalid_stop_spec_kind_raises_actionable_valueerror() -> None:
    signal = _signal(metadata={"stop_spec": {"kind": "banana"}})

    with pytest.raises(ValueError, match="signal.metadata.stop_spec.kind") as exc_info:
        normalize_stop_spec(signal, config={})

    assert "['atr', 'explicit', 'hybrid', 'structural']" in str(exc_info.value)


def test_invalid_contract_version_raises() -> None:
    signal = _signal(
        metadata={
            "stop_spec": {"kind": "atr", "atr_multiple": 2.0, "contract_version": 999}
        }
    )

    with pytest.raises(ValueError, match="contract_version") as exc_info:
        normalize_stop_spec(signal, config={})

    assert "unsupported" in str(exc_info.value).lower()


def test_invalid_stop_price_type_raises() -> None:
    signal = _signal(metadata={"stop_spec": {"kind": "explicit", "stop_price": "abc"}})

    with pytest.raises(ValueError, match="signal.metadata.stop_spec.stop_price") as exc_info:
        normalize_stop_spec(signal, config={})

    assert "'abc'" in str(exc_info.value)


def test_invalid_risk_hybrid_policy_in_config_raises() -> None:
    signal = _signal(
        metadata={
            "stop_spec": {"kind": "hybrid", "stop_price": 101.0, "atr_multiple": 2.0}
        }
    )

    with pytest.raises(ValueError, match="config.risk.hybrid_policy"):
        normalize_stop_spec(signal, config={"risk": {"hybrid_policy": "sideways"}})


def test_normalization_is_deterministic_for_same_inputs() -> None:
    signal = _signal(metadata={"stop_spec": {"kind": "structural", "stop_price": 111.0}})

    first = normalize_stop_spec(signal, config={})
    second = normalize_stop_spec(signal, config={})

    assert first == second
