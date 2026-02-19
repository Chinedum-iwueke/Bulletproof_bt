from dataclasses import FrozenInstanceError

from bt.risk.contract import StopResolutionResult, StopSpec


def test_stop_spec_defaults_are_immutable_and_versioned() -> None:
    stop_spec = StopSpec(kind="explicit")

    assert stop_spec.contract_version == 1
    assert stop_spec.stop_price is None
    assert stop_spec.atr_multiple is None
    assert stop_spec.hybrid_policy is None
    assert stop_spec.raw_source is None
    assert stop_spec.details is None

    try:
        stop_spec.kind = "atr"
    except FrozenInstanceError:
        pass
    else:
        raise AssertionError("StopSpec should be frozen")


def test_stop_resolution_result_defaults_are_safe() -> None:
    result = StopResolutionResult(
        stop_price=101.5,
        stop_distance=2.5,
        stop_source="explicit",
        is_valid=True,
        used_fallback=False,
        reason_code="risk_reject:stop_missing",
    )

    assert result.details is None


def test_stop_spec_details_not_shared_mutable_default() -> None:
    without_details_a = StopSpec(kind="explicit")
    without_details_b = StopSpec(kind="explicit")

    assert without_details_a.details is None
    assert without_details_b.details is None

    details_a = {"source": "a"}
    details_b = {"source": "b"}
    with_details_a = StopSpec(kind="explicit", details=details_a)
    with_details_b = StopSpec(kind="explicit", details=details_b)

    details_a["updated"] = True

    assert with_details_a.details is details_a
    assert with_details_b.details is details_b
    assert with_details_a.details is not with_details_b.details
    assert "updated" not in with_details_b.details
