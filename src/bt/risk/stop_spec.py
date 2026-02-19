from __future__ import annotations

import math
from typing import Any, Literal

from bt.core.types import Signal
from bt.risk.contract import StopSpec

_ALLOWED_KINDS = {"explicit", "structural", "atr", "hybrid"}
_ALLOWED_HYBRID_POLICIES = {"wider", "tighter"}


def _validation_error(path: str, expected: str, fix_snippet: str, value: Any) -> ValueError:
    return ValueError(
        f"Invalid {path}: expected {expected}, got {value!r}. "
        f"Example fix:\n{fix_snippet}"
    )


def _coerce_positive_finite_float(*, value: Any, path: str, strict_positive: bool = True) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _validation_error(
            path,
            "a finite numeric value",
            f"{path}: 123.45",
            value,
        )

    normalized = float(value)
    if not math.isfinite(normalized):
        raise _validation_error(
            path,
            "a finite numeric value",
            f"{path}: 123.45",
            value,
        )

    if strict_positive and normalized <= 0:
        raise _validation_error(
            path,
            "> 0",
            f"{path}: 123.45",
            value,
        )

    return normalized


def _normalize_explicit_stop(*, stop_price: Any, path: str, raw_source: str) -> StopSpec:
    normalized_price = _coerce_positive_finite_float(value=stop_price, path=path)
    return StopSpec(
        kind="explicit",
        stop_price=normalized_price,
        contract_version=1,
        raw_source=raw_source,
    )


def _normalize_structured_stop_spec(stop_spec_payload: Any) -> StopSpec:
    path = "signal.metadata.stop_spec"
    if not isinstance(stop_spec_payload, dict):
        raise _validation_error(
            path,
            "a mapping/dict",
            'signal:\n  metadata:\n    stop_spec:\n      kind: "atr"\n      atr_multiple: 2.0',
            stop_spec_payload,
        )

    contract_version = stop_spec_payload.get("contract_version", 1)
    if not isinstance(contract_version, int) or contract_version != 1:
        raise ValueError(
            "Unsupported contract_version at signal.metadata.stop_spec.contract_version: "
            f"got {contract_version!r}, expected 1. Example fix:\n"
            'signal:\n  metadata:\n    stop_spec:\n      contract_version: 1\n      kind: "atr"\n      atr_multiple: 2.0'
        )

    kind = stop_spec_payload.get("kind")
    if kind not in _ALLOWED_KINDS:
        raise _validation_error(
            "signal.metadata.stop_spec.kind",
            f"one of {sorted(_ALLOWED_KINDS)}",
            'signal:\n  metadata:\n    stop_spec:\n      kind: "structural"\n      stop_price: 123.4',
            kind,
        )

    details = {k: v for k, v in stop_spec_payload.items() if k not in {"kind", "contract_version", "stop_price", "atr_multiple", "hybrid_policy"}}

    if kind == "explicit":
        stop_price = stop_spec_payload.get("stop_price")
        return _normalize_explicit_stop(
            stop_price=stop_price,
            path="signal.metadata.stop_spec.stop_price",
            raw_source="signal.metadata.stop_spec",
        )

    if kind == "structural":
        stop_price = _coerce_positive_finite_float(
            value=stop_spec_payload.get("stop_price"),
            path="signal.metadata.stop_spec.stop_price",
        )
        return StopSpec(
            kind="structural",
            stop_price=stop_price,
            contract_version=1,
            raw_source="signal.metadata.stop_spec",
            details=details or None,
        )

    if kind == "atr":
        atr_multiple = _coerce_positive_finite_float(
            value=stop_spec_payload.get("atr_multiple"),
            path="signal.metadata.stop_spec.atr_multiple",
        )
        return StopSpec(
            kind="atr",
            atr_multiple=atr_multiple,
            contract_version=1,
            raw_source="signal.metadata.stop_spec",
            details=details or None,
        )

    hybrid_policy = stop_spec_payload.get("hybrid_policy")
    if hybrid_policy is not None and hybrid_policy not in _ALLOWED_HYBRID_POLICIES:
        raise _validation_error(
            "signal.metadata.stop_spec.hybrid_policy",
            f"one of {sorted(_ALLOWED_HYBRID_POLICIES)}",
            'signal:\n  metadata:\n    stop_spec:\n      kind: "hybrid"\n      stop_price: 100.0\n      atr_multiple: 2.0\n      hybrid_policy: "wider"',
            hybrid_policy,
        )

    stop_price = _coerce_positive_finite_float(
        value=stop_spec_payload.get("stop_price"),
        path="signal.metadata.stop_spec.stop_price",
    )
    atr_multiple = _coerce_positive_finite_float(
        value=stop_spec_payload.get("atr_multiple"),
        path="signal.metadata.stop_spec.atr_multiple",
    )
    return StopSpec(
        kind="hybrid",
        stop_price=stop_price,
        atr_multiple=atr_multiple,
        hybrid_policy=hybrid_policy,
        contract_version=1,
        raw_source="signal.metadata.stop_spec",
        details=details or None,
    )


def _validate_hybrid_policy_config(config: dict[str, Any]) -> None:
    risk_config = config.get("risk", {})
    if not isinstance(risk_config, dict):
        return

    hybrid_policy = risk_config.get("hybrid_policy")
    if hybrid_policy is not None and hybrid_policy not in _ALLOWED_HYBRID_POLICIES:
        raise _validation_error(
            "config.risk.hybrid_policy",
            f"one of {sorted(_ALLOWED_HYBRID_POLICIES)}",
            'risk:\n  hybrid_policy: "wider"',
            hybrid_policy,
        )


def normalize_stop_spec(
    signal: Signal,
    *,
    config: dict[str, Any],
) -> StopSpec | None:
    """
    Convert a Signal into a normalized StopSpec (contract_version=1).

    Returns:
      - StopSpec if the signal provides any stop intent (explicit/structural/atr/hybrid)
      - None if no stop intent is present at all

    This function does NOT enforce safe/strict behavior; it only parses/validates.
    """
    _validate_hybrid_policy_config(config)

    metadata = signal.metadata if isinstance(signal.metadata, dict) else {}

    if "stop_spec" in metadata:
        return _normalize_structured_stop_spec(metadata["stop_spec"])

    signal_stop_price = getattr(signal, "stop_price", None)
    if signal_stop_price is not None:
        return _normalize_explicit_stop(
            stop_price=signal_stop_price,
            path="signal.stop_price",
            raw_source="signal.stop_price",
        )

    if "stop_price" in metadata:
        return _normalize_explicit_stop(
            stop_price=metadata["stop_price"],
            path="signal.metadata.stop_price",
            raw_source="signal.metadata.stop_price",
        )

    return None
