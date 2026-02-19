from __future__ import annotations

from copy import deepcopy
from typing import Any

from bt.core.types import Bar
from bt.risk.contract import StopResolutionResult, StopSpec
from bt.risk.reject_codes import RISK_FALLBACK_LEGACY_PROXY
from bt.risk.stop_distance import resolve_stop_distance
from bt.risk.stop_resolution import STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY


def _missing_field_error(kind: str, field_name: str) -> ValueError:
    return ValueError(
        f"Invalid StopSpec for kind={kind!r}: missing required field '{field_name}'. "
        "Example fix snippet for signal.metadata.stop_spec:\n"
        f"signal:\n  metadata:\n    stop_spec:\n      kind: \"{kind}\"\n      {field_name}: 123.45"
    )


def _resolved_result(*, stop_price: float | None, stop_distance: float, stop_source: str, reason_code: str) -> StopResolutionResult:
    used_fallback = stop_source == STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY
    return StopResolutionResult(
        stop_price=stop_price,
        stop_distance=stop_distance,
        stop_source=stop_source,
        is_valid=True,
        used_fallback=used_fallback,
        reason_code=RISK_FALLBACK_LEGACY_PROXY if used_fallback else reason_code,
    )


def _resolve_via_existing(
    *,
    symbol: str,
    side: str,
    entry_price: float,
    stop_price: float | None,
    bar: Bar | Any,
    ctx: dict[str, Any],
    config: dict[str, Any],
) -> StopResolutionResult:
    stop_result = resolve_stop_distance(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        signal={"stop_price": stop_price} if stop_price is not None else {},
        bars_by_symbol={symbol: bar},
        ctx=ctx,
        config=config,
    )
    return _resolved_result(
        stop_price=stop_price,
        stop_distance=stop_result.stop_distance,
        stop_source=stop_result.source,
        reason_code="resolved:delegated",
    )


def resolve_stop_from_spec(
    spec: StopSpec,
    *,
    symbol: str,
    side: str,
    entry_price: float,
    bar: Any,
    ctx: dict[str, Any],
    config: dict[str, Any],
) -> StopResolutionResult:
    """
    Resolve a stop from a StopSpec into a StopResolutionResult.

    MUST preserve existing logic by delegating to the current stop-distance machinery
    (src/bt/risk/stop_distance.py and friends) rather than reimplementing calculations.

    This function is additive: existing callers remain unchanged in this task.
    """
    kind = spec.kind

    if kind in {"explicit", "structural"}:
        if spec.stop_price is None:
            raise _missing_field_error(kind, "stop_price")
        result = _resolve_via_existing(
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            stop_price=float(spec.stop_price),
            bar=bar,
            ctx=ctx,
            config=config,
        )
        return StopResolutionResult(
            stop_price=float(spec.stop_price),
            stop_distance=result.stop_distance,
            stop_source=result.stop_source,
            is_valid=result.is_valid,
            used_fallback=result.used_fallback,
            reason_code=f"resolved:{kind}",
            details=result.details,
        )

    if kind == "atr":
        if spec.atr_multiple is None:
            raise _missing_field_error(kind, "atr_multiple")
        delegated_config = deepcopy(config)
        risk_cfg = delegated_config.setdefault("risk", {})
        stop_cfg = risk_cfg.setdefault("stop", {})
        stop_cfg["mode"] = "atr"
        stop_cfg["atr_multiple"] = float(spec.atr_multiple)

        result = _resolve_via_existing(
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            stop_price=None,
            bar=bar,
            ctx=ctx,
            config=delegated_config,
        )
        return StopResolutionResult(
            stop_price=None,
            stop_distance=result.stop_distance,
            stop_source=result.stop_source,
            is_valid=result.is_valid,
            used_fallback=result.used_fallback,
            reason_code="resolved:atr",
            details=result.details,
        )

    if kind == "hybrid":
        raise NotImplementedError(
            "StopSpec kind='hybrid' is not supported by current stop-distance machinery yet. "
            "Provide signal.metadata.stop_spec with kind='explicit' (stop_price) or kind='atr' (atr_multiple)."
        )

    raise ValueError(f"{symbol}: unsupported StopSpec.kind={kind!r}.")
