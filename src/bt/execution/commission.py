from __future__ import annotations

from dataclasses import dataclass

from bt.instruments.spec import InstrumentSpec


@dataclass(frozen=True)
class CommissionSpec:
    mode: str  # "none" | "per_trade" | "per_share" | "per_lot"
    per_trade: float | None = None
    per_share: float | None = None
    per_lot: float | None = None


def _as_non_negative(value: float | None, *, key_path: str) -> float:
    if value is None:
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key_path} must be >= 0 (got: {value!r})") from exc
    if parsed < 0:
        raise ValueError(f"{key_path} must be >= 0 (got: {value!r})")
    return parsed


def compute_commission(
    *,
    instrument: InstrumentSpec | None,
    qty: float,
    commission: CommissionSpec,
) -> float:
    """Returns commission in account-currency units."""
    abs_qty = abs(float(qty))
    if abs_qty < 0:
        raise ValueError(f"qty must be finite and non-negative in magnitude (got: {qty!r})")

    mode = commission.mode
    if mode == "none":
        return 0.0
    if mode == "per_trade":
        return _as_non_negative(commission.per_trade, key_path="execution.commission.per_trade")
    if mode == "per_share":
        if instrument is not None and instrument.type != "equity":
            raise ValueError(
                "execution.commission.mode=per_share is equity-only. "
                "Set instrument.type=equity or use execution.commission.mode=per_trade."
            )
        per_share = _as_non_negative(commission.per_share, key_path="execution.commission.per_share")
        return abs_qty * per_share
    if mode == "per_lot":
        if instrument is None or instrument.type != "forex":
            raise ValueError(
                "execution.commission.mode=per_lot is forex-only. "
                "Set instrument.type=forex or use execution.commission.mode=per_trade."
            )
        per_lot = _as_non_negative(commission.per_lot, key_path="execution.commission.per_lot")
        return abs_qty * per_lot

    raise ValueError(
        "execution.commission.mode must be one of {'none','per_trade','per_share','per_lot'} "
        f"(got: {mode!r})"
    )
