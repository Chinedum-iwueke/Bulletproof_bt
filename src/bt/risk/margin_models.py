from __future__ import annotations

from dataclasses import dataclass

from bt.instruments.spec import InstrumentSpec


@dataclass(frozen=True)
class MarginResult:
    notional: float
    margin_required: float
    leverage: float


def compute_margin_required(
    *,
    instrument: InstrumentSpec | None,
    qty: float,
    price: float,
    leverage: float | None,
) -> MarginResult | None:
    """Compute basic instrument-aware margin requirement."""
    if leverage is None:
        return None

    lev = float(leverage)
    if lev <= 0:
        raise ValueError(f"risk.margin.leverage must be > 0 (got: {leverage!r})")

    abs_qty = abs(float(qty))
    px = float(price)

    if instrument is None or instrument.type == "crypto":
        notional = abs_qty * px
    elif instrument.type == "equity":
        notional = abs_qty * px
    elif instrument.type == "forex":
        if instrument.contract_size is None or float(instrument.contract_size) <= 0:
            raise ValueError(
                "instrument.contract_size is required for forex margin and must be > 0. "
                "Set instrument.contract_size (e.g., 100000)."
            )
        notional = abs_qty * float(instrument.contract_size) * px
    else:
        raise ValueError(
            f"instrument.type={instrument.type!r} is not supported for margin yet."
        )

    return MarginResult(
        notional=notional,
        margin_required=notional / lev,
        leverage=lev,
    )
