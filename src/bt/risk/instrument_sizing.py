from __future__ import annotations

from dataclasses import dataclass
import math

from bt.instruments.spec import InstrumentSpec


@dataclass(frozen=True)
class SizingResult:
    qty: float
    qty_rounded: float
    rounding_unit: float
    notional: float
    margin_required: float | None
    sizing_notes: dict[str, object]


def _floor_to_step(value: float, step: float) -> float:
    return math.floor(value / step) * step


def size_position_from_risk(
    *,
    instrument: InstrumentSpec | None,
    entry_price: float,
    stop_price: float,
    risk_amount: float,
    account_leverage: float | None,
    fx_lot_step: float | None = None,
    fx_pip_value_override: float | None = None,
) -> SizingResult:
    """Instrument-aware sizing from risk amount and stop distance."""
    if risk_amount <= 0:
        raise ValueError(f"risk_amount must be > 0 (got: {risk_amount!r})")
    stop_distance = abs(float(entry_price) - float(stop_price))
    if account_leverage is not None and float(account_leverage) <= 0:
        raise ValueError(f"risk.margin.leverage must be > 0 (got: {account_leverage!r})")

    if stop_distance <= 0:
        raise ValueError(
            "stop_distance must be > 0 from entry_price/stop_price; "
            f"got entry_price={entry_price!r}, stop_price={stop_price!r}"
        )

    # Preserve crypto/default behavior: qty = risk_amount / stop_distance
    if instrument is None or instrument.type == "crypto":
        qty = risk_amount / stop_distance
        notional = abs(qty) * float(entry_price)
        margin_required = None if account_leverage is None else notional / float(account_leverage)
        return SizingResult(
            qty=qty,
            qty_rounded=qty,
            rounding_unit=0.0,
            notional=notional,
            margin_required=margin_required,
            sizing_notes={"instrument_type": "crypto", "stop_distance": stop_distance},
        )

    if instrument.type == "equity":
        qty_raw = risk_amount / stop_distance
        qty_rounded = float(math.floor(qty_raw))
        if qty_rounded <= 0:
            raise ValueError(
                "equity sizing rounds to 0 shares; increase risk_amount or widen stop_distance. "
                f"Computed shares={qty_raw!r}"
            )
        notional = abs(qty_rounded) * float(entry_price)
        margin_required = None if account_leverage is None else notional / float(account_leverage)
        return SizingResult(
            qty=qty_raw,
            qty_rounded=qty_rounded,
            rounding_unit=1.0,
            notional=notional,
            margin_required=margin_required,
            sizing_notes={"instrument_type": "equity", "stop_distance": stop_distance},
        )

    if instrument.type == "forex":
        if instrument.contract_size is None or float(instrument.contract_size) <= 0:
            raise ValueError(
                "instrument.contract_size is required for forex sizing and must be > 0. "
                "Set instrument.contract_size (e.g., 100000)."
            )
        if fx_lot_step is None or float(fx_lot_step) <= 0:
            raise ValueError(
                "risk.fx.lot_step is required for forex sizing and must be > 0. "
                "Set risk.fx.lot_step (e.g., 0.01 for micro lots)."
            )

        account_ccy = instrument.account_currency
        quote_ccy = instrument.quote_currency
        if account_ccy and quote_ccy and account_ccy != quote_ccy:
            if instrument.pip_value is None and fx_pip_value_override is None:
                raise ValueError(
                    "forex sizing requires conversion when instrument.account_currency != instrument.quote_currency. "
                    "Set instrument.pip_value or risk.fx.pip_value_override."
                )

        contract_size = float(instrument.contract_size)
        qty_raw = risk_amount / (stop_distance * contract_size)
        lot_step = float(fx_lot_step)
        qty_rounded = _floor_to_step(qty_raw, lot_step)
        qty_rounded = round(qty_rounded, 12)
        if qty_rounded <= 0:
            raise ValueError(
                "forex sizing rounds to 0: risk too small for lot_step; reduce risk.fx.lot_step or increase risk. "
                f"Computed lots={qty_raw!r}, risk.fx.lot_step={lot_step!r}"
            )

        notional = abs(qty_rounded) * contract_size * float(entry_price)
        margin_required = None if account_leverage is None else notional / float(account_leverage)
        return SizingResult(
            qty=qty_raw,
            qty_rounded=qty_rounded,
            rounding_unit=lot_step,
            notional=notional,
            margin_required=margin_required,
            sizing_notes={"instrument_type": "forex", "stop_distance": stop_distance, "contract_size": contract_size},
        )

    # futures and others remain unsupported in T3 sizing path
    raise ValueError(
        f"instrument.type={instrument.type!r} is not supported by risk sizing yet. "
        "Use instrument.type in {'crypto','forex','equity'}."
    )
