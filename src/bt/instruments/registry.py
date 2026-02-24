from __future__ import annotations

from typing import Any, Optional, cast

from bt.instruments.spec import InstrumentSpec


def resolve_instrument_spec(config: dict[str, Any], *, symbol: Optional[str] = None) -> InstrumentSpec | None:
    """
    Return InstrumentSpec if config includes an `instrument:` block, else None.

    Rules:
    - instrument block is optional.
    - If symbol is passed and instrument.symbol missing, default instrument.symbol = symbol.
    - If instrument.symbol exists and symbol is passed but differs, raise ValueError with actionable message.
    - Validate spec before returning.
    """
    instrument_raw = config.get("instrument")
    if instrument_raw is None:
        return None
    if not isinstance(instrument_raw, dict):
        raise ValueError("instrument must be a mapping when provided")

    instrument = dict(instrument_raw)

    resolved_symbol = instrument.get("symbol")
    if resolved_symbol is None and symbol is not None:
        instrument["symbol"] = symbol
    elif resolved_symbol is not None and symbol is not None and str(resolved_symbol) != symbol:
        raise ValueError(
            "instrument.symbol conflicts with requested symbol: "
            f"instrument.symbol={resolved_symbol!r}, symbol={symbol!r}. "
            "Set instrument.symbol to match the resolved symbol universe."
        )

    spec = InstrumentSpec(
        type=cast(Any, instrument.get("type", "crypto")),
        symbol=str(instrument.get("symbol", "")),
        tick_size=instrument.get("tick_size"),
        contract_size=instrument.get("contract_size"),
        pip_size=instrument.get("pip_size"),
        pip_value=instrument.get("pip_value"),
        account_currency=instrument.get("account_currency"),
        quote_currency=instrument.get("quote_currency"),
        base_currency=instrument.get("base_currency"),
    )
    spec.validate()
    return spec
