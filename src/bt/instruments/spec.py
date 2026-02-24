from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

InstrumentType = Literal["crypto", "forex", "equity", "futures"]
_ALLOWED_TYPES = {"crypto", "forex", "equity", "futures"}


@dataclass(frozen=True)
class InstrumentSpec:
    type: InstrumentType
    symbol: str

    # sizing / price grid
    tick_size: Optional[float] = None
    contract_size: Optional[float] = None  # FX/futures notionals; None for equity shares

    # FX convenience (may be explicit or "auto" later)
    pip_size: Optional[float] = None
    pip_value: Optional[float] = None

    # account currency context (optional for now)
    account_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    base_currency: Optional[str] = None

    # notes: later we'll add sessions/calendar, leverage defaults, etc.

    def validate(self) -> None:
        symbol = self.symbol.strip() if isinstance(self.symbol, str) else ""
        if not symbol:
            raise ValueError(f"instrument.symbol must be a non-empty string (got: {self.symbol!r})")

        if self.type not in _ALLOWED_TYPES:
            raise ValueError(
                "instrument.type must be one of "
                f"{sorted(_ALLOWED_TYPES)} (got: {self.type!r})"
            )

        self._validate_positive_optional("instrument.tick_size", self.tick_size)
        self._validate_positive_optional("instrument.contract_size", self.contract_size)
        self._validate_positive_optional("instrument.pip_size", self.pip_size)
        self._validate_positive_optional("instrument.pip_value", self.pip_value)

        # TODO(T2+): require contract_size for forex once FX execution/risk uses it.

    @staticmethod
    def _validate_positive_optional(key_path: str, value: Optional[float]) -> None:
        if value is None:
            return
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key_path} must be > 0 (got: {value!r})") from exc
        if parsed <= 0:
            raise ValueError(f"{key_path} must be > 0 (got: {value!r})")
