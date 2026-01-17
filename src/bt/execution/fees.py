"""Fee models."""
from __future__ import annotations


class FeeModel:
    """Simple fee model in basis points."""

    def __init__(self, *, maker_fee_bps: float, taker_fee_bps: float) -> None:
        if maker_fee_bps < 0:
            raise ValueError("maker_fee_bps must be >= 0")
        if taker_fee_bps < 0:
            raise ValueError("taker_fee_bps must be >= 0")
        self._maker_fee_bps = maker_fee_bps
        self._taker_fee_bps = taker_fee_bps

    def fee_for_notional(self, *, notional: float, is_maker: bool) -> float:
        """Return absolute fee in quote currency."""
        if notional < 0:
            raise ValueError("notional must be >= 0")
        bps = self._maker_fee_bps if is_maker else self._taker_fee_bps
        return notional * bps / 10_000
