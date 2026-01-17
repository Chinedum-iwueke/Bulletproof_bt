"""Slippage models."""
from __future__ import annotations

from bt.core.types import Bar


class SlippageModel:
    """Simple volume/volatility based slippage model."""

    def __init__(
        self,
        *,
        k: float = 1.0,
        atr_pct_cap: float = 0.20,
        impact_cap: float = 0.05,
        eps: float = 1e-12,
    ) -> None:
        if k < 0:
            raise ValueError("k must be >= 0")
        if atr_pct_cap < 0:
            raise ValueError("atr_pct_cap must be >= 0")
        if impact_cap < 0:
            raise ValueError("impact_cap must be >= 0")
        if eps <= 0:
            raise ValueError("eps must be > 0")
        self._k = k
        self._atr_pct_cap = atr_pct_cap
        self._impact_cap = impact_cap
        self._eps = eps

    def estimate_slippage(self, *, qty: float, bar: Bar) -> float:
        """Absolute slippage in quote currency (>=0)."""
        atr_pct = (bar.high - bar.low) / max(bar.close, self._eps)
        atr_pct = max(0.0, min(atr_pct, self._atr_pct_cap))
        bar_dollar_volume = bar.close * bar.volume
        order_notional = abs(qty) * bar.close
        impact = 0.0
        if order_notional > 0:
            impact = self._k * atr_pct * (order_notional / max(bar_dollar_volume, self._eps))
        impact = max(0.0, min(impact, self._impact_cap))
        return order_notional * impact
