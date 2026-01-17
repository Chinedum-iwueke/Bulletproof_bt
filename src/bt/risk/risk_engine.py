"""Risk engine that converts signals into order intents."""
from __future__ import annotations

from dataclasses import replace

import pandas as pd

from bt.core.enums import OrderType
from bt.core.types import Bar, OrderIntent, Signal


class RiskEngine:
    def __init__(
        self,
        *,
        max_positions: int,
        risk_per_trade_pct: float,
        max_notional_per_symbol: float | None = None,
        eps: float = 1e-12,
    ) -> None:
        self.max_positions = max_positions
        self.risk_per_trade_pct = risk_per_trade_pct
        self.max_notional_per_symbol = max_notional_per_symbol
        self.eps = eps

    def signal_to_order_intent(
        self,
        *,
        ts: pd.Timestamp,
        signal: Signal,
        bar: Bar,
        equity: float,
        free_margin: float,
        open_positions: int,
        max_leverage: float,
    ) -> tuple[OrderIntent | None, str]:
        """
        Returns (order_intent_or_none, reason_string).
        reason_string must be non-empty for both approve and reject.
        """

        if signal.side is None:
            return None, "risk_rejected:no_side"
        if signal.symbol != bar.symbol:
            return None, "risk_rejected:symbol_mismatch"
        if open_positions >= self.max_positions:
            return None, "risk_rejected:max_positions"
        if equity <= 0:
            return None, "risk_rejected:no_equity"
        if self.risk_per_trade_pct <= 0:
            return None, "risk_rejected:invalid_risk_pct"

        risk_budget = equity * self.risk_per_trade_pct
        stop_dist = max(bar.high - bar.low, self.eps)
        qty = risk_budget / stop_dist
        notional = abs(qty) * bar.close
        cap_applied = False

        if self.max_notional_per_symbol is not None and notional > self.max_notional_per_symbol:
            scale = self.max_notional_per_symbol / notional
            qty *= scale
            notional = abs(qty) * bar.close
            cap_applied = True

        leverage_for_margin = max(max_leverage, self.eps)
        margin_required = notional / leverage_for_margin
        max_notional_by_margin = free_margin * leverage_for_margin
        scaled_by_margin = False

        if free_margin <= 0:
            return None, "risk_rejected:insufficient_free_margin"

        if margin_required > free_margin:
            qty = min(qty, max_notional_by_margin / bar.close)
            if qty <= 0:
                return None, "risk_rejected:insufficient_free_margin"
            scaled_by_margin = True
            notional = abs(qty) * bar.close
            margin_required = notional / leverage_for_margin

        reason = "risk_approved"
        metadata = dict(signal.metadata)
        metadata.update(
            {
                "risk_budget": risk_budget,
                "stop_dist": stop_dist,
                "notional_est": notional,
                "cap_applied": cap_applied,
                "margin_required": margin_required,
                "free_margin": free_margin,
                "max_leverage": max_leverage,
                "scaled_by_margin": scaled_by_margin,
                "max_notional_by_margin": max_notional_by_margin,
                "reason": reason,
            }
        )
        signal_with_metadata = replace(signal, metadata=metadata)

        order_intent = OrderIntent(
            ts=ts,
            symbol=signal.symbol,
            side=signal.side,
            qty=qty,
            order_type=OrderType.MARKET,
            limit_price=None,
            reason=reason,
            metadata=signal_with_metadata.metadata,
        )
        return order_intent, reason
