"""Risk engine that converts signals into order intents."""
from __future__ import annotations

from dataclasses import replace

import pandas as pd

from bt.core.enums import OrderType, Side
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
        current_qty: float,
    ) -> tuple[OrderIntent | None, str]:
        """
        Returns (order_intent_or_none, reason_string).
        reason_string must be non-empty for both approve and reject.
        """

        if signal.side is None:
            return None, "risk_rejected:no_side"
        if signal.symbol != bar.symbol:
            return None, "risk_rejected:symbol_mismatch"
        if open_positions >= self.max_positions and current_qty == 0:
            return None, "risk_rejected:max_positions"
        if equity <= 0:
            return None, "risk_rejected:no_equity"
        if self.risk_per_trade_pct <= 0:
            return None, "risk_rejected:invalid_risk_pct"

        cur_qty = current_qty
        cur_side = None
        if cur_qty > 0:
            cur_side = Side.BUY
        elif cur_qty < 0:
            cur_side = Side.SELL

        if cur_qty != 0 and signal.side == cur_side:
            return None, "risk_rejected:already_in_position"

        risk_budget = equity * self.risk_per_trade_pct
        stop_dist = max(bar.high - bar.low, self.eps)
        desired_qty = risk_budget / stop_dist
        desired_notional = abs(desired_qty) * bar.close
        cap_applied = False

        if self.max_notional_per_symbol is not None and desired_notional > self.max_notional_per_symbol:
            scale = self.max_notional_per_symbol / desired_notional
            desired_qty *= scale
            desired_notional = abs(desired_qty) * bar.close
            cap_applied = True

        flip = cur_qty != 0 and signal.side != cur_side
        if flip:
            if signal.side == Side.SELL and cur_qty > 0:
                order_qty = -cur_qty - desired_qty
            elif signal.side == Side.BUY and cur_qty < 0:
                order_qty = -cur_qty + desired_qty
            else:
                return None, "risk_rejected:invalid_flip"
        else:
            order_qty = desired_qty if signal.side == Side.BUY else -desired_qty

        leverage_for_margin = max(max_leverage, self.eps)
        max_notional_by_margin = free_margin * leverage_for_margin
        scaled_by_margin = False

        if free_margin <= 0:
            return None, "risk_rejected:insufficient_free_margin"

        notional = abs(order_qty) * bar.close
        margin_required = notional / leverage_for_margin

        if margin_required > free_margin:
            max_order_qty = max_notional_by_margin / bar.close
            if flip:
                max_desired_qty = max_order_qty - abs(cur_qty)
            else:
                max_desired_qty = max_order_qty

            desired_qty = min(desired_qty, max_desired_qty)
            if desired_qty <= 0:
                return None, "risk_rejected:insufficient_free_margin"

            if flip:
                if signal.side == Side.SELL and cur_qty > 0:
                    order_qty = -cur_qty - desired_qty
                else:
                    order_qty = -cur_qty + desired_qty
            else:
                order_qty = desired_qty if signal.side == Side.BUY else -desired_qty

            scaled_by_margin = True
            notional = abs(order_qty) * bar.close
            margin_required = notional / leverage_for_margin

        reason = "risk_approved"
        metadata = dict(signal.metadata)
        metadata.update(
            {
                "risk_budget": risk_budget,
                "stop_dist": stop_dist,
                "current_qty": cur_qty,
                "desired_qty": desired_qty,
                "flip": flip,
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
            qty=order_qty,
            order_type=OrderType.MARKET,
            limit_price=None,
            reason=reason,
            metadata=signal_with_metadata.metadata,
        )
        return order_intent, reason
