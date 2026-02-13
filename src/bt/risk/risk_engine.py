"""Risk engine that converts signals into order intents."""
from __future__ import annotations

from dataclasses import replace
import math
from typing import Any

import pandas as pd

from bt.core.enums import OrderType, Side
from bt.core.types import Bar, OrderIntent, Signal
from bt.risk.spec import parse_risk_spec
from bt.risk.stop_distance import resolve_stop_distance
from bt.risk.stop_resolution import STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY


class RiskEngine:
    def __init__(
        self,
        *,
        max_positions: int,
        risk_per_trade_pct: float,
        max_notional_per_symbol: float | None = None,
        margin_buffer_tier: int = 1,
        taker_fee_bps: float = 0.0,
        slippage_k_proxy: float = 0.0,
        eps: float = 1e-12,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.max_positions = max_positions
        self.risk_per_trade_pct = risk_per_trade_pct
        self.max_notional_per_symbol = max_notional_per_symbol
        self.margin_buffer_tier = int(margin_buffer_tier)
        self.taker_fee_bps = float(taker_fee_bps)
        self.slippage_k_proxy = float(slippage_k_proxy)
        self.eps = eps
        if config is None:
            raise ValueError("risk.mode and risk.r_per_trade are required")
        self._config = config
        self._risk_spec = parse_risk_spec(config)

    @staticmethod
    def _extract_stop_price(signal: object) -> float | None:
        if isinstance(signal, dict):
            value = signal.get("stop_price")
            return None if value is None else float(value)
        value = getattr(signal, "stop_price", None)
        if value is not None:
            return float(value)
        metadata = getattr(signal, "metadata", None)
        if isinstance(metadata, dict):
            value = metadata.get("stop_price")
            if value is not None:
                return float(value)
        return None

    @staticmethod
    def _round_qty(qty: float, rounding: str) -> float:
        if rounding == "none":
            return qty
        scale = 10**8
        if rounding == "floor":
            return math.floor(qty * scale) / scale
        if rounding == "round":
            return round(qty, 8)
        raise ValueError(f"Invalid risk.qty_rounding={rounding!r}")

    def compute_position_size_r(
        self,
        *,
        symbol: str,
        side: str,
        entry_price: float,
        signal: object,
        bars_by_symbol: dict[str, object],
        ctx: dict[str, object],
        equity: float,
    ) -> tuple[float, dict[str, object]]:
        if equity <= 0:
            raise ValueError(f"{symbol}: equity must be > 0, got {equity}")
        if self._risk_spec.r_per_trade <= 0:
            raise ValueError(f"{symbol}: r_per_trade must be > 0, got {self._risk_spec.r_per_trade}")

        risk_amount = equity * self._risk_spec.r_per_trade
        stop_result = resolve_stop_distance(
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            signal=signal,
            bars_by_symbol=bars_by_symbol,
            ctx=ctx,
            config=self._config,
        )
        stop_distance = float(stop_result.stop_distance)
        if stop_distance <= 0:
            raise ValueError(f"{symbol}: invalid stop_distance computed: {stop_distance}")

        min_stop_distance = self._risk_spec.min_stop_distance
        if min_stop_distance is not None:
            stop_distance = max(stop_distance, min_stop_distance)

        qty = risk_amount / stop_distance
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        qty_rounding = "none"
        if isinstance(risk_cfg, dict):
            qty_rounding = str(risk_cfg.get("qty_rounding", "none"))
        qty = self._round_qty(qty, qty_rounding)
        if not math.isfinite(qty) or qty <= 0:
            raise ValueError(f"{symbol}: invalid qty computed: {qty}")

        return qty, {
            "risk_amount": risk_amount,
            "stop_distance": stop_distance,
            "stop_source": stop_result.source,
            "stop_details": stop_result.details,
            "used_legacy_stop_proxy": False,
            "r_metrics_valid": stop_result.source in {"explicit_stop_price", "atr_multiple"},
        }

    def estimate_required_margin(
        self,
        *,
        notional: float,
        max_leverage: float,
        fee_buffer: float,
        slippage_buffer: float,
    ) -> float:
        leverage_for_margin = max(max_leverage, self.eps)
        return (notional / leverage_for_margin) + fee_buffer + slippage_buffer

    def _estimate_buffers(self, notional: float) -> tuple[float, float]:
        if self.margin_buffer_tier <= 1:
            return 0.0, 0.0
        fee_buffer = notional * (self.taker_fee_bps / 1e4)
        slippage_buffer = notional * self.slippage_k_proxy
        if self.margin_buffer_tier >= 3:
            fee_buffer *= 2.0
            slippage_buffer *= 2.0
        return fee_buffer, slippage_buffer

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

        if signal.side == Side.BUY:
            side = "long"
        elif signal.side == Side.SELL:
            side = "short"
        else:
            return None, "risk_rejected:invalid_side"

        stop_price = self._extract_stop_price(signal)
        signal_payload: object = signal if stop_price is None else {"stop_price": stop_price}
        bars_payload: dict[str, object] = {signal.symbol: bar}
        ctx_payload: dict[str, object] = {}
        if isinstance(signal, Signal):
            maybe_ctx = signal.metadata.get("ctx")
            if isinstance(maybe_ctx, dict):
                ctx_payload = maybe_ctx

        if self._risk_spec.mode not in {"r_fixed", "equity_pct"}:
            raise NotImplementedError(f"Unsupported risk mode: {self._risk_spec.mode}")
        try:
            desired_qty, risk_meta = self.compute_position_size_r(
                symbol=signal.symbol,
                side=side,
                entry_price=bar.close,
                signal=signal_payload,
                bars_by_symbol=bars_payload,
                ctx=ctx_payload,
                equity=equity,
            )
        except ValueError as exc:
            message = str(exc)
            if "stop distance cannot be resolved" in message:
                fallback_stop_distance = max(bar.high - bar.low, self.eps)
                desired_qty = (equity * self._risk_spec.r_per_trade) / fallback_stop_distance
                risk_meta = {
                    "risk_amount": None,
                    "stop_distance": None,
                    "stop_source": STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY,
                    "stop_details": {"fallback_stop_distance": fallback_stop_distance},
                    "used_legacy_stop_proxy": True,
                    "r_metrics_valid": False,
                }
            else:
                return None, f"risk_rejected:invalid_stop:{exc}"

        risk_meta.setdefault("used_legacy_stop_proxy", risk_meta.get("stop_source") == STOP_RESOLUTION_LEGACY_HIGH_LOW_PROXY)
        risk_meta.setdefault("r_metrics_valid", bool(risk_meta.get("stop_source") in {"explicit_stop_price", "atr_multiple"}))

        risk_budget = risk_meta["risk_amount"]
        stop_dist = risk_meta["stop_distance"]
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

        if free_margin <= 0:
            return None, "risk_rejected:insufficient_free_margin"

        notional = abs(order_qty) * bar.close
        fee_buffer, slippage_buffer = self._estimate_buffers(notional)
        margin_required = self.estimate_required_margin(
            notional=notional,
            max_leverage=max_leverage,
            fee_buffer=fee_buffer,
            slippage_buffer=slippage_buffer,
        )
        if margin_required > free_margin:
            return None, "risk_rejected:insufficient_free_margin"

        reason = "risk_approved"
        metadata = dict(signal.metadata)
        metadata.update(
            {
                "risk_budget": risk_budget,
                "stop_dist": stop_dist,
                "risk_amount": risk_meta["risk_amount"],
                "stop_distance": risk_meta["stop_distance"],
                "stop_source": risk_meta["stop_source"],
                "stop_details": risk_meta["stop_details"],
                "used_legacy_stop_proxy": risk_meta["used_legacy_stop_proxy"],
                "r_metrics_valid": risk_meta["r_metrics_valid"],
                "current_qty": cur_qty,
                "desired_qty": desired_qty,
                "flip": flip,
                "notional_est": notional,
                "cap_applied": cap_applied,
                "margin_required": margin_required,
                "margin_fee_buffer": fee_buffer,
                "margin_slippage_buffer": slippage_buffer,
                "free_margin": free_margin,
                "max_leverage": max_leverage,
                "scaled_by_margin": False,
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
