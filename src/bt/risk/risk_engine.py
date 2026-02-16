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


class RiskEngine:
    def __init__(
        self,
        *,
        max_positions: int,
        max_notional_per_symbol: float | None = None,
        margin_buffer_tier: int = 1,
        taker_fee_bps: float = 0.0,
        slippage_k_proxy: float = 0.0,
        eps: float = 1e-12,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.max_positions = max_positions
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

    def _stop_resolution_mode(self) -> str:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        mode = risk_cfg.get("stop_resolution", "strict") if isinstance(risk_cfg, dict) else "strict"
        return str(mode)

    def _min_stop_distance_pct(self) -> float:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        if not isinstance(risk_cfg, dict):
            return 0.001
        return float(risk_cfg.get("min_stop_distance_pct", 0.001))

    def _max_notional_pct_equity(self) -> float:
        risk_cfg = self._config.get("risk", {}) if isinstance(self._config, dict) else {}
        if not isinstance(risk_cfg, dict):
            return 1.0
        return float(risk_cfg.get("max_notional_pct_equity", 1.0))

    @staticmethod
    def _qty_sign_invariant_ok(*, signal_side: Side, current_qty: float, flip: bool, order_qty: float, close_only: bool) -> bool:
        if close_only:
            return order_qty == -current_qty
        if signal_side == Side.BUY and current_qty >= 0 and order_qty <= 0:
            return False
        if signal_side == Side.SELL and current_qty <= 0 and order_qty >= 0:
            return False
        if current_qty != 0 and flip:
            if current_qty > 0 and signal_side == Side.SELL and order_qty >= 0:
                return False
            if current_qty < 0 and signal_side == Side.BUY and order_qty <= 0:
                return False
        return True

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
            "r_metrics_valid": bool(stop_result.source) and stop_distance > 0,
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
        cur_qty = current_qty
        close_only = bool(signal.metadata.get("close_only"))
        if close_only and cur_qty == 0:
            return None, "risk_rejected:close_only_no_position"
        cur_side = None
        if cur_qty > 0:
            cur_side = Side.BUY
        elif cur_qty < 0:
            cur_side = Side.SELL

        if close_only and cur_qty != 0:
            order_qty = -cur_qty
            if not self._qty_sign_invariant_ok(
                signal_side=signal.side,
                current_qty=cur_qty,
                flip=(cur_qty != 0 and signal.side != cur_side),
                order_qty=order_qty,
                close_only=True,
            ):
                return None, f"risk_rejected:qty_sign_invariant_failed:current_qty={cur_qty}:signal_side={signal.side.value}:flip={cur_qty != 0 and signal.side != cur_side}:order_qty={order_qty}"
            reason = "risk_approved:close_only"
            metadata = dict(signal.metadata)
            metadata.update(
                {
                    "current_qty": cur_qty,
                    "desired_qty": 0.0,
                    "flip": False,
                    "close_only": True,
                    "notional_est": abs(order_qty) * bar.close,
                    "cap_applied": False,
                    "margin_required": 0.0,
                    "margin_fee_buffer": 0.0,
                    "margin_slippage_buffer": 0.0,
                    "margin_adverse_move_buffer": 0.0,
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
        stop_resolution_mode = self._stop_resolution_mode()
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
            if "stop distance cannot be resolved" in str(exc):
                if stop_resolution_mode == "strict":
                    return None, "risk_rejected:stop_unresolvable:strict"
                if stop_resolution_mode == "allow_legacy_proxy":
                    legacy_cfg = {"risk": dict(self._config.get("risk", {}))}
                    legacy_stop_cfg = dict(legacy_cfg["risk"].get("stop", {}))
                    legacy_stop_cfg["mode"] = "legacy_proxy"
                    legacy_cfg["risk"]["stop"] = legacy_stop_cfg
                    try:
                        risk_amount = equity * self._risk_spec.r_per_trade
                        stop_result = resolve_stop_distance(
                            symbol=signal.symbol,
                            side=side,
                            entry_price=bar.close,
                            signal=signal_payload,
                            bars_by_symbol=bars_payload,
                            ctx=ctx_payload,
                            config=legacy_cfg,
                        )
                        stop_distance = float(stop_result.stop_distance)
                        min_stop_distance = self._risk_spec.min_stop_distance
                        if min_stop_distance is not None:
                            stop_distance = max(stop_distance, min_stop_distance)
                        desired_qty = self._round_qty(risk_amount / stop_distance, str(self._config.get("risk", {}).get("qty_rounding", "none")))
                        if not math.isfinite(desired_qty) or desired_qty <= 0:
                            raise ValueError(f"{signal.symbol}: invalid qty computed: {desired_qty}")
                        risk_meta = {
                            "risk_amount": risk_amount,
                            "stop_distance": stop_distance,
                            "stop_source": "legacy_high_low_proxy",
                            "stop_details": stop_result.details,
                            "used_legacy_stop_proxy": True,
                            "r_metrics_valid": False,
                        }
                    except ValueError as legacy_exc:
                        return None, f"risk_rejected:invalid_stop:{legacy_exc}"
                else:
                    return None, f"risk_rejected:invalid_stop_resolution_mode:{stop_resolution_mode}"
            else:
                return None, f"risk_rejected:invalid_stop:{exc}"

        risk_meta["r_metrics_valid"] = bool(risk_meta.get("r_metrics_valid", True)) and bool(risk_meta.get("stop_source")) and float(risk_meta.get("stop_distance", 0.0)) > 0

        risk_budget = risk_meta["risk_amount"]
        stop_dist = float(risk_meta["stop_distance"])
        min_stop_distance_pct = self._min_stop_distance_pct()
        if bar.close > 0:
            stop_distance_pct = stop_dist / bar.close
            if stop_distance_pct < min_stop_distance_pct:
                return None, "stop_too_small"

        desired_notional = abs(desired_qty) * bar.close
        cap_applied = False
        cap_reason: str | None = None
        max_notional: float | None = None

        if self.max_notional_per_symbol is not None and desired_notional > self.max_notional_per_symbol:
            scale = self.max_notional_per_symbol / desired_notional
            desired_qty *= scale
            desired_notional = abs(desired_qty) * bar.close
            cap_applied = True
            cap_reason = "max_notional_per_symbol"
            max_notional = float(self.max_notional_per_symbol)

        max_notional_equity = equity * self._max_notional_pct_equity()
        if desired_notional > max_notional_equity:
            desired_qty = math.copysign(max_notional_equity / bar.close, desired_qty)
            desired_notional = abs(desired_qty) * bar.close
            cap_applied = True
            cap_reason = "max_notional_pct_equity"
            max_notional = max_notional_equity

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

        if not self._qty_sign_invariant_ok(
            signal_side=signal.side,
            current_qty=cur_qty,
            flip=flip,
            order_qty=order_qty,
            close_only=False,
        ):
            return None, f"risk_rejected:qty_sign_invariant_failed:current_qty={cur_qty}:signal_side={signal.side.value}:flip={flip}:order_qty={order_qty}"

        if free_margin <= 0:
            return None, "risk_rejected:insufficient_free_margin"

        notional = abs(order_qty) * bar.close
        fee_buffer, slippage_buffer = self._estimate_buffers(notional)
        adverse_move_buffer = 0.0
        if signal.side == Side.BUY:
            adverse_move_buffer = abs(order_qty) * max(bar.high - bar.close, 0.0)
        elif signal.side == Side.SELL:
            adverse_move_buffer = abs(order_qty) * max(bar.close - bar.low, 0.0)
        margin_required = self.estimate_required_margin(
            notional=notional,
            max_leverage=max_leverage,
            fee_buffer=fee_buffer + adverse_move_buffer,
            slippage_buffer=slippage_buffer,
        )
        scaled_by_margin = False
        if margin_required > free_margin:
            safe_free_margin = free_margin - fee_buffer - slippage_buffer - adverse_move_buffer
            max_affordable_notional = safe_free_margin * max_leverage
            if max_affordable_notional <= 0:
                return None, "risk_rejected:insufficient_free_margin"

            max_affordable_qty = max_affordable_notional / bar.close
            if max_affordable_qty <= 0:
                return None, "risk_rejected:insufficient_free_margin"

            if abs(order_qty) > max_affordable_qty:
                order_qty = math.copysign(max_affordable_qty, order_qty)
                scaled_by_margin = True
                if abs(order_qty) <= 0:
                    return None, "risk_rejected:insufficient_free_margin"
                notional = abs(order_qty) * bar.close
                fee_buffer, slippage_buffer = self._estimate_buffers(notional)
                adverse_move_buffer = 0.0
                if signal.side == Side.BUY:
                    adverse_move_buffer = abs(order_qty) * max(bar.high - bar.close, 0.0)
                elif signal.side == Side.SELL:
                    adverse_move_buffer = abs(order_qty) * max(bar.close - bar.low, 0.0)
                margin_required = self.estimate_required_margin(
                    notional=notional,
                    max_leverage=max_leverage,
                    fee_buffer=fee_buffer + adverse_move_buffer,
                    slippage_buffer=slippage_buffer,
                )

            if abs(order_qty) <= 0 or margin_required > free_margin:
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
                "r_metrics_valid": risk_meta["r_metrics_valid"],
                "used_legacy_stop_proxy": bool(risk_meta.get("used_legacy_stop_proxy", False)),
                "stop_resolution_mode": stop_resolution_mode,
                "current_qty": cur_qty,
                "desired_qty": desired_qty,
                "flip": flip,
                "notional_est": notional,
                "cap_applied": cap_applied,
                "cap_reason": cap_reason,
                "max_notional": max_notional,
                "margin_required": margin_required,
                "margin_fee_buffer": fee_buffer,
                "margin_slippage_buffer": slippage_buffer,
                "margin_adverse_move_buffer": adverse_move_buffer,
                "free_margin": free_margin,
                "max_leverage": max_leverage,
                "scaled_by_margin": scaled_by_margin,
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
