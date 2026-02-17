"""Canonical margin/equity accounting helpers shared by risk and liquidation."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarginSnapshot:
    equity: float
    margin_locked: float
    maintenance_required: float
    fee_buffer: float
    slippage_buffer: float
    adverse_move_buffer: float
    free_margin_post: float
    total_required: float
    mark_price_used_for_margin: float


def initial_margin_required(*, notional: float, max_leverage: float, eps: float = 1e-12) -> float:
    return float(notional) / max(float(max_leverage), eps)


def maintenance_margin_required(*, notional: float, max_leverage: float, eps: float = 1e-12) -> float:
    # Keep maintenance and initial margin aligned in this engine's canonical model.
    return initial_margin_required(notional=notional, max_leverage=max_leverage, eps=eps)


def compute_snapshot(
    *,
    equity: float,
    notional: float,
    max_leverage: float,
    maintenance_free_margin_pct: float,
    fee_buffer: float,
    slippage_buffer: float,
    adverse_move_buffer: float,
    mark_price_used_for_margin: float,
) -> MarginSnapshot:
    margin_locked = initial_margin_required(notional=notional, max_leverage=max_leverage)
    maintenance_required = maintenance_margin_required(notional=notional, max_leverage=max_leverage)
    total_required = margin_locked + fee_buffer + slippage_buffer + adverse_move_buffer
    free_margin_post = equity - total_required - (equity * maintenance_free_margin_pct)
    return MarginSnapshot(
        equity=equity,
        margin_locked=margin_locked,
        maintenance_required=maintenance_required,
        fee_buffer=fee_buffer,
        slippage_buffer=slippage_buffer,
        adverse_move_buffer=adverse_move_buffer,
        free_margin_post=free_margin_post,
        total_required=total_required,
        mark_price_used_for_margin=mark_price_used_for_margin,
    )

