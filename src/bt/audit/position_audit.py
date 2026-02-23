from __future__ import annotations

from typing import Any


def inspect_position(symbol: str, position: Any) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    qty = float(position.qty)
    if qty > 0 and position.side is not None and position.side.value != "buy":
        violations.append({"type": "sign_side_mismatch", "symbol": symbol, "qty": qty, "side": position.side.value})
    if qty < 0:
        violations.append({"type": "negative_qty_internal", "symbol": symbol, "qty": qty})
    if qty == 0 and float(position.avg_entry_price) != 0.0:
        violations.append({"type": "zero_qty_nonzero_entry", "symbol": symbol, "avg_entry_price": position.avg_entry_price})
    return violations


def inspect_realized_transition(*, prev_qty: float, next_qty: float, realized_delta: float, closed_trades: int) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    crossed_zero = (prev_qty > 0 and next_qty < 0) or (prev_qty < 0 and next_qty > 0)
    if crossed_zero and closed_trades != 1:
        violations.append({"type": "cross_zero_close_count", "prev_qty": prev_qty, "next_qty": next_qty, "closed_trades": closed_trades})
    if realized_delta != 0.0 and prev_qty == 0.0:
        violations.append({"type": "realized_changed_while_flat", "realized_delta": realized_delta})
    return violations
