from __future__ import annotations

from typing import Any


def inspect_order(*, ts: Any, order: Any, min_qty: float = 0.0, min_notional: float = 0.0) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    violations: list[dict[str, Any]] = []
    notional_est = abs(float(order.qty)) * float(order.limit_price or 0.0)
    if order.qty <= 0:
        violations.append({"type": "non_positive_qty", "order_id": order.id})
    if abs(order.qty) < min_qty:
        violations.append({"type": "min_qty_violation", "order_id": order.id, "qty": order.qty, "min_qty": min_qty})
    if min_notional > 0 and notional_est < min_notional:
        violations.append({"type": "min_notional_violation", "order_id": order.id, "notional": notional_est, "min_notional": min_notional})
    if "price_reference" not in order.metadata:
        violations.append({"type": "missing_price_reference", "order_id": order.id})
    intent = {
        "ts": ts,
        "order_id": order.id,
        "symbol": order.symbol,
        "side": order.side.value,
        "qty": order.qty,
        "type": order.order_type.value,
        "limit_price": order.limit_price,
        "metadata": dict(order.metadata),
    }
    return intent, violations
