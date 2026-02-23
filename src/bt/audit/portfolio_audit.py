from __future__ import annotations


def inspect_portfolio(*, cash: float, equity: float, used_margin: float, allow_negative_cash: bool = False) -> list[dict]:
    violations: list[dict] = []
    if not allow_negative_cash and cash < 0:
        violations.append({"type": "negative_cash", "cash": cash})
    if equity + 1e-9 < used_margin:
        violations.append({"type": "margin_exceeds_equity", "equity": equity, "used_margin": used_margin})
    return violations
