from __future__ import annotations

from typing import Optional


def compute_r_multiple(pnl: float, risk_amount: Optional[float]) -> Optional[float]:
    """
    Return pnl / risk_amount if risk_amount is valid (>0) else None.
    Deterministic: do not round here; rounding/formatting belongs to writers.
    """
    if risk_amount is None:
        return None
    if risk_amount <= 0:
        return None
    return pnl / risk_amount
