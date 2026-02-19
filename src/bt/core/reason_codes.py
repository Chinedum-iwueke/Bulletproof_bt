"""Canonical client-facing reason codes."""
from __future__ import annotations

RISK_REJECT_INSUFFICIENT_MARGIN = "risk_rejected:insufficient_margin"
RISK_REJECT_MAX_POSITIONS = "risk_rejected:max_positions"
RISK_REJECT_NOTIONAL_CAP = "risk_rejected:notional_cap"
RISK_REJECT_STOP_UNRESOLVABLE = "risk_rejected:stop_unresolvable"
RISK_REJECT_MIN_STOP_DISTANCE = "risk_rejected:min_stop_distance"
RISK_SCALED_BY_MARGIN = "risk_scaled:margin"
FORCED_LIQUIDATION_END_OF_RUN = "liquidation:end_of_run"
FORCED_LIQUIDATION_MARGIN = "liquidation:negative_free_margin"
