from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import math
import statistics
from typing import Any, Iterable, Optional


@dataclass(frozen=True)
class BenchmarkMetrics:
    n_points: int
    total_return: float
    max_drawdown: float
    max_drawdown_pct: float
    volatility: Optional[float]
    sharpe: Optional[float]
    sortino: Optional[float]
    cagr: Optional[float]


def _parse_point(point: tuple[datetime, float] | object) -> tuple[datetime, float]:
    if isinstance(point, tuple) and len(point) == 2:
        ts, equity = point
    else:
        ts = getattr(point, "ts", None)
        equity = getattr(point, "equity", None)

    if not isinstance(ts, datetime):
        raise ValueError("benchmark equity points must include datetime ts values")

    try:
        equity_value = float(equity)
    except (TypeError, ValueError) as exc:
        raise ValueError("benchmark equity points must include numeric equity values") from exc

    return ts, equity_value


def _is_utc(ts: datetime) -> bool:
    return ts.tzinfo is not None and ts.utcoffset() == timezone.utc.utcoffset(ts)


def _infer_annualization_factor(ts_list: list[datetime]) -> Optional[float]:
    if len(ts_list) < 3:
        return None

    deltas = [
        (current - previous).total_seconds()
        for previous, current in zip(ts_list, ts_list[1:])
    ]
    if not deltas or any(delta <= 0 for delta in deltas):
        return None

    median_delta = statistics.median(deltas)
    tolerance = 0.15

    if abs(median_delta - 86400.0) <= 86400.0 * tolerance:
        return 252.0
    if abs(median_delta - 60.0) <= 60.0 * tolerance:
        return float(365 * 24 * 60)
    if abs(median_delta - 3600.0) <= 3600.0 * tolerance:
        return float(365 * 24)
    return None


def _std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def compute_benchmark_metrics(
    *,
    equity_points: Iterable[tuple[datetime, float]] | Iterable[object],
    risk_free_rate: float = 0.0,
    annualization_factor: Optional[float] = None,
) -> dict[str, Any]:
    """Compute benchmark metrics from (ts, equity) series."""
    ts_list: list[datetime] = []
    eq_list: list[float] = []

    for point in equity_points:
        ts, equity = _parse_point(point)
        ts_list.append(ts)
        eq_list.append(equity)

    if len(ts_list) < 2:
        raise ValueError("benchmark equity series must contain at least 2 points")

    previous_ts: datetime | None = None
    for ts in ts_list:
        if not _is_utc(ts):
            raise ValueError("benchmark equity timestamps must be tz-aware UTC datetimes")
        if previous_ts is not None and ts <= previous_ts:
            raise ValueError("benchmark equity timestamps must be strictly increasing")
        previous_ts = ts

    for equity in eq_list:
        if not math.isfinite(equity) or equity <= 0:
            raise ValueError("benchmark equity must be > 0 and finite")

    first_equity = eq_list[0]
    last_equity = eq_list[-1]
    total_return = (last_equity / first_equity) - 1.0

    running_peak = eq_list[0]
    max_drawdown = 0.0
    for equity in eq_list:
        if equity > running_peak:
            running_peak = equity
        drawdown = (running_peak - equity) / running_peak
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    returns = [
        (current / previous) - 1.0 for previous, current in zip(eq_list, eq_list[1:])
    ]

    resolved_annualization = annualization_factor
    if resolved_annualization is None:
        resolved_annualization = _infer_annualization_factor(ts_list)

    volatility: Optional[float] = None
    sharpe: Optional[float] = None
    sortino: Optional[float] = None

    if resolved_annualization is not None and returns:
        volatility = _std(returns) * math.sqrt(resolved_annualization)
        rf_per_period = risk_free_rate / resolved_annualization
        excess = [value - rf_per_period for value in returns]
        excess_std = _std(excess)
        if excess_std > 0:
            sharpe = (sum(excess) / len(excess)) / excess_std * math.sqrt(resolved_annualization)

        downside = [min(value, 0.0) for value in excess]
        downside_std = _std(downside)
        if downside_std > 0:
            sortino = (sum(excess) / len(excess)) / downside_std * math.sqrt(resolved_annualization)

    duration_seconds = (ts_list[-1] - ts_list[0]).total_seconds()
    duration_days = duration_seconds / (24.0 * 60.0 * 60.0)
    cagr: Optional[float]
    if duration_days < 30.0:
        cagr = None
    else:
        years = duration_seconds / (365.25243600 * 24.0 * 60.0 * 60.0)
        if years <= 0:
            cagr = None
        else:
            cagr = (last_equity / first_equity) ** (1.0 / years) - 1.0

    metrics = BenchmarkMetrics(
        n_points=len(eq_list),
        total_return=total_return,
        max_drawdown=max_drawdown,
        max_drawdown_pct=max_drawdown * 100.0,
        volatility=volatility,
        sharpe=sharpe,
        sortino=sortino,
        cagr=cagr,
    )
    return asdict(metrics)
