"""R-multiple summary metrics helpers."""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable, Optional


@dataclass(frozen=True)
class RSummary:
    n: int
    ev_r: Optional[float]
    win_rate: Optional[float]
    avg_r_win: Optional[float]
    avg_r_loss: Optional[float]
    sum_r_pos: Optional[float]
    sum_r_neg_abs: Optional[float]
    profit_factor_r: Optional[float]
    payoff_ratio_r: Optional[float]


def summarize_r(r_values: Iterable[Optional[float]]) -> RSummary:
    """Summarize expectancy-style R-multiple metrics from per-trade R values."""
    values = []
    for value in r_values:
        if value is None:
            continue
        numeric = float(value)
        if not math.isfinite(numeric):
            continue
        values.append(numeric)
    n = len(values)
    if n == 0:
        return RSummary(
            n=0,
            ev_r=None,
            win_rate=None,
            avg_r_win=None,
            avg_r_loss=None,
            sum_r_pos=None,
            sum_r_neg_abs=None,
            profit_factor_r=None,
            payoff_ratio_r=None,
        )

    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]

    ev_r = sum(values) / n
    win_rate = len(wins) / n
    avg_r_win = (sum(wins) / len(wins)) if wins else None
    avg_r_loss = (sum(losses) / len(losses)) if losses else None

    sum_r_pos = sum(wins) if wins else None
    sum_r_neg_abs = abs(sum(losses)) if losses else None

    if sum_r_pos is None or sum_r_neg_abs is None or sum_r_neg_abs == 0:
        profit_factor_r = None
    else:
        profit_factor_r = sum_r_pos / sum_r_neg_abs

    if avg_r_win is None or avg_r_loss is None or avg_r_loss == 0:
        payoff_ratio_r = None
    else:
        payoff_ratio_r = avg_r_win / abs(avg_r_loss)

    return RSummary(
        n=n,
        ev_r=ev_r,
        win_rate=win_rate,
        avg_r_win=avg_r_win,
        avg_r_loss=avg_r_loss,
        sum_r_pos=sum_r_pos,
        sum_r_neg_abs=sum_r_neg_abs,
        profit_factor_r=profit_factor_r,
        payoff_ratio_r=payoff_ratio_r,
    )

