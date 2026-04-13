"""L1-H9 reusable momentum-breakout primitives."""
from __future__ import annotations


def ema_spread_pct(*, ema_fast: float, ema_slow: float, price: float) -> float | None:
    """Percent spread between fast/slow EMA relative to price."""
    if price == 0:
        return None
    return abs(float(ema_fast) - float(ema_slow)) / abs(float(price)) * 100.0


def breakout_distance_atr(*, close: float, breakout_level: float, atr: float, side: str) -> float | None:
    """Signed breakout distance in ATR units aligned to trade side."""
    if atr <= 0:
        return None
    side_key = str(side).lower()
    if side_key == "long":
        dist = float(close) - float(breakout_level)
    elif side_key == "short":
        dist = float(breakout_level) - float(close)
    else:
        raise ValueError(f"Unsupported side={side!r}")
    return dist / float(atr)


def breakout_close_strength(*, open_price: float, high: float, low: float, close: float, side: str) -> float | None:
    """Directional close strength in [0,1] where 1 is strongest close for side."""
    bar_range = float(high) - float(low)
    if bar_range <= 0:
        return None
    side_key = str(side).lower()
    if side_key == "long":
        raw = (float(close) - float(open_price)) / bar_range
    elif side_key == "short":
        raw = (float(open_price) - float(close)) / bar_range
    else:
        raise ValueError(f"Unsupported side={side!r}")
    return max(0.0, min(1.0, float(raw)))


__all__ = [
    "ema_spread_pct",
    "breakout_distance_atr",
    "breakout_close_strength",
]
