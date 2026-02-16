from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite
from typing import Iterable, Literal

PriceField = Literal["close", "open"]


@dataclass(frozen=True)
class EquityPoint:
    ts: datetime  # tz-aware UTC
    equity: float


def _is_tz_aware_utc(ts: datetime) -> bool:
    tzinfo = ts.tzinfo
    if tzinfo is None:
        return False

    offset = tzinfo.utcoffset(ts)
    if offset != timedelta(0):
        return False

    tz_name = tzinfo.tzname(ts)
    return tz_name == "UTC"


def _symbol_of(bar: object) -> str:
    symbol = getattr(bar, "symbol", None)
    if isinstance(symbol, str) and symbol.strip():
        return symbol
    return "<unknown>"


def _get_valid_price(*, bar: object, field: PriceField, symbol: str) -> float:
    raw_price = getattr(bar, field, None)
    if isinstance(raw_price, bool) or not isinstance(raw_price, (int, float)):
        raise ValueError(
            f"Invalid benchmark price for symbol '{symbol}': {field} must be finite and > 0 "
            f"(got: {raw_price!r})"
        )

    price = float(raw_price)
    if not isfinite(price) or price <= 0:
        raise ValueError(
            f"Invalid benchmark price for symbol '{symbol}': {field} must be finite and > 0 "
            f"(got: {raw_price!r})"
        )

    return price


def compute_buy_hold_equity(
    *,
    bars: Iterable[object],
    initial_equity: float,
    price_field: PriceField = "close",
) -> list[EquityPoint]:
    """
    Compute buy & hold equity curve for a single symbol.

    Contract:
      - 'bars' is an iterable of Bar-like objects (from this repo) that have:
          bar.ts (datetime), bar.open, bar.close (float)
      - Only uses the provided bars; emits one EquityPoint per bar.
      - Entry happens at the first bar encountered:
          shares = initial_equity / entry_price
        where entry_price is bar.close (default) or bar.open (if configured).
      - equity_t = shares * price_t at each bar.
      - Raises ValueError if:
          - initial_equity <= 0
          - no bars are provided
          - any required price is missing/<=0
          - timestamps are not tz-aware UTC
    """

    if isinstance(initial_equity, bool) or not isinstance(initial_equity, (int, float)):
        raise ValueError(f"benchmark.initial_equity must be > 0 (got: {initial_equity!r})")

    initial_equity_value = float(initial_equity)
    if initial_equity_value <= 0:
        raise ValueError(f"benchmark.initial_equity must be > 0 (got: {initial_equity!r})")

    if price_field not in {"close", "open"}:
        raise ValueError(f"Unsupported price_field for benchmark buy&hold (got: {price_field!r})")

    points: list[EquityPoint] = []
    shares: float | None = None

    for bar in bars:
        ts = getattr(bar, "ts", None)
        symbol = _symbol_of(bar)
        if not isinstance(ts, datetime) or not _is_tz_aware_utc(ts):
            raise ValueError(
                f"Invalid benchmark bar timestamp for symbol '{symbol}': ts must be tz-aware UTC "
                f"(got: {ts!r})"
            )

        price = _get_valid_price(bar=bar, field=price_field, symbol=symbol)

        if shares is None:
            shares = initial_equity_value / price

        points.append(EquityPoint(ts=ts, equity=shares * price))

    if not points:
        raise ValueError("No bars provided for benchmark buy&hold")

    return points
