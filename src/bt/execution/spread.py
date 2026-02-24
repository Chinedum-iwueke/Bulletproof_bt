"""Spread modeling helpers for deterministic execution pricing."""
from __future__ import annotations

from typing import Any, Literal

from bt.instruments.spec import InstrumentSpec


SpreadMode = Literal["none", "fixed_bps", "bar_range_proxy", "fixed_pips"]

_BAR_RANGE_PROXY_FACTOR = 0.5


def apply_spread(
    *,
    mode: SpreadMode,
    spread_bps: float,
    price: float,
    bar_high: float,
    bar_low: float,
    side: Literal["buy", "sell"],
) -> float:
    """Apply spread adjustment to a raw fill price."""
    if price <= 0:
        raise ValueError("price must be > 0")
    if bar_high < bar_low:
        raise ValueError("bar_high must be >= bar_low")
    if side not in {"buy", "sell"}:
        raise ValueError(f"Unsupported side: {side}")

    if mode == "none":
        return price

    if mode == "fixed_bps":
        if spread_bps < 0:
            raise ValueError("spread_bps must be >= 0 for fixed_bps mode")
        spread_frac = spread_bps / 10_000.0
        adjusted = price * (1.0 + spread_frac) if side == "buy" else price * (1.0 - spread_frac)
    elif mode == "bar_range_proxy":
        if spread_bps < 0:
            raise ValueError("spread_bps must be >= 0")
        proxy_spread = (bar_high - bar_low) * _BAR_RANGE_PROXY_FACTOR
        half_spread = proxy_spread / 2.0
        adjusted = price + half_spread if side == "buy" else price - half_spread
    else:
        raise ValueError(f"Unsupported spread mode: {mode}")

    if adjusted <= 0:
        raise ValueError("spread-adjusted price must be > 0")
    return adjusted


def apply_instrument_spread(
    *,
    price: float,
    side: str,
    spread: Any,
    instrument: InstrumentSpec | None,
) -> float:
    """Return spread-adjusted price with instrument-aware semantics."""
    if price <= 0:
        raise ValueError("price must be > 0")
    if side not in {"buy", "sell"}:
        raise ValueError(f"Unsupported side: {side!r}; expected 'buy' or 'sell'.")

    spread_cfg = spread if isinstance(spread, dict) else {}
    mode = spread_cfg.get("mode", "none")

    if mode == "none":
        return price

    if mode == "fixed_bps":
        raw_bps = spread_cfg.get("spread_bps", 0.0)
        try:
            spread_bps = float(raw_bps)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"execution.spread_bps must be >= 0 (got: {raw_bps!r})") from exc
        if spread_bps < 0:
            raise ValueError(f"execution.spread_bps must be >= 0 (got: {raw_bps!r})")
        # Keep legacy fixed_bps behavior: buy moved by +price*bps/10k and sell by -price*bps/10k.
        # Since this function applies +/- half_spread, represent total spread as 2x that move.
        spread_abs = 2.0 * price * (spread_bps / 10_000.0)
    elif mode == "fixed_pips":
        if instrument is None or instrument.type != "forex":
            raise ValueError(
                "execution.spread_mode=fixed_pips is FX-only. "
                "Set instrument.type=forex or use execution.spread_mode=fixed_bps."
            )
        raw_pips = spread_cfg.get("spread_pips")
        try:
            spread_pips = float(raw_pips)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"execution.spread_pips must be > 0 (got: {raw_pips!r})") from exc
        if spread_pips <= 0:
            raise ValueError(f"execution.spread_pips must be > 0 (got: {raw_pips!r})")

        if instrument.pip_size is not None:
            spread_abs = spread_pips * float(instrument.pip_size)
        elif instrument.tick_size is not None:
            spread_abs = spread_pips * float(instrument.tick_size)
        else:
            raise ValueError(
                "execution.spread_mode=fixed_pips requires instrument.pip_size or instrument.tick_size. "
                "Set instrument.pip_size (preferred) or instrument.tick_size."
            )
    elif mode == "bar_range_proxy":
        bar_high = spread_cfg.get("bar_high")
        bar_low = spread_cfg.get("bar_low")
        if bar_high is None or bar_low is None:
            raise ValueError("bar_range_proxy spread requires bar_high and bar_low in spread payload")
        spread_abs = (float(bar_high) - float(bar_low)) * _BAR_RANGE_PROXY_FACTOR
    else:
        raise ValueError(
            "Invalid execution.spread_mode: expected one of {'none', 'fixed_bps', 'bar_range_proxy', 'fixed_pips'} "
            f"got {mode!r}"
        )

    half_spread = spread_abs / 2.0
    adjusted = price + half_spread if side == "buy" else price - half_spread
    if adjusted <= 0:
        raise ValueError("spread-adjusted price must be > 0")
    return adjusted
