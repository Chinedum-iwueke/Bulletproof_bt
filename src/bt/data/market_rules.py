from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional
from zoneinfo import ZoneInfo

MarketType = Literal["crypto_24x7", "fx_24x5", "equity_session"]

_ALLOWED_DAYS = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
_DAY_INDEX = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


@dataclass(frozen=True)
class EquitySessionSpec:
    timezone: str
    open_time: str
    close_time: str
    trading_days: list[str]


@dataclass(frozen=True)
class MarketRules:
    market: MarketType
    allow_weekend_bars: bool
    equity_session: Optional[EquitySessionSpec]


def _parse_hhmm(value: str, *, key_path: str) -> tuple[int, int]:
    if not isinstance(value, str) or len(value) != 5 or value[2] != ":":
        raise ValueError(f"{key_path} must be 'HH:MM' (got: {value!r})")
    hh_raw, mm_raw = value[:2], value[3:]
    if not (hh_raw.isdigit() and mm_raw.isdigit()):
        raise ValueError(f"{key_path} must be 'HH:MM' (got: {value!r})")
    hh = int(hh_raw)
    mm = int(mm_raw)
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError(f"{key_path} must be valid 24h time 'HH:MM' (got: {value!r})")
    return hh, mm


def parse_market_rules(config: dict) -> MarketRules:
    data_cfg = config.get("data", {}) if isinstance(config, dict) else {}
    if not isinstance(data_cfg, dict):
        raise ValueError("config.data must be a mapping when provided")

    market_raw = data_cfg.get("market", "crypto_24x7")
    if market_raw not in {"crypto_24x7", "fx_24x5", "equity_session"}:
        raise ValueError(
            "data.market must be one of {'crypto_24x7','fx_24x5','equity_session'} "
            f"(got: {market_raw!r})"
        )
    market: MarketType = market_raw

    allow_weekend_bars = bool(data_cfg.get("allow_weekend_bars", False))

    equity_session: Optional[EquitySessionSpec] = None
    if market == "equity_session":
        raw_session = data_cfg.get("equity_session")
        if not isinstance(raw_session, dict):
            raise ValueError(
                "data.equity_session is required when data.market=equity_session. "
                "Set data.equity_session.timezone/open_time/close_time/trading_days."
            )
        timezone = raw_session.get("timezone")
        if not isinstance(timezone, str) or not timezone.strip():
            raise ValueError("data.equity_session.timezone must be a non-empty tz database string")
        try:
            ZoneInfo(timezone)
        except Exception as exc:
            raise ValueError(f"data.equity_session.timezone is invalid (got: {timezone!r})") from exc

        open_time = raw_session.get("open_time")
        close_time = raw_session.get("close_time")
        _parse_hhmm(open_time, key_path="data.equity_session.open_time")
        _parse_hhmm(close_time, key_path="data.equity_session.close_time")

        trading_days = raw_session.get("trading_days")
        if not isinstance(trading_days, list) or not trading_days:
            raise ValueError(
                "data.equity_session.trading_days must be a non-empty list like ['Mon','Tue','Wed','Thu','Fri']"
            )
        normalized_days: list[str] = []
        for item in trading_days:
            if not isinstance(item, str) or item not in _ALLOWED_DAYS:
                raise ValueError(
                    "data.equity_session.trading_days entries must be one of "
                    f"{sorted(_ALLOWED_DAYS)} (got: {item!r})"
                )
            if item not in normalized_days:
                normalized_days.append(item)

        equity_session = EquitySessionSpec(
            timezone=timezone,
            open_time=open_time,
            close_time=close_time,
            trading_days=normalized_days,
        )

    return MarketRules(
        market=market,
        allow_weekend_bars=allow_weekend_bars,
        equity_session=equity_session,
    )


def validate_market_timestamp(
    *,
    market_rules: MarketRules,
    symbol: str,
    ts_utc: datetime,
    path: str,
) -> None:
    if ts_utc.tzinfo is None:
        raise ValueError(f"{path}: symbol={symbol}: ts must be timezone-aware UTC (got: {ts_utc!r})")

    market = market_rules.market
    weekday = ts_utc.weekday()  # Mon=0 ... Sun=6

    if market == "crypto_24x7":
        return

    if market == "fx_24x5":
        if market_rules.allow_weekend_bars:
            return
        if weekday in {5, 6}:
            raise ValueError(
                f"{path}: symbol={symbol}: ts={ts_utc.isoformat()} violates data.market=fx_24x5 (weekend bar). "
                "Fix: remove weekend rows or set data.allow_weekend_bars=true."
            )
        return

    if market == "equity_session":
        session = market_rules.equity_session
        if session is None:
            raise ValueError(
                f"{path}: symbol={symbol}: data.market=equity_session requires data.equity_session.* configuration"
            )
        local_ts = ts_utc.astimezone(ZoneInfo(session.timezone))
        local_day = local_ts.weekday()
        allowed_day_indices = {_DAY_INDEX[d] for d in session.trading_days}

        if local_day not in allowed_day_indices:
            raise ValueError(
                f"{path}: symbol={symbol}: ts_utc={ts_utc.isoformat()} local={local_ts.isoformat()} violates "
                "data.market=equity_session trading day rule. "
                "Fix: adjust data.equity_session.trading_days or remove out-of-session rows."
            )

        open_h, open_m = _parse_hhmm(session.open_time, key_path="data.equity_session.open_time")
        close_h, close_m = _parse_hhmm(session.close_time, key_path="data.equity_session.close_time")
        local_minutes = local_ts.hour * 60 + local_ts.minute
        open_minutes = open_h * 60 + open_m
        close_minutes = close_h * 60 + close_m

        if not (open_minutes <= local_minutes < close_minutes):
            raise ValueError(
                f"{path}: symbol={symbol}: ts_utc={ts_utc.isoformat()} local={local_ts.isoformat()} violates "
                "data.market=equity_session trading hours. "
                "Fix: adjust data.equity_session.open_time/close_time/timezone or remove out-of-session rows."
            )
        return

    raise ValueError(f"Unsupported data.market={market!r}")
