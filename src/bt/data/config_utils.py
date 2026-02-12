"""Config parsing helpers for data ingestion knobs."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import pandas as pd


def _parse_bound_utc(raw: Any, *, name: str) -> datetime:
    if isinstance(raw, (int, float)):
        ts = pd.to_datetime(raw, unit="ms", utc=True)
    else:
        ts = pd.to_datetime(raw, utc=True)

    if pd.isna(ts):
        raise ValueError(f"data.date_range {name} is invalid (got: {raw!r})")

    py_dt = ts.to_pydatetime()
    if py_dt.tzinfo is None:
        raise ValueError(f"data.date_range {name} must be timezone-aware UTC (got: {raw!r})")
    return py_dt


def parse_date_range(cfg: dict[str, Any]) -> Optional[tuple[datetime, datetime]]:
    """
    Reads cfg["data"]["date_range"] and returns (start_utc, end_utc) as tz-aware UTC datetimes,
    or None if date_range not provided.
    Accepts mapping form: {"start": <iso|epoch-ms>, "end": <iso|epoch-ms>}.
    Raises ValueError with actionable message on invalid.
    """
    data_cfg = cfg.get("data", {}) if isinstance(cfg, dict) else {}
    if not isinstance(data_cfg, dict):
        raise ValueError("config.data must be a mapping when provided")

    date_range = data_cfg.get("date_range")
    if date_range is None:
        return None

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_raw, end_raw = date_range
    elif isinstance(date_range, dict):
        start_raw = date_range.get("start")
        end_raw = date_range.get("end")
    else:
        raise ValueError("data.date_range must be a mapping with keys start/end")

    if start_raw is None or end_raw is None:
        raise ValueError(
            f"data.date_range requires both start and end (got: start={start_raw!r}, end={end_raw!r})"
        )

    try:
        start = _parse_bound_utc(start_raw, name="start")
        end = _parse_bound_utc(end_raw, name="end")
    except Exception as exc:
        if isinstance(exc, ValueError):
            raise
        raise ValueError(
            f"data.date_range contains invalid value(s): start={start_raw!r}, end={end_raw!r}"
        ) from exc

    if start >= end:
        raise ValueError(
            f"data.date_range must satisfy start < end (got: start={start_raw!r}, end={end_raw!r})"
        )

    return (start, end)
