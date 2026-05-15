"""Validation and coverage reporting for research datasets."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.research_data.time import timeframe_delta


def assert_utc_monotonic_unique(df: pd.DataFrame, key: tuple[str, ...] = ("exchange", "symbol", "ts")) -> None:
    if df.empty:
        return
    if "ts" not in df.columns:
        raise ValueError("missing ts column")
    if not pd.api.types.is_datetime64tz_dtype(df["ts"]):
        raise ValueError("ts must be timezone-aware UTC")
    if str(df["ts"].dt.tz) != "UTC":
        raise ValueError("ts must be UTC")
    if df.duplicated(list(key)).any():
        raise ValueError("duplicate timestamps detected")
    sort_cols = [col for col in ("exchange", "symbol", "ts") if col in df.columns]
    ordered = df.sort_values(sort_cols)
    if not ordered["ts"].equals(df["ts"].reset_index(drop=True)) and {"exchange", "symbol"}.issubset(df.columns):
        grouped = df.groupby(["exchange", "symbol"], sort=False)["ts"]
        if any(not values.is_monotonic_increasing for _, values in grouped):
            raise ValueError("timestamps must be monotonic within exchange/symbol")


def assert_causal_sources(panel: pd.DataFrame) -> None:
    for col in ("funding_source_ts", "oi_source_ts"):
        if col in panel.columns:
            source = pd.to_datetime(panel[col], utc=True, errors="coerce")
            mask = source.notna()
            if (source[mask] > panel.loc[mask, "ts"]).any():
                raise ValueError(f"{col} contains future data")


def coverage_report(df: pd.DataFrame, timeframe: str = "1m") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["exchange", "symbol", "start_ts", "end_ts", "rows", "expected_rows", "missing_bars"]
        )
    step = timeframe_delta(timeframe)
    rows: list[dict[str, object]] = []
    for (exchange, symbol), group in df.groupby(["exchange", "symbol"]):
        ordered = group.sort_values("ts")
        start = ordered["ts"].iloc[0]
        end = ordered["ts"].iloc[-1]
        expected = int(((end - start) / step)) + 1 if pd.notna(start) and pd.notna(end) else 0
        rows.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "start_ts": start,
                "end_ts": end,
                "rows": len(ordered),
                "expected_rows": expected,
                "missing_bars": max(expected - len(ordered), 0),
            }
        )
    return pd.DataFrame(rows)


def validate_parquet_file(path: Path, timeframe: str = "1m") -> dict[str, object]:
    df = pd.read_parquet(path)
    assert_utc_monotonic_unique(df)
    if path.name == "research_panel.parquet":
        assert_causal_sources(df)
    report = coverage_report(df, timeframe)
    missing_bars = int(report["missing_bars"].sum()) if "missing_bars" in report else 0
    return {
        "path": str(path),
        "rows": len(df),
        "status": "ok",
        "missing_bars": missing_bars,
    }
