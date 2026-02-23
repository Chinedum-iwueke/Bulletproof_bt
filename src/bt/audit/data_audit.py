from __future__ import annotations

from typing import Any

import pandas as pd

from bt.data.resample import normalize_timeframe


def run_data_audit(df: pd.DataFrame, *, timeframe: str = "1m", max_violations: int = 25) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []

    tf = normalize_timeframe(timeframe, key_path="audit.timeframe")
    expected = pd.Timedelta(minutes=1 if tf.endswith("m") else 60)

    required = ["ts", "symbol", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            violations.append({"type": "missing_column", "column": col})

    if "ts" in df.columns:
        if df["ts"].duplicated().any():
            dup = df.loc[df["ts"].duplicated(), "ts"].iloc[0]
            violations.append({"type": "duplicate_timestamp", "ts": dup.isoformat() if hasattr(dup, "isoformat") else str(dup)})
        if not df["ts"].is_monotonic_increasing:
            violations.append({"type": "non_monotonic_timestamp"})
        if isinstance(df["ts"].dtype, pd.DatetimeTZDtype):
            if str(df["ts"].dt.tz) != "UTC":
                violations.append({"type": "timestamp_not_utc"})

    if all(col in df.columns for col in ["open", "high", "low", "close", "volume"]):
        invalid_ohlc = df[(df["low"] > df[["open", "close"]].min(axis=1)) | (df["high"] < df[["open", "close"]].max(axis=1)) | (df["high"] < df["low"])]
        if not invalid_ohlc.empty:
            row = invalid_ohlc.iloc[0]
            ts_val = row.get("ts")
            violations.append({"type": "invalid_ohlc", "ts": ts_val.isoformat() if hasattr(ts_val, "isoformat") else str(ts_val), "symbol": row.get("symbol")})
        invalid_vol = df[df["volume"] < 0]
        if not invalid_vol.empty:
            row = invalid_vol.iloc[0]
            ts_val = row.get("ts")
            violations.append({"type": "negative_volume", "ts": ts_val.isoformat() if hasattr(ts_val, "isoformat") else str(ts_val), "symbol": row.get("symbol")})

    if "ts" in df.columns and "symbol" in df.columns:
        for symbol, group in df.groupby("symbol", sort=False):
            gaps = group["ts"].sort_values().diff().dropna()
            bad = gaps[gaps > expected]
            if not bad.empty:
                violations.append({"type": "timestamp_gap", "symbol": symbol, "gap": str(bad.iloc[0])})
                break

    violations = violations[:max_violations]
    return {
        "passed": len(violations) == 0,
        "violation_count": len(violations),
        "violations": violations,
    }
