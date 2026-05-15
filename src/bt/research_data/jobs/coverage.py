"""Coverage scanning and static dashboard generation."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.research_data.schemas import normalize_frame
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import timeframe_delta, utc_ts

COVERAGE_COLUMNS = (
    "exchange",
    "native_symbol",
    "canonical_symbol",
    "dataset",
    "timeframe",
    "start_ts",
    "end_ts",
    "expected_rows",
    "actual_rows",
    "missing_rows",
    "duplicate_rows",
    "gap_count",
    "largest_gap_minutes",
    "last_updated_at",
    "status",
)


def build_coverage(store: ResearchDataStore | None = None, exchange: str | None = None) -> pd.DataFrame:
    store = store or ResearchDataStore()
    rows: list[dict[str, object]] = []
    for path in _dataset_paths(store.root, exchange):
        rows.append(coverage_for_path(path))
    coverage = pd.DataFrame(rows, columns=COVERAGE_COLUMNS)
    store.write_atomic(coverage, store.manifest_path("coverage"))
    return coverage


def coverage_for_path(path: Path) -> dict[str, object]:
    dataset = path.name.removesuffix(".parquet")
    if path.name == "data.parquet":
        dataset = path.parent.parent.name
    timeframe = _timeframe_from_path(path)
    exchange, native_symbol = _exchange_symbol_from_path(path)
    try:
        df = pd.read_parquet(path)
        return coverage_for_frame(df, exchange, native_symbol, dataset, timeframe)
    except Exception:
        return _empty_row(exchange, native_symbol, dataset, timeframe, status="failed")


def coverage_for_frame(
    df: pd.DataFrame,
    exchange: str,
    native_symbol: str,
    dataset: str,
    timeframe: str,
) -> dict[str, object]:
    if df.empty or "ts" not in df.columns:
        return _empty_row(exchange, native_symbol, dataset, timeframe, status="empty")
    data = normalize_frame(df)
    ts = pd.to_datetime(data["ts"], utc=True, errors="coerce")
    canonical_symbol = (
        str(data["canonical_symbol"].dropna().iloc[0])
        if "canonical_symbol" in data.columns and data["canonical_symbol"].notna().any()
        else native_symbol
    )
    duplicate_rows = int(data.duplicated(_dedupe_key(data)).sum())
    ts_unique = ts.dropna().sort_values().drop_duplicates()
    if ts_unique.empty:
        return _empty_row(exchange, native_symbol, dataset, timeframe, status="failed")
    start = ts_unique.iloc[0]
    end = ts_unique.iloc[-1]
    freq = _expected_frequency(dataset, timeframe)
    expected_rows = len(ts_unique)
    missing_rows = 0
    gap_count = 0
    largest_gap_minutes = 0.0
    status = "ok"
    if freq is not None and len(ts_unique) > 1:
        diffs = ts_unique.diff().dropna()
        expected_rows = int(((end - start) / freq)) + 1
        missing_rows = max(expected_rows - len(ts_unique), 0)
        gap_count = int((diffs > freq).sum())
        max_gap = diffs.max()
        largest_gap_minutes = float(max_gap / pd.Timedelta(minutes=1)) if pd.notna(max_gap) else 0.0
    if duplicate_rows or missing_rows or gap_count:
        status = "warning"
    return {
        "exchange": exchange,
        "native_symbol": native_symbol,
        "canonical_symbol": canonical_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "start_ts": start,
        "end_ts": end,
        "expected_rows": int(expected_rows),
        "actual_rows": int(len(ts_unique)),
        "missing_rows": int(missing_rows),
        "duplicate_rows": duplicate_rows,
        "gap_count": gap_count,
        "largest_gap_minutes": largest_gap_minutes,
        "last_updated_at": utc_ts("now"),
        "status": status,
    }


def write_coverage_dashboard(store: ResearchDataStore | None = None) -> Path:
    store = store or ResearchDataStore()
    coverage_path = store.manifest_path("coverage")
    coverage = store.read(coverage_path)
    if coverage.empty:
        coverage = build_coverage(store)
    report_dir = store.root / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    output = report_dir / "coverage_summary.html"
    summary = (
        coverage.groupby(["exchange", "dataset", "status"], dropna=False)
        .size()
        .reset_index(name="count")
        if not coverage.empty
        else pd.DataFrame(columns=["exchange", "dataset", "status", "count"])
    )
    html = "\n".join(
        [
            "<html><head><title>Research Data Coverage</title>",
            "<style>body{font-family:sans-serif;margin:24px}table{border-collapse:collapse;width:100%;margin:16px 0}td,th{border:1px solid #ddd;padding:6px;text-align:left}th{background:#f5f5f5}.warning{color:#9a6700}.failed{color:#b42318}.ok{color:#067647}</style>",
            "</head><body>",
            "<h1>Research Data Coverage</h1>",
            f"<p>Generated at {utc_ts('now').isoformat()}</p>",
            "<h2>Status Summary</h2>",
            summary.to_html(index=False, escape=True),
            "<h2>Coverage Detail</h2>",
            coverage.sort_values(["exchange", "native_symbol", "dataset"]).to_html(index=False, escape=True),
            "</body></html>",
        ]
    )
    output.write_text(html, encoding="utf-8")
    return output


def _dataset_paths(root: Path, exchange: str | None) -> list[Path]:
    paths: list[Path] = []
    raw_root = root / "raw"
    canonical_root = root / "canonical"
    exchange_glob = exchange or "*"
    paths.extend(raw_root.glob(f"{exchange_glob}/*/*/timeframe=*/data.parquet"))
    paths.extend(canonical_root.glob(f"{exchange_glob}/*/timeframe=*/*.parquet"))
    return sorted(paths)


def _timeframe_from_path(path: Path) -> str:
    for part in path.parts:
        if part.startswith("timeframe="):
            return part.split("=", 1)[1]
    return "event"


def _exchange_symbol_from_path(path: Path) -> tuple[str, str]:
    parts = path.parts
    if "raw" in parts:
        idx = parts.index("raw")
        return parts[idx + 1], parts[idx + 2]
    if "canonical" in parts:
        idx = parts.index("canonical")
        return parts[idx + 1], parts[idx + 2]
    return "", ""


def _expected_frequency(dataset: str, timeframe: str) -> pd.Timedelta | None:
    if dataset in {"ohlcv", "mark", "index", "research_panel", "liquidation_1m"} and timeframe != "event":
        return timeframe_delta(timeframe)
    if dataset == "oi":
        return pd.Timedelta(minutes=5)
    if dataset == "funding":
        return pd.Timedelta(hours=8)
    return None


def _dedupe_key(df: pd.DataFrame) -> list[str]:
    if {"exchange", "native_symbol", "ts"}.issubset(df.columns):
        return ["exchange", "native_symbol", "ts"]
    if {"exchange", "symbol", "ts"}.issubset(df.columns):
        return ["exchange", "symbol", "ts"]
    return ["ts"]


def _empty_row(exchange: str, native_symbol: str, dataset: str, timeframe: str, status: str) -> dict[str, object]:
    return {
        "exchange": exchange,
        "native_symbol": native_symbol,
        "canonical_symbol": native_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "start_ts": pd.NaT,
        "end_ts": pd.NaT,
        "expected_rows": 0,
        "actual_rows": 0,
        "missing_rows": 0,
        "duplicate_rows": 0,
        "gap_count": 0,
        "largest_gap_minutes": 0.0,
        "last_updated_at": utc_ts("now"),
        "status": status,
    }
