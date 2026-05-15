"""Load canonical research_data panels for Bulletproof_bt backtests."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from bt.core.errors import DataError
from bt.data.feed import HistoricalDataFeed
from bt.data.parquet_io import ensure_pyarrow_parquet

FEATURE_COLUMNS = (
    "mark_close",
    "index_close",
    "funding_rate",
    "funding_source_ts",
    "open_interest",
    "oi_source_ts",
    "oi_change_1",
    "oi_change_pct_1",
    "premium_mark_vs_index",
    "basis_close_vs_index",
    "liq_buy_notional",
    "liq_sell_notional",
)

BAR_COLUMNS = ("ts", "symbol", "open", "high", "low", "close", "volume")


def research_panel_path(root: str | Path, exchange: str, symbol: str, timeframe: str = "1m") -> Path:
    return Path(root) / "canonical" / exchange / symbol / f"timeframe={timeframe}" / "research_panel.parquet"


def load_research_panels(
    root: str | Path,
    exchange: str,
    symbols: Iterable[str],
    timeframe: str = "1m",
) -> pd.DataFrame:
    ensure_pyarrow_parquet()
    frames: list[pd.DataFrame] = []
    missing: list[Path] = []
    for symbol in symbols:
        path = research_panel_path(root, exchange, symbol, timeframe)
        if not path.exists():
            missing.append(path)
            continue
        frame = pd.read_parquet(path)
        if frame.empty:
            continue
        frames.append(_prepare_panel_frame(frame, path))
    if missing:
        raise DataError(f"Missing research panel(s); first missing: {missing[0]}")
    if not frames:
        raise DataError("No research panel rows loaded")
    combined = pd.concat(frames, ignore_index=True)
    _assert_no_lookahead(combined)
    return combined.sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)


def load_stable_research_panel(
    root: str | Path,
    exchange: str,
    timeframe: str = "1m",
    stable_manifest: str | Path | None = None,
) -> pd.DataFrame:
    manifest_path = Path(stable_manifest) if stable_manifest else Path(root) / "manifests" / "stable_universe.parquet"
    if not manifest_path.exists():
        raise DataError(f"Stable universe manifest missing: {manifest_path}")
    stable = pd.read_parquet(manifest_path)
    native_col = "native_symbol" if "native_symbol" in stable.columns else "symbol"
    if "available" in stable.columns:
        stable = stable[stable["available"].astype(bool)]
    symbols = stable.loc[stable["exchange"].eq(exchange), native_col].astype(str).drop_duplicates().tolist()
    if not symbols:
        raise DataError(f"No stable symbols found for exchange={exchange}")
    return load_research_panels(root, exchange, symbols, timeframe)


def load_volatile_research_panel(
    root: str | Path,
    exchange: str,
    membership_path: str | Path,
    timeframe: str = "1m",
) -> pd.DataFrame:
    membership_file = Path(membership_path)
    if not membership_file.exists():
        raise DataError(f"Volatile universe membership missing: {membership_file}")
    membership = pd.read_parquet(membership_file)
    if membership.empty:
        raise DataError("Volatile universe membership is empty")
    membership["ts"] = pd.to_datetime(membership["ts"], utc=True)
    membership = membership[membership["exchange"].eq(exchange)].sort_values(["ts", "symbol"])
    if membership.empty:
        raise DataError(f"No volatile membership found for exchange={exchange}")
    symbols = membership["symbol"].astype(str).drop_duplicates().tolist()
    panels = load_research_panels(root, exchange, symbols, timeframe)
    return apply_volatile_membership(panels, membership)


def apply_volatile_membership(panels: pd.DataFrame, membership: pd.DataFrame) -> pd.DataFrame:
    """Filter panel rows to symbols active at each timestamp."""
    if panels.empty:
        return panels
    intervals = _membership_intervals(membership)
    if intervals.empty:
        return panels.iloc[0:0].copy()
    frames: list[pd.DataFrame] = []
    panels_by_symbol = {symbol: group for symbol, group in panels.groupby("symbol", sort=False)}
    for row in intervals.itertuples(index=False):
        group = panels_by_symbol.get(row.symbol)
        if group is None:
            continue
        mask = group["ts"].ge(row.start_ts)
        if pd.notna(row.end_ts):
            mask &= group["ts"].lt(row.end_ts)
        frames.append(group.loc[mask])
    if not frames:
        return panels.iloc[0:0].copy()
    filtered = pd.concat(frames, ignore_index=True)
    return filtered.drop_duplicates(["ts", "symbol"]).sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)


def build_research_panel_feed_from_config(config: dict[str, object]) -> HistoricalDataFeed:
    df = load_research_panel_from_config(config)
    return HistoricalDataFeed(df)


def load_research_panel_from_config(config: dict[str, object]) -> pd.DataFrame:
    exchange = str(config.get("exchange", "binance"))
    root = str(config.get("root", "research_data"))
    timeframe = str(config.get("timeframe", "1m"))
    universe = str(config.get("universe", "stable"))
    if universe == "stable":
        stable_manifest = config.get("stable_manifest")
        return load_stable_research_panel(root, exchange, timeframe, str(stable_manifest) if stable_manifest else None)
    if universe == "volatile":
        membership_path = config.get("membership_path")
        if not membership_path:
            raise DataError("Volatile research panel config requires membership_path")
        return load_volatile_research_panel(root, exchange, str(membership_path), timeframe)
    symbols = config.get("symbols")
    if not symbols:
        raise DataError("Research panel config requires universe stable/volatile or explicit symbols")
    if isinstance(symbols, str):
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
    else:
        symbol_list = list(symbols)  # type: ignore[arg-type]
    return load_research_panels(root, exchange, symbol_list, timeframe)


def _prepare_panel_frame(frame: pd.DataFrame, path: Path) -> pd.DataFrame:
    required = set(BAR_COLUMNS)
    missing = required - set(frame.columns)
    if missing:
        raise DataError(f"Research panel missing columns {sorted(missing)} at {path}")
    out = frame.copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    out = out[list(BAR_COLUMNS) + [col for col in FEATURE_COLUMNS if col in out.columns]]
    return out


def _assert_no_lookahead(df: pd.DataFrame) -> None:
    for col in ("funding_source_ts", "oi_source_ts"):
        if col not in df.columns:
            continue
        source = pd.to_datetime(df[col], utc=True, errors="coerce")
        mask = source.notna()
        if (source[mask] > df.loc[mask, "ts"]).any():
            raise DataError(f"{col} contains timestamps after bar ts")


def _membership_intervals(membership: pd.DataFrame) -> pd.DataFrame:
    unique_rebalances = membership["ts"].drop_duplicates().sort_values().reset_index(drop=True)
    next_by_ts = dict(zip(unique_rebalances, unique_rebalances.shift(-1)))
    intervals = membership.copy()
    intervals["start_ts"] = intervals["ts"]
    intervals["end_ts"] = intervals["ts"].map(next_by_ts)
    return intervals[["symbol", "start_ts", "end_ts"]].drop_duplicates()
