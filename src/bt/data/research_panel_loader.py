"""Load canonical research_data panels for Bulletproof_bt backtests."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Iterator

import pandas as pd

from bt.core.errors import DataError
from bt.core.types import Bar
from bt.data.feed import HistoricalDataFeed
from bt.data.parquet_io import ensure_pyarrow_parquet

FEATURE_COLUMNS = (
    "mark_close",
    "mark_price",
    "mark",
    "index_close",
    "index_price",
    "index",
    "funding_rate",
    "funding",
    "funding_raw",
    "funding_rate_realized",
    "funding_source_ts",
    "funding_available_at",
    "open_interest",
    "oi",
    "oi_value",
    "oi_contracts",
    "oi_usd",
    "oi_source_ts",
    "oi_available_at",
    "oi_change_1",
    "oi_change_pct_1",
    "premium_mark_vs_index",
    "premium",
    "premium_pct",
    "basis_close_vs_index",
    "basis",
    "basis_pct",
    "mark_index_basis",
    "mark_index_basis_pct",
    "liq_buy_notional",
    "liq_sell_notional",
    "available_at",
    "mark_available_at",
    "index_available_at",
)

BAR_COLUMNS = ("ts", "symbol", "open", "high", "low", "close", "volume")
STREAM_COLUMNS = BAR_COLUMNS + FEATURE_COLUMNS


def _is_present(value: object) -> bool:
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return True
    if isinstance(missing, bool):
        return not missing
    return True


def research_panel_path(root: str | Path, exchange: str, symbol: str, timeframe: str = "1m") -> Path:
    return Path(root) / "canonical" / exchange / symbol / f"timeframe={timeframe}" / "research_panel.parquet"


def load_research_panels(
    root: str | Path,
    exchange: str,
    symbols: Iterable[str],
    timeframe: str = "1m",
    *,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    row_limit_per_symbol: int | None = None,
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
        frame = _prepare_panel_frame(frame, path)
        frame = _filter_time_range(frame, start_ts=start_ts, end_ts=end_ts)
        if row_limit_per_symbol is not None and row_limit_per_symbol > 0:
            frame = frame.tail(row_limit_per_symbol)
        if frame.empty:
            continue
        frames.append(frame)
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
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    symbols_subset: list[str] | None = None,
    max_symbols: int | None = None,
    row_limit_per_symbol: int | None = None,
) -> pd.DataFrame:
    symbols = _stable_symbols(
        root=root,
        exchange=exchange,
        stable_manifest=str(stable_manifest) if stable_manifest else None,
        symbols_subset=symbols_subset,
        max_symbols=max_symbols,
    )
    return load_research_panels(
        root,
        exchange,
        symbols,
        timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        row_limit_per_symbol=row_limit_per_symbol,
    )


def _stable_symbols(
    *,
    root: str | Path,
    exchange: str,
    stable_manifest: str | None = None,
    symbols_subset: list[str] | None = None,
    max_symbols: int | None = None,
) -> list[str]:
    manifest_path = Path(stable_manifest) if stable_manifest else Path(root) / "manifests" / "stable_universe.parquet"
    if not manifest_path.exists():
        raise DataError(f"Stable universe manifest missing: {manifest_path}")
    stable = pd.read_parquet(manifest_path)
    native_col = "native_symbol" if "native_symbol" in stable.columns else "symbol"
    if "available" in stable.columns:
        stable = stable[stable["available"].astype(bool)]
    symbols = stable.loc[stable["exchange"].eq(exchange), native_col].astype(str).drop_duplicates().tolist()
    symbols = _apply_symbol_scope(symbols, symbols_subset=symbols_subset, max_symbols=max_symbols)
    if not symbols:
        raise DataError(f"No stable symbols found for exchange={exchange}")
    return symbols


def load_volatile_research_panel(
    root: str | Path,
    exchange: str,
    membership_path: str | Path,
    timeframe: str = "1m",
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    symbols_subset: list[str] | None = None,
    max_symbols: int | None = None,
    row_limit_per_symbol: int | None = None,
) -> pd.DataFrame:
    scoped_membership = _volatile_membership(
        exchange=exchange,
        membership_path=str(membership_path),
        start_ts=start_ts,
        end_ts=end_ts,
        symbols_subset=symbols_subset,
        max_symbols=max_symbols,
    )
    symbols = scoped_membership["symbol"].astype(str).drop_duplicates().tolist()
    panels = load_research_panels(
        root,
        exchange,
        symbols,
        timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        row_limit_per_symbol=row_limit_per_symbol,
    )
    return apply_volatile_membership(panels, scoped_membership)


def _volatile_membership(
    *,
    exchange: str,
    membership_path: str,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    symbols_subset: list[str] | None = None,
    max_symbols: int | None = None,
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
    membership = _dedupe_volatile_membership(membership)
    scoped_membership = _membership_overlapping_range(membership, start_ts=start_ts, end_ts=end_ts)
    symbols = scoped_membership["symbol"].astype(str).drop_duplicates().tolist()
    symbols = _apply_symbol_scope(symbols, symbols_subset=symbols_subset, max_symbols=max_symbols)
    return scoped_membership[scoped_membership["symbol"].astype(str).isin(set(symbols))]


def apply_volatile_membership(panels: pd.DataFrame, membership: pd.DataFrame) -> pd.DataFrame:
    """Annotate panel rows with volatile membership while preserving exit visibility."""
    if panels.empty:
        return panels
    intervals = _membership_intervals(membership)
    if intervals.empty:
        return panels.iloc[0:0].copy()
    frames: list[pd.DataFrame] = []
    panels_by_symbol = {symbol: group for symbol, group in panels.groupby("symbol", sort=False)}
    for symbol, symbol_intervals in intervals.groupby("symbol", sort=False):
        group = panels_by_symbol.get(symbol)
        if group is None:
            continue
        frames.append(_annotate_frame_to_membership(group, symbol_intervals))
    if not frames:
        return panels.iloc[0:0].copy()
    filtered = pd.concat(frames, ignore_index=True)
    return filtered.drop_duplicates(["ts", "symbol"]).sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)


def build_research_panel_feed_from_config(config: dict[str, object]) -> HistoricalDataFeed:
    mode = str(config.get("mode", "streaming"))
    if mode == "streaming":
        return build_streaming_research_panel_feed_from_config(config)  # type: ignore[return-value]
    if mode != "dataframe":
        raise DataError(f"Research panel data.mode must be streaming or dataframe, got {mode!r}")
    df = load_research_panel_from_config(config)
    return HistoricalDataFeed(df)


def build_streaming_research_panel_feed_from_config(config: dict[str, object]) -> "ResearchPanelStreamingFeed":
    exchange = str(config.get("exchange", "binance"))
    root = str(config.get("root", "research_data"))
    timeframe = str(config.get("timeframe", "1m"))
    universe = str(config.get("universe", "stable"))
    start_ts, end_ts = _parse_date_range(config.get("date_range"))
    symbols_subset = _symbols_from_config(config.get("symbols_subset", config.get("symbols")))
    max_symbols = _optional_int(config.get("max_symbols"))
    row_limit = _optional_int(config.get("row_limit_per_symbol"))
    chunksize = _optional_int(config.get("chunksize")) or 5_000

    if universe == "stable":
        stable_manifest = config.get("stable_manifest")
        symbols = _stable_symbols(
            root=root,
            exchange=exchange,
            stable_manifest=str(stable_manifest) if stable_manifest else None,
            symbols_subset=symbols_subset,
            max_symbols=max_symbols,
        )
        return ResearchPanelStreamingFeed(
            root=root,
            exchange=exchange,
            symbols=symbols,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            row_limit_per_symbol=row_limit,
            chunksize=chunksize,
        )

    if universe == "volatile":
        membership_path = config.get("membership_path")
        if not membership_path:
            raise DataError("Volatile research panel config requires membership_path")
        membership = _volatile_membership(
            exchange=exchange,
            membership_path=str(membership_path),
            start_ts=start_ts,
            end_ts=end_ts,
            symbols_subset=symbols_subset,
            max_symbols=max_symbols,
        )
        symbols = membership["symbol"].astype(str).drop_duplicates().tolist()
        return ResearchPanelStreamingFeed(
            root=root,
            exchange=exchange,
            symbols=symbols,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            row_limit_per_symbol=row_limit,
            chunksize=chunksize,
            membership=membership,
        )

    symbols = config.get("symbols")
    if not symbols:
        raise DataError("Research panel config requires universe stable/volatile or explicit symbols")
    if isinstance(symbols, str):
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
    else:
        symbol_list = [str(item).strip() for item in symbols]  # type: ignore[union-attr]
    symbol_list = _apply_symbol_scope(symbol_list, symbols_subset=symbols_subset, max_symbols=max_symbols)
    return ResearchPanelStreamingFeed(
        root=root,
        exchange=exchange,
        symbols=symbol_list,
        timeframe=timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        row_limit_per_symbol=row_limit,
        chunksize=chunksize,
    )


def load_research_panel_from_config(config: dict[str, object]) -> pd.DataFrame:
    exchange = str(config.get("exchange", "binance"))
    root = str(config.get("root", "research_data"))
    timeframe = str(config.get("timeframe", "1m"))
    universe = str(config.get("universe", "stable"))
    start_ts, end_ts = _parse_date_range(config.get("date_range"))
    symbols_subset = _symbols_from_config(config.get("symbols_subset", config.get("symbols")))
    max_symbols = _optional_int(config.get("max_symbols"))
    row_limit = _optional_int(config.get("row_limit_per_symbol"))
    if universe == "stable":
        stable_manifest = config.get("stable_manifest")
        return load_stable_research_panel(
            root,
            exchange,
            timeframe,
            str(stable_manifest) if stable_manifest else None,
            start_ts=start_ts,
            end_ts=end_ts,
            symbols_subset=symbols_subset,
            max_symbols=max_symbols,
            row_limit_per_symbol=row_limit,
        )
    if universe == "volatile":
        membership_path = config.get("membership_path")
        if not membership_path:
            raise DataError("Volatile research panel config requires membership_path")
        return load_volatile_research_panel(
            root,
            exchange,
            str(membership_path),
            timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            symbols_subset=symbols_subset,
            max_symbols=max_symbols,
            row_limit_per_symbol=row_limit,
        )
    symbols = config.get("symbols")
    if not symbols:
        raise DataError("Research panel config requires universe stable/volatile or explicit symbols")
    if isinstance(symbols, str):
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
    else:
        symbol_list = list(symbols)  # type: ignore[arg-type]
    symbol_list = _apply_symbol_scope(symbol_list, symbols_subset=symbols_subset, max_symbols=max_symbols)
    return load_research_panels(
        root,
        exchange,
        symbol_list,
        timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        row_limit_per_symbol=row_limit,
    )


class ResearchPanelStreamingFeed:
    """Bounded-memory feed for canonical research panel parquet files."""

    def __init__(
        self,
        *,
        root: str | Path,
        exchange: str,
        symbols: Iterable[str],
        timeframe: str = "1m",
        start_ts: pd.Timestamp | None = None,
        end_ts: pd.Timestamp | None = None,
        row_limit_per_symbol: int | None = None,
        chunksize: int = 5_000,
        membership: pd.DataFrame | None = None,
    ) -> None:
        if chunksize <= 0:
            raise DataError("Research panel chunksize must be positive")
        if row_limit_per_symbol is not None and row_limit_per_symbol <= 0:
            raise DataError("Research panel row_limit_per_symbol must be positive")
        self._root = Path(root)
        self._exchange = exchange
        self._symbols = list(dict.fromkeys(str(symbol) for symbol in symbols))
        self._timeframe = timeframe
        self._start_ts = start_ts
        self._end_ts = end_ts
        self._row_limit_per_symbol = row_limit_per_symbol
        self._chunksize = chunksize
        self._membership_intervals = _membership_intervals(membership) if membership is not None else None
        self._iter_by_symbol: dict[str, Iterator[Bar]] = {}
        self._buf_by_symbol: dict[str, Bar] = {}
        self._next_ts: pd.Timestamp | None = None
        self.reset()

    def symbols(self) -> list[str]:
        return list(self._symbols)

    def reset(self) -> None:
        ensure_pyarrow_parquet()
        self._iter_by_symbol = {}
        self._buf_by_symbol = {}
        missing: list[Path] = []
        for symbol in self._symbols:
            path = research_panel_path(self._root, self._exchange, symbol, self._timeframe)
            if not path.exists():
                missing.append(path)
                continue
            iterator = _iter_research_panel_bars(
                path=path,
                expected_symbol=symbol,
                start_ts=self._start_ts,
                end_ts=self._end_ts,
                row_limit_per_symbol=self._row_limit_per_symbol,
                chunksize=self._chunksize,
                membership_intervals=self._membership_intervals,
            )
            self._iter_by_symbol[symbol] = iterator
            try:
                self._buf_by_symbol[symbol] = next(iterator)
            except StopIteration:
                self._iter_by_symbol.pop(symbol, None)
        if missing:
            raise DataError(f"Missing research panel(s); first missing: {missing[0]}")
        self._next_ts = self._compute_next_ts()

    def _compute_next_ts(self) -> pd.Timestamp | None:
        if not self._buf_by_symbol:
            return None
        return min(bar.ts for bar in self._buf_by_symbol.values())

    def next(self) -> list[Bar] | None:
        if self._next_ts is None:
            return None
        current_ts = self._next_ts
        bars: list[Bar] = []
        symbols_to_advance: list[str] = []
        for symbol in self._symbols:
            bar = self._buf_by_symbol.get(symbol)
            if bar is None or bar.ts != current_ts:
                continue
            bars.append(bar)
            symbols_to_advance.append(symbol)
        for symbol in symbols_to_advance:
            iterator = self._iter_by_symbol[symbol]
            try:
                self._buf_by_symbol[symbol] = next(iterator)
            except StopIteration:
                self._buf_by_symbol.pop(symbol, None)
                self._iter_by_symbol.pop(symbol, None)
        self._next_ts = self._compute_next_ts()
        return bars


def _iter_research_panel_bars(
    *,
    path: Path,
    expected_symbol: str,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    row_limit_per_symbol: int | None,
    chunksize: int,
    membership_intervals: pd.DataFrame | None,
) -> Iterator[Bar]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    emitted = 0
    last_ts: pd.Timestamp | None = None
    source = pa.memory_map(str(path), "r")
    parquet_file = pq.ParquetFile(source)
    available_columns = set(parquet_file.schema_arrow.names)
    missing = set(BAR_COLUMNS) - available_columns
    if missing:
        raise DataError(f"Research panel missing columns {sorted(missing)} at {path}")
    read_columns = [col for col in STREAM_COLUMNS if col in available_columns]
    intervals = None
    if membership_intervals is not None and not membership_intervals.empty:
        intervals = membership_intervals[membership_intervals["symbol"].astype(str).eq(expected_symbol)]
        if intervals.empty:
            return
    row_groups = _overlapping_row_groups(parquet_file, start_ts=start_ts, end_ts=end_ts)
    for batch in parquet_file.iter_batches(batch_size=chunksize, columns=read_columns, row_groups=row_groups):
        frame = batch.to_pandas()
        if frame.empty:
            continue
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
        if start_ts is not None:
            frame = frame[frame["ts"].ge(start_ts)]
        if end_ts is not None:
            frame = frame[frame["ts"].lt(end_ts)]
        if intervals is not None:
            frame = _annotate_frame_to_membership(frame, intervals)
        for row in frame.itertuples(index=False):
            payload = row._asdict()
            bar = _row_to_bar(payload, expected_symbol=expected_symbol, path=path, last_ts=last_ts)
            yield bar
            emitted += 1
            last_ts = bar.ts
            if row_limit_per_symbol is not None and emitted >= row_limit_per_symbol:
                return


def _overlapping_row_groups(
    parquet_file: Any,
    *,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> list[int] | None:
    if start_ts is None and end_ts is None:
        return None
    try:
        ts_index = parquet_file.schema_arrow.names.index("ts")
    except ValueError:
        return None
    selected: list[int] = []
    for idx in range(parquet_file.num_row_groups):
        column = parquet_file.metadata.row_group(idx).column(ts_index)
        stats = column.statistics
        if stats is None or stats.min is None or stats.max is None:
            return None
        min_ts = pd.Timestamp(stats.min).tz_convert("UTC")
        max_ts = pd.Timestamp(stats.max).tz_convert("UTC")
        if start_ts is not None and max_ts < start_ts:
            continue
        if end_ts is not None and min_ts >= end_ts:
            continue
        selected.append(idx)
    return selected


def _row_to_bar(payload: dict[str, Any], *, expected_symbol: str, path: Path, last_ts: pd.Timestamp | None) -> Bar:
    symbol = str(payload.get("symbol", expected_symbol))
    if symbol != expected_symbol:
        raise DataError(f"Research panel symbol mismatch at {path}: expected {expected_symbol}, got {symbol}")
    ts = pd.to_datetime(payload["ts"], utc=True)
    if last_ts is not None and ts <= last_ts:
        raise DataError(f"Research panel non-monotonic timestamp at {path}: {ts}")
    extra = {key: value for key, value in payload.items() if key not in BAR_COLUMNS and _is_present(value)}
    for col in (
        "available_at",
        "funding_source_ts",
        "funding_available_at",
        "oi_source_ts",
        "oi_available_at",
        "mark_available_at",
        "index_available_at",
    ):
        if col not in extra:
            continue
        source_ts = pd.to_datetime(extra[col], utc=True)
        if source_ts > ts:
            raise DataError(f"{col} contains timestamps after bar ts at {path}")
        extra[col] = source_ts
    return Bar(
        ts=ts,
        symbol=symbol,
        open=float(payload["open"]),
        high=float(payload["high"]),
        low=float(payload["low"]),
        close=float(payload["close"]),
        volume=float(payload["volume"]),
        extra=extra,
    )


def _membership_active_mask(frame: pd.DataFrame, intervals: pd.DataFrame) -> pd.Series:
    mask = pd.Series(False, index=frame.index)
    for row in intervals.itertuples(index=False):
        interval_mask = frame["ts"].ge(row.start_ts)
        if pd.notna(row.end_ts):
            interval_mask &= frame["ts"].lt(row.end_ts)
        mask |= interval_mask
    return mask


def _annotate_frame_to_membership(frame: pd.DataFrame, intervals: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    active = _membership_active_mask(out, intervals)
    out["volatile_active"] = active.astype(bool)
    out["universe_active"] = active.astype(bool)
    return out


def _prepare_panel_frame(frame: pd.DataFrame, path: Path) -> pd.DataFrame:
    required = set(BAR_COLUMNS)
    missing = required - set(frame.columns)
    if missing:
        raise DataError(f"Research panel missing columns {sorted(missing)} at {path}")
    out = frame.copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    out = out[list(BAR_COLUMNS) + [col for col in FEATURE_COLUMNS if col in out.columns]]
    return out


def _parse_date_range(value: object) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if not isinstance(value, dict):
        return None, None
    start = pd.to_datetime(value.get("start"), utc=True, errors="raise") if value.get("start") else None
    end = pd.to_datetime(value.get("end"), utc=True, errors="raise") if value.get("end") else None
    return start, end


def _filter_time_range(
    frame: pd.DataFrame,
    *,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if start_ts is None and end_ts is None:
        return frame
    mask = pd.Series(True, index=frame.index)
    if start_ts is not None:
        mask &= frame["ts"].ge(start_ts)
    if end_ts is not None:
        mask &= frame["ts"].lt(end_ts)
    return frame.loc[mask]


def _symbols_from_config(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        symbols = [item.strip() for item in value.split(",") if item.strip()]
        return symbols or None
    if isinstance(value, list):
        symbols = [str(item).strip() for item in value if str(item).strip()]
        return symbols or None
    raise DataError("Research panel symbols/symbols_subset must be a list or comma-separated string")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _apply_symbol_scope(
    symbols: list[str],
    *,
    symbols_subset: list[str] | None = None,
    max_symbols: int | None = None,
) -> list[str]:
    out = list(dict.fromkeys(symbols))
    if symbols_subset is not None:
        allowed = set(symbols_subset)
        out = [symbol for symbol in out if symbol in allowed]
    if max_symbols is not None and max_symbols > 0:
        out = out[:max_symbols]
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
    membership = _dedupe_volatile_membership(membership)
    unique_rebalances = membership["ts"].drop_duplicates().sort_values().reset_index(drop=True)
    next_by_ts = dict(zip(unique_rebalances, unique_rebalances.shift(-1)))
    intervals = membership.copy()
    intervals["start_ts"] = intervals["ts"]
    intervals["end_ts"] = intervals["ts"].map(next_by_ts)
    return intervals[["symbol", "start_ts", "end_ts"]].drop_duplicates()


def _membership_overlapping_range(
    membership: pd.DataFrame,
    *,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    intervals = _membership_intervals(membership)
    if intervals.empty or (start_ts is None and end_ts is None):
        return membership
    mask = pd.Series(True, index=intervals.index)
    if end_ts is not None:
        mask &= intervals["start_ts"].lt(end_ts)
    if start_ts is not None:
        mask &= intervals["end_ts"].isna() | intervals["end_ts"].gt(start_ts)
    active_symbols = set(intervals.loc[mask, "symbol"].astype(str))
    return membership[membership["symbol"].astype(str).isin(active_symbols)]


def _dedupe_volatile_membership(membership: pd.DataFrame) -> pd.DataFrame:
    if membership is None or membership.empty:
        return membership
    out = membership.copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    out["symbol"] = out["symbol"].astype(str)
    if "score" in out.columns:
        out["_score"] = pd.to_numeric(out["score"], errors="coerce")
    else:
        out["_score"] = pd.Series(float("nan"), index=out.index, dtype="float64")
    out["_abs_score"] = out["_score"].abs().fillna(-1.0)
    rank_type = out["rank_type"].astype(str) if "rank_type" in out.columns else pd.Series("", index=out.index)
    out["_side_priority"] = 1
    out.loc[out["_score"].gt(0) & rank_type.eq("gainer"), "_side_priority"] = 0
    out.loc[out["_score"].lt(0) & rank_type.eq("loser"), "_side_priority"] = 0
    out.loc[out["_score"].eq(0) & rank_type.eq("gainer"), "_side_priority"] = 0
    out["_rank"] = pd.to_numeric(out["rank"], errors="coerce").fillna(1_000_000) if "rank" in out.columns else 1_000_000
    out = out.sort_values(
        ["ts", "symbol", "_abs_score", "_side_priority", "_rank"],
        ascending=[True, True, False, True, True],
        kind="mergesort",
    )
    return out.drop_duplicates(["ts", "symbol"], keep="first").drop(
        columns=["_score", "_abs_score", "_side_priority", "_rank"]
    )
