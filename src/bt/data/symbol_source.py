"""Streaming per-symbol data source with strict row validation."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional

import pandas as pd
import pyarrow.dataset as ds


def _normalize_columns(cols: Iterable[str]) -> dict[str, str]:
    rename_map = {
        "timestamp": "ts",
        "time": "ts",
        "date": "ts",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "vol": "volume",
    }
    return {col: rename_map.get(col.lower(), col.lower()) for col in cols}


def _parse_ts_like(series: pd.Series) -> pd.Series:
    if isinstance(series.dtype, pd.DatetimeTZDtype):
        parsed = pd.to_datetime(series, errors="coerce", utc=True)
    elif pd.api.types.is_datetime64_dtype(series):
        raise ValueError("timestamps must be timezone-aware UTC")
    elif pd.api.types.is_numeric_dtype(series):
        parsed = pd.to_datetime(series, unit="ms", errors="coerce", utc=True)
    else:
        parsed = pd.to_datetime(series, errors="coerce", utc=True)

    if parsed.isna().any():
        bad_index = int(parsed[parsed.isna()].index[0])
        bad_value = series.loc[bad_index]
        raise ValueError(f"unparseable timestamp value at index {bad_index}: {bad_value!r}")

    return parsed


def _parse_ts_array(values: object) -> list[datetime]:
    parsed = _parse_ts_like(pd.Series(values))
    return [value.to_pydatetime() for value in parsed]


def _validate_date_range(date_range: tuple[datetime, datetime]) -> tuple[datetime, datetime]:
    start, end = date_range
    if start.tzinfo is None or start.utcoffset() is None:
        raise ValueError("date_range start must be tz-aware UTC")
    if end.tzinfo is None or end.utcoffset() is None:
        raise ValueError("date_range end must be tz-aware UTC")
    if start.astimezone(timezone.utc) != start:
        raise ValueError("date_range start must be UTC")
    if end.astimezone(timezone.utc) != end:
        raise ValueError("date_range end must be UTC")
    if not start < end:
        raise ValueError("date_range must satisfy start < end (inclusive start, exclusive end)")
    return start, end


class SymbolDataSource:
    def __init__(
        self,
        symbol: str,
        path: str,
        *,
        date_range: Optional[tuple[datetime, datetime]] = None,
        row_limit: Optional[int] = None,
        chunksize: int = 200_000,
    ) -> None:
        self.symbol = symbol
        self.path = path
        self._path = Path(path)
        self.row_limit = row_limit
        self.chunksize = chunksize
        self.date_range = _validate_date_range(date_range) if date_range is not None else None

        if self.row_limit is not None and self.row_limit < 0:
            raise ValueError("row_limit must be >= 0")
        if self.chunksize <= 0:
            raise ValueError("chunksize must be > 0")

    def __iter__(self) -> Iterator[tuple[datetime, float, float, float, float, float]]:
        prev_ts: datetime | None = None
        emitted_count = 0

        for row in self._iter_rows():
            ts, o, h, l, c, v = row

            if prev_ts is not None and ts <= prev_ts:
                raise ValueError(
                    f"{self.symbol} {self.path}: non-monotonic ts at {ts} (prev={prev_ts})"
                )
            if l > min(o, c) or h < max(o, c) or l > h:
                raise ValueError(
                    f"{self.symbol} {self.path}: invalid OHLC at {ts} "
                    f"(open={o}, high={h}, low={l}, close={c})"
                )
            if v < 0:
                raise ValueError(
                    f"{self.symbol} {self.path}: negative volume at {ts} (volume={v})"
                )

            prev_ts = ts
            yield row
            emitted_count += 1

            if self.row_limit is not None and emitted_count >= self.row_limit:
                return

    def _iter_rows(self) -> Iterator[tuple[datetime, float, float, float, float, float]]:
        suffix = self._path.suffix.lower()
        if suffix == ".csv":
            yield from self._iter_rows_csv()
            return
        if suffix == ".parquet":
            yield from self._iter_rows_parquet()
            return
        raise ValueError(f"Unsupported file type for symbol source: {self.path}")

    def _iter_rows_csv(self) -> Iterator[tuple[datetime, float, float, float, float, float]]:
        for chunk in pd.read_csv(self._path, chunksize=self.chunksize):
            normalized = _normalize_columns(chunk.columns)
            chunk = chunk.rename(columns=normalized)

            if "ts" not in chunk.columns:
                raise ValueError(f"{self.symbol} {self.path}: missing ts/timestamp column")
            for col in ("open", "high", "low", "close", "volume"):
                if col not in chunk.columns:
                    raise ValueError(f"{self.symbol} {self.path}: missing required column {col}")

            ts_values = _parse_ts_like(chunk["ts"])

            for idx, ts in enumerate(ts_values):
                ts_dt = ts.to_pydatetime()
                if self.date_range is not None:
                    start, end = self.date_range
                    if ts_dt < start or ts_dt >= end:
                        continue

                if "symbol" in chunk.columns:
                    row_symbol = str(chunk.iloc[idx]["symbol"])
                    if row_symbol != self.symbol:
                        raise ValueError(
                            f"{self.symbol} {self.path}: unexpected symbol {row_symbol!r} at {ts_dt}"
                        )

                yield (
                    ts_dt,
                    float(chunk.iloc[idx]["open"]),
                    float(chunk.iloc[idx]["high"]),
                    float(chunk.iloc[idx]["low"]),
                    float(chunk.iloc[idx]["close"]),
                    float(chunk.iloc[idx]["volume"]),
                )

    def _iter_rows_parquet(self) -> Iterator[tuple[datetime, float, float, float, float, float]]:
        dataset = ds.dataset(self._path, format="parquet")
        available_columns = list(dataset.schema.names)
        normalized = _normalize_columns(available_columns)

        timestamp_col = self._select_timestamp_column(available_columns, normalized)
        required = {
            "open": self._select_column("open", available_columns, normalized),
            "high": self._select_column("high", available_columns, normalized),
            "low": self._select_column("low", available_columns, normalized),
            "close": self._select_column("close", available_columns, normalized),
            "volume": self._select_column("volume", available_columns, normalized),
        }

        symbol_col = None
        for col in available_columns:
            if normalized[col] == "symbol":
                symbol_col = col
                break

        scan_columns = [timestamp_col, *required.values()]
        if symbol_col is not None:
            scan_columns.append(symbol_col)

        scanner = dataset.scanner(columns=scan_columns, batch_size=self.chunksize)
        for batch in scanner.to_batches():
            batch_data = batch.to_pydict()
            ts_values = _parse_ts_array(batch_data[timestamp_col])

            opens = batch_data[required["open"]]
            highs = batch_data[required["high"]]
            lows = batch_data[required["low"]]
            closes = batch_data[required["close"]]
            volumes = batch_data[required["volume"]]
            symbols = batch_data[symbol_col] if symbol_col is not None else None

            for idx, ts in enumerate(ts_values):
                if self.date_range is not None:
                    start, end = self.date_range
                    if ts < start or ts >= end:
                        continue

                if symbols is not None and str(symbols[idx]) != self.symbol:
                    raise ValueError(
                        f"{self.symbol} {self.path}: unexpected symbol {symbols[idx]!r} at {ts}"
                    )

                yield (
                    ts,
                    float(opens[idx]),
                    float(highs[idx]),
                    float(lows[idx]),
                    float(closes[idx]),
                    float(volumes[idx]),
                )

    def _select_timestamp_column(self, columns: list[str], normalized: dict[str, str]) -> str:
        for col in columns:
            if col.lower() == "ts":
                return col
        for col in columns:
            if col.lower() == "timestamp":
                return col
        for col in columns:
            if normalized[col] == "ts":
                return col
        raise ValueError(f"{self.symbol} {self.path}: missing ts/timestamp column")

    def _select_column(self, target: str, columns: list[str], normalized: dict[str, str]) -> str:
        for col in columns:
            if normalized[col] == target:
                return col
        raise ValueError(f"{self.symbol} {self.path}: missing required column {target}")
