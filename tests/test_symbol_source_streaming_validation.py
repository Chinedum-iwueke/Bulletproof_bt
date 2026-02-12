from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bt.data.symbol_source import SymbolDataSource


def _ts(minutes: int) -> datetime:
    return datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=minutes)


def _write_csv(path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_parquet(path, rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(rows)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path)


def test_monotonicity_violation_raises_csv(tmp_path) -> None:
    path = tmp_path / "aaa.csv"
    _write_csv(
        path,
        [
            {"ts": _ts(0).isoformat(), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 1},
            {"ts": _ts(2).isoformat(), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 1},
            {"ts": _ts(1).isoformat(), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 1},
        ],
    )

    with pytest.raises(ValueError, match="AAA") as exc:
        list(SymbolDataSource("AAA", str(path)))

    assert str(path) in str(exc.value)
    assert "2024-01-01 00:01:00+00:00" in str(exc.value)


def test_duplicate_timestamp_raises_parquet(tmp_path) -> None:
    path = tmp_path / "aaa.parquet"
    _write_parquet(
        path,
        [
            {"ts": _ts(0), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 1},
            {"ts": _ts(0), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 1},
            {"ts": _ts(1), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 1},
        ],
    )

    with pytest.raises(ValueError, match="non-monotonic ts"):
        list(SymbolDataSource("AAA", str(path)))


def test_ohlc_sanity_raises(tmp_path) -> None:
    path = tmp_path / "aaa.csv"
    _write_csv(
        path,
        [
            {"ts": _ts(0).isoformat(), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 1},
            {"ts": _ts(1).isoformat(), "open": 1, "high": 2, "low": 1.2, "close": 1.1, "volume": 1},
        ],
    )

    with pytest.raises(ValueError, match="invalid OHLC") as exc:
        list(SymbolDataSource("AAA", str(path)))

    assert "open=1.0" in str(exc.value)
    assert "low=1.2" in str(exc.value)


def test_volume_negative_raises(tmp_path) -> None:
    path = tmp_path / "aaa.csv"
    _write_csv(
        path,
        [
            {"ts": _ts(0).isoformat(), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": -1},
        ],
    )

    with pytest.raises(ValueError, match="negative volume"):
        list(SymbolDataSource("AAA", str(path)))


def test_missing_bars_not_synthesized(tmp_path) -> None:
    path = tmp_path / "aaa.parquet"
    _write_parquet(
        path,
        [
            {"ts": _ts(0), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
            {"ts": _ts(2), "open": 2, "high": 3, "low": 1.5, "close": 2.5, "volume": 11},
        ],
    )

    rows = list(SymbolDataSource("AAA", str(path)))
    assert len(rows) == 2
    assert [row[0] for row in rows] == [_ts(0), _ts(2)]


def test_date_range_filter_works(tmp_path) -> None:
    path = tmp_path / "aaa.csv"
    _write_csv(
        path,
        [
            {"ts": _ts(0).isoformat(), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
            {"ts": _ts(1).isoformat(), "open": 2, "high": 3, "low": 1.5, "close": 2.5, "volume": 11},
            {"ts": _ts(2).isoformat(), "open": 3, "high": 4, "low": 2.5, "close": 3.5, "volume": 12},
        ],
    )

    rows = list(SymbolDataSource("AAA", str(path), date_range=(_ts(1), _ts(2))))
    assert len(rows) == 1
    assert rows[0][0] == _ts(1)


def test_row_limit_works(tmp_path) -> None:
    path = tmp_path / "aaa.parquet"
    _write_parquet(
        path,
        [
            {"ts": _ts(0), "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
            {"ts": _ts(1), "open": 2, "high": 3, "low": 1.5, "close": 2.5, "volume": 11},
        ],
    )

    rows = list(SymbolDataSource("AAA", str(path), row_limit=1))
    assert len(rows) == 1
    assert rows[0][0] == _ts(0)
