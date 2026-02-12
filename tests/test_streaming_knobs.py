from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.data.load_feed import load_feed


def _write_legacy_dataset(dataset_dir: Path) -> None:
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.yaml").write_text(
        "format: per_symbol_parquet\n"
        "symbols: [AAA, BBB]\n"
        'path: "symbols/{symbol}.parquet"\n',
        encoding="utf-8",
    )

    rows = [
        {
            "ts": pd.Timestamp("2024-01-01T00:00:00Z"),
            "open": 1.0,
            "high": 1.2,
            "low": 0.9,
            "close": 1.1,
            "volume": 10.0,
        },
        {
            "ts": pd.Timestamp("2024-01-01T00:01:00Z"),
            "open": 2.0,
            "high": 2.2,
            "low": 1.9,
            "close": 2.1,
            "volume": 11.0,
        },
        {
            "ts": pd.Timestamp("2024-01-01T00:02:00Z"),
            "open": 3.0,
            "high": 3.2,
            "low": 2.9,
            "close": 3.1,
            "volume": 12.0,
        },
    ]

    for symbol in ("AAA", "BBB"):
        frame = pd.DataFrame(rows)
        frame["symbol"] = symbol
        frame.to_parquet(symbols_dir / f"{symbol}.parquet", index=False)


def _collect_ticks(dataset_dir: Path, config: dict) -> list[tuple[pd.Timestamp, list[str]]]:
    feed = load_feed(str(dataset_dir), config)
    out: list[tuple[pd.Timestamp, list[str]]] = []
    while True:
        bars = feed.next()
        if bars is None:
            break
        ts = next(iter(bars.values())).ts
        out.append((ts, list(bars.keys())))
    return out


def test_date_range_filters_rows_end_exclusive(tmp_path: Path) -> None:
    _write_legacy_dataset(tmp_path)
    t1 = pd.Timestamp("2024-01-01T00:01:00Z")
    t2 = pd.Timestamp("2024-01-01T00:02:00Z")

    ticks = _collect_ticks(
        tmp_path,
        {"data": {"date_range": {"start": t1.isoformat(), "end": t2.isoformat()}}},
    )

    assert [ts for ts, _ in ticks] == [t1]
    assert ticks[0][1] == ["AAA", "BBB"]


def test_row_limit_per_symbol_limits_each_symbol_independently(tmp_path: Path) -> None:
    _write_legacy_dataset(tmp_path)

    ticks = _collect_ticks(tmp_path, {"data": {"row_limit_per_symbol": 1}})

    assert len(ticks) == 1
    assert ticks[0][0] == pd.Timestamp("2024-01-01T00:00:00Z")
    assert ticks[0][1] == ["AAA", "BBB"]


def test_chunksize_validation_rejects_non_positive(tmp_path: Path) -> None:
    _write_legacy_dataset(tmp_path)

    with pytest.raises(ValueError, match="data.chunksize"):
        load_feed(str(tmp_path), {"data": {"chunksize": 0}})


def test_row_limit_validation_rejects_non_positive(tmp_path: Path) -> None:
    _write_legacy_dataset(tmp_path)

    with pytest.raises(ValueError, match="data.row_limit_per_symbol"):
        load_feed(str(tmp_path), {"data": {"row_limit_per_symbol": 0}})


def test_date_range_validation_rejects_bad_order(tmp_path: Path) -> None:
    _write_legacy_dataset(tmp_path)

    with pytest.raises(ValueError, match="data.date_range"):
        load_feed(
            str(tmp_path),
            {
                "data": {
                    "date_range": {
                        "start": "2024-01-01T00:02:00Z",
                        "end": "2024-01-01T00:01:00Z",
                    }
                }
            },
        )
