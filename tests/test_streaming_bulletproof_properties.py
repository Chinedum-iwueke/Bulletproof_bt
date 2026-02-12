from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bt.data.load_feed import load_feed


def _utc_ts(seconds: int) -> datetime:
    return datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def _write_manifest(dataset_dir: Path, symbols: list[str]) -> None:
    (dataset_dir / "manifest.yaml").write_text(
        "format: per_symbol_parquet\n"
        f"symbols: [{', '.join(symbols)}]\n"
        'path: "symbols/{symbol}.parquet"\n',
        encoding="utf-8",
    )


def _write_symbol_parquet(dataset_dir: Path, symbol: str, rows: list[dict[str, Any]]) -> None:
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
        frame["symbol"] = symbol
    else:
        frame = pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "symbol"])

    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, symbols_dir / f"{symbol}.parquet")


def _iterate_ticks(feed: Any):
    if hasattr(feed, "peek_time") and hasattr(feed, "next"):
        while feed.peek_time() is not None:
            ts = feed.peek_time()
            bars = feed.next()
            if bars is None:
                break
            yield ts, bars
        return

    for entry in feed:
        if isinstance(entry, tuple) and len(entry) == 2:
            ts, bars = entry
        else:
            bars = entry
            ts = next(iter(bars.values())).ts if bars else None
        if bars is None:
            continue
        if ts is None:
            ts = next(iter(bars.values())).ts
        yield ts, bars


def _collect_sequence(feed: Any) -> list[tuple[str, tuple[str, ...], tuple[tuple[str, float, float, float, float, float], ...]]]:
    out = []
    for ts, bars in _iterate_ticks(feed):
        keys = tuple(sorted(bars.keys()))
        values = tuple(
            (
                symbol,
                round(float(bars[symbol].open), 10),
                round(float(bars[symbol].high), 10),
                round(float(bars[symbol].low), 10),
                round(float(bars[symbol].close), 10),
                round(float(bars[symbol].volume), 10),
            )
            for symbol in keys
        )
        out.append((pd.Timestamp(ts).isoformat(), keys, values))
    return out


def test_emit_only_from_buffered_rows_no_lookahead(tmp_path: Path) -> None:
    _write_manifest(tmp_path, ["AAA", "BBB"])
    _write_symbol_parquet(
        tmp_path,
        "AAA",
        [
            {"ts": _utc_ts(0), "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
            {"ts": _utc_ts(100), "open": 11, "high": 12, "low": 10, "close": 11.5, "volume": 101},
        ],
    )
    _write_symbol_parquet(
        tmp_path,
        "BBB",
        [
            {"ts": _utc_ts(50), "open": 20, "high": 21, "low": 19, "close": 20.5, "volume": 200},
        ],
    )

    feed = load_feed(str(tmp_path), config={})
    ticks = list(_iterate_ticks(feed))

    emitted_ts = [pd.Timestamp(ts) for ts, _ in ticks]
    assert emitted_ts == [_utc_ts(0), _utc_ts(50), _utc_ts(100)]
    assert emitted_ts.index(_utc_ts(100)) > emitted_ts.index(_utc_ts(50))


def test_missing_bars_not_filled_across_symbols(tmp_path: Path) -> None:
    _write_manifest(tmp_path, ["AAA", "BBB"])
    _write_symbol_parquet(
        tmp_path,
        "AAA",
        [
            {"ts": _utc_ts(0), "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
            {"ts": _utc_ts(120), "open": 12, "high": 13, "low": 11, "close": 12.5, "volume": 102},
        ],
    )
    _write_symbol_parquet(
        tmp_path,
        "BBB",
        [
            {"ts": _utc_ts(0), "open": 20, "high": 21, "low": 19, "close": 20.5, "volume": 200},
            {"ts": _utc_ts(60), "open": 21, "high": 22, "low": 20, "close": 21.5, "volume": 201},
            {"ts": _utc_ts(120), "open": 22, "high": 23, "low": 21, "close": 22.5, "volume": 202},
        ],
    )

    feed = load_feed(str(tmp_path), config={})
    ticks = list(_iterate_ticks(feed))

    assert [pd.Timestamp(ts) for ts, _ in ticks] == [_utc_ts(0), _utc_ts(60), _utc_ts(120)]
    assert set(ticks[0][1].keys()) == {"AAA", "BBB"}
    assert set(ticks[1][1].keys()) == {"BBB"}
    assert "AAA" not in ticks[1][1]
    assert set(ticks[2][1].keys()) == {"AAA", "BBB"}


def test_repeat_run_same_output_sequence(tmp_path: Path) -> None:
    _write_manifest(tmp_path, ["AAA", "BBB"])
    _write_symbol_parquet(
        tmp_path,
        "AAA",
        [
            {"ts": _utc_ts(0), "open": 1.0000000001, "high": 1.2, "low": 0.9, "close": 1.1, "volume": 10},
            {"ts": _utc_ts(120), "open": 2.0, "high": 2.2, "low": 1.9, "close": 2.1, "volume": 12},
        ],
    )
    _write_symbol_parquet(
        tmp_path,
        "BBB",
        [
            {"ts": _utc_ts(0), "open": 3.0, "high": 3.2, "low": 2.9, "close": 3.1, "volume": 20},
            {"ts": _utc_ts(60), "open": 4.0, "high": 4.2, "low": 3.9, "close": 4.1, "volume": 21},
            {"ts": _utc_ts(120), "open": 5.0, "high": 5.2, "low": 4.9, "close": 5.1, "volume": 22},
        ],
    )

    feed1 = load_feed(str(tmp_path), config={})
    sequence1 = _collect_sequence(feed1)

    feed2 = load_feed(str(tmp_path), config={})
    sequence2 = _collect_sequence(feed2)

    assert sequence1 == sequence2
    assert sequence1[0][1] == ("AAA", "BBB")
    assert sequence1[-1][1] == ("AAA", "BBB")
