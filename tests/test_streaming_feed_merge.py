from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.data.dataset import load_dataset_manifest
from bt.data.stream_feed import StreamingHistoricalDataFeed


def _write_legacy_manifest(dataset_dir: Path, symbols: list[str]) -> None:
    manifest = (
        "format: per_symbol_parquet\n"
        f"symbols: [{', '.join(symbols)}]\n"
        'path: "symbols/{symbol}.parquet"\n'
    )
    (dataset_dir / "manifest.yaml").write_text(manifest, encoding="utf-8")


def _write_symbol_parquet(dataset_dir: Path, symbol: str, rows: list[tuple[str, float, float, float, float, float]]) -> None:
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        rows,
        columns=["ts", "open", "high", "low", "close", "volume"],
    )
    if not frame.empty:
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
        frame["symbol"] = symbol
    else:
        frame = pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "symbol"])
    frame.to_parquet(symbols_dir / f"{symbol}.parquet", index=False)


def _collect(feed: StreamingHistoricalDataFeed):
    out = []
    while True:
        bars = feed.next()
        if bars is None:
            break
        ts = next(iter(bars.values())).ts
        snapshot = (
            ts,
            list(bars.keys()),
            [
                (
                    sym,
                    round(bar.open, 6),
                    round(bar.high, 6),
                    round(bar.low, 6),
                    round(bar.close, 6),
                    round(bar.volume, 6),
                )
                for sym, bar in bars.items()
            ],
        )
        out.append(snapshot)
    return out


def test_merge_two_symbols_with_gaps_preserves_gaps(tmp_path: Path) -> None:
    _write_legacy_manifest(tmp_path, ["AAA", "BBB"])
    _write_symbol_parquet(
        tmp_path,
        "AAA",
        [
            ("2024-01-01T00:00:00Z", 10, 11, 9, 10.5, 100),
            ("2024-01-01T00:02:00Z", 11, 12, 10, 11.5, 120),
        ],
    )
    _write_symbol_parquet(
        tmp_path,
        "BBB",
        [
            ("2024-01-01T00:00:00Z", 20, 21, 19, 20.5, 200),
            ("2024-01-01T00:01:00Z", 21, 22, 20, 21.5, 210),
            ("2024-01-01T00:02:00Z", 22, 23, 21, 22.5, 220),
        ],
    )

    manifest = load_dataset_manifest(str(tmp_path))
    feed = StreamingHistoricalDataFeed(str(tmp_path), manifest, config={})

    ticks = _collect(feed)
    assert len(ticks) == 3
    assert ticks[0][0] == pd.Timestamp("2024-01-01T00:00:00Z")
    assert ticks[0][1] == ["AAA", "BBB"]
    assert ticks[1][0] == pd.Timestamp("2024-01-01T00:01:00Z")
    assert ticks[1][1] == ["BBB"]
    assert ticks[2][0] == pd.Timestamp("2024-01-01T00:02:00Z")
    assert ticks[2][1] == ["AAA", "BBB"]


def test_global_timestamp_ordering_is_monotonic(tmp_path: Path) -> None:
    _write_legacy_manifest(tmp_path, ["AAA", "BBB"])
    _write_symbol_parquet(
        tmp_path,
        "AAA",
        [
            ("2024-01-01T00:01:00Z", 10, 11, 9, 10.5, 100),
            ("2024-01-01T00:03:00Z", 11, 12, 10, 11.5, 120),
        ],
    )
    _write_symbol_parquet(
        tmp_path,
        "BBB",
        [
            ("2024-01-01T00:00:00Z", 20, 21, 19, 20.5, 200),
            ("2024-01-01T00:02:00Z", 21, 22, 20, 21.5, 210),
        ],
    )

    manifest = load_dataset_manifest(str(tmp_path))
    feed = StreamingHistoricalDataFeed(str(tmp_path), manifest, config={})
    emitted = [entry[0] for entry in _collect(feed)]

    assert emitted == sorted(emitted)
    assert all(emitted[i] < emitted[i + 1] for i in range(len(emitted) - 1))


def test_deterministic_emission_order_same_ts(tmp_path: Path) -> None:
    _write_legacy_manifest(tmp_path, ["AAA", "BBB"])
    _write_symbol_parquet(
        tmp_path,
        "AAA",
        [
            ("2024-01-01T00:00:00Z", 10, 11, 9, 10.5, 100),
            ("2024-01-01T00:01:00Z", 11, 12, 10, 11.5, 120),
        ],
    )
    _write_symbol_parquet(
        tmp_path,
        "BBB",
        [
            ("2024-01-01T00:00:00Z", 20, 21, 19, 20.5, 200),
            ("2024-01-01T00:01:00Z", 21, 22, 20, 21.5, 210),
        ],
    )

    manifest = load_dataset_manifest(str(tmp_path))
    feed_a = StreamingHistoricalDataFeed(str(tmp_path), manifest, config={})
    run_a = _collect(feed_a)

    feed_b = StreamingHistoricalDataFeed(str(tmp_path), manifest, config={})
    run_b = _collect(feed_b)

    assert run_a == run_b
    assert run_a[0][1] == manifest.symbols
    assert run_a[1][1] == manifest.symbols


def test_empty_symbol_file_results_in_empty_feed_or_skips_symbol(tmp_path: Path) -> None:
    _write_legacy_manifest(tmp_path, ["AAA", "BBB"])
    _write_symbol_parquet(tmp_path, "AAA", [])
    _write_symbol_parquet(
        tmp_path,
        "BBB",
        [("2024-01-01T00:00:00Z", 20, 21, 19, 20.5, 200)],
    )

    manifest = load_dataset_manifest(str(tmp_path))
    feed = StreamingHistoricalDataFeed(str(tmp_path), manifest, config={})

    ticks = _collect(feed)
    assert ticks == [
        (
            pd.Timestamp("2024-01-01T00:00:00Z"),
            ["BBB"],
            [("BBB", 20.0, 21.0, 19.0, 20.5, 200.0)],
        )
    ]
