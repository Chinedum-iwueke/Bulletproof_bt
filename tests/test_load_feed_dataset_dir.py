from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.data.load_feed import load_feed


def _write_dataset_dir(dataset_dir: Path) -> None:
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.yaml").write_text(
        "format: per_symbol_parquet\n"
        "symbols: [AAA, BBB]\n"
        'path: "symbols/{symbol}.parquet"\n',
        encoding="utf-8",
    )

    for symbol, base in (("AAA", 1.0), ("BBB", 2.0)):
        frame = pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp("2024-01-01T00:00:00Z"),
                    "open": base,
                    "high": base + 0.1,
                    "low": base - 0.1,
                    "close": base + 0.05,
                    "volume": 100.0,
                    "symbol": symbol,
                }
            ]
        )
        frame.to_parquet(symbols_dir / f"{symbol}.parquet", index=False)


def test_dataset_dir_default_returns_streaming_feed(tmp_path: Path) -> None:
    ds_dir = tmp_path / "ds"
    ds_dir.mkdir()
    _write_dataset_dir(ds_dir)

    feed = load_feed(str(ds_dir), {})

    assert feed is not None
    assert feed.peek_time() is not None
    bars = feed.next()
    assert bars is not None
    assert len(bars) >= 1


def test_dataset_dir_missing_manifest_raises_valueerror(tmp_path: Path) -> None:
    ds_dir = tmp_path / "ds"
    ds_dir.mkdir()

    with pytest.raises(ValueError, match="manifest.yaml") as exc:
        load_feed(str(ds_dir), {})

    assert str(ds_dir) in str(exc.value)


def test_dataset_dir_dataframe_mode_is_notimplemented(tmp_path: Path) -> None:
    ds_dir = tmp_path / "ds"
    ds_dir.mkdir()
    _write_dataset_dir(ds_dir)

    with pytest.raises(NotImplementedError, match="Dataset directories are not supported in dataframe mode"):
        load_feed(str(ds_dir), {"data": {"mode": "dataframe"}})


def test_single_file_still_uses_legacy_dataframe_path(tmp_path: Path) -> None:
    csv_path = tmp_path / "bars.csv"
    pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.2,
                "low": 0.9,
                "close": 1.1,
                "volume": 10.0,
            },
            {
                "ts": pd.Timestamp("2025-01-01 00:01:00", tz="UTC"),
                "symbol": "AAA",
                "open": 1.1,
                "high": 1.3,
                "low": 1.0,
                "close": 1.2,
                "volume": 11.0,
            },
        ]
    ).to_csv(csv_path, index=False)

    feed = load_feed(str(csv_path), {})
    batch = feed.next()

    assert batch is not None
    assert len(batch) == 1
    bar = batch[0]
    assert bar.ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    assert bar.open == pytest.approx(1.0)
    assert bar.high == pytest.approx(1.2)
    assert bar.low == pytest.approx(0.9)
    assert bar.close == pytest.approx(1.1)
    assert bar.volume == pytest.approx(10.0)
