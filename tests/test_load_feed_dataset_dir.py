from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.data.load_feed import load_feed


def _create_dataset_dir(tmp_path: Path) -> Path:
    ds_dir = tmp_path / "ds"
    symbols_dir = ds_dir / "symbols"
    symbols_dir.mkdir(parents=True)

    manifest = {
        "format": "per_symbol_parquet",
        "symbols": ["AAA", "BBB"],
        "path": "symbols/{symbol}.parquet",
    }
    (ds_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    aaa = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100.0,
            }
        ]
    )
    bbb = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2025-01-01 00:01:00", tz="UTC"),
                "open": 20.0,
                "high": 22.0,
                "low": 19.0,
                "close": 21.0,
                "volume": 200.0,
            }
        ]
    )
    aaa.to_parquet(symbols_dir / "AAA.parquet", index=False)
    bbb.to_parquet(symbols_dir / "BBB.parquet", index=False)
    return ds_dir


def test_dataset_dir_default_returns_streaming_feed(tmp_path: Path) -> None:
    ds_dir = _create_dataset_dir(tmp_path)

    feed = load_feed(str(ds_dir), {})

    assert feed is not None
    first_batch = feed.next()
    assert first_batch is not None
    assert len(first_batch) >= 1


def test_dataset_dir_missing_manifest_raises_valueerror(tmp_path: Path) -> None:
    ds_dir = tmp_path / "ds"
    ds_dir.mkdir()

    with pytest.raises(ValueError, match="manifest.yaml") as exc_info:
        load_feed(str(ds_dir), {})

    assert str(ds_dir) in str(exc_info.value)


def test_dataset_dir_dataframe_mode_is_notimplemented(tmp_path: Path) -> None:
    ds_dir = _create_dataset_dir(tmp_path)

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

    first_batch = feed.next()
    assert first_batch is not None
    first_bar = first_batch[0]
    assert first_bar.ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    assert first_bar.open == pytest.approx(1.0)
    assert first_bar.close == pytest.approx(1.1)
