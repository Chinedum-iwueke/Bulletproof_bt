from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

from bt.research_orchestration.data_profiles import (
    preflight_research_data_profile,
    resolve_data_profile,
    write_data_profile_config,
)
from bt.experiments.parallel_grid import resolve_parallel_grid_data_args
from bt.data.research_panel_loader import load_volatile_research_panel
from orchestrator.research_daemon import build_pipeline_command, build_research_memory_command, merge_payload_with_defaults


def _panel(root: Path, symbol: str, rows: int = 2) -> None:
    ts = pd.date_range(pd.Timestamp("2021-01-01 00:00", tz="UTC"), periods=rows, freq="1min")
    df = pd.DataFrame(
        {
            "ts": ts,
            "exchange": "binance",
            "symbol": symbol,
            "canonical_symbol": [symbol.removesuffix("USDT") + "-USDT-PERP"] * rows,
            "open": [1.0] * rows,
            "high": [1.0] * rows,
            "low": [1.0] * rows,
            "close": [1.0] * rows,
            "volume": [1.0] * rows,
            "funding_source_ts": ts,
            "oi_source_ts": ts,
        }
    )
    path = root / "canonical" / "binance" / symbol / "timeframe=1m" / "research_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _stable_manifest(root: Path, symbols: list[str]) -> Path:
    path = root / "manifests" / "stable_universe.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {"exchange": ["binance"] * len(symbols), "native_symbol": symbols, "available": [True] * len(symbols)}
    ).to_parquet(path, index=False)
    return path


def _volatile_manifest(root: Path) -> Path:
    path = root / "manifests" / "volatile_universe_membership.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                pd.Timestamp("2021-01-01 00:01", tz="UTC"),
            ],
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
        }
    ).to_parquet(path, index=False)
    return path


def test_old_data_path_mode_still_resolves_exact_path(tmp_path: Path) -> None:
    data = tmp_path / "legacy_curated"
    data.mkdir()
    args = Namespace(
        data=str(data),
        data_root=None,
        data_kind=None,
        exchange="binance",
        universe="stable",
        timeframe="1m",
        membership_path=None,
        stable_manifest=None,
    )

    resolved, overrides = resolve_parallel_grid_data_args(args, tmp_path / "exp")

    assert resolved == data
    assert overrides == []


def test_new_stable_research_panel_mode_resolves_and_writes_override(tmp_path: Path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT")
    stable_manifest = _stable_manifest(root, ["BTCUSDT"])
    args = Namespace(
        data=None,
        data_root=str(root),
        data_kind="research_panel",
        exchange="binance",
        universe="stable",
        timeframe="1m",
        membership_path=None,
        stable_manifest=str(stable_manifest),
    )

    resolved, overrides = resolve_parallel_grid_data_args(args, tmp_path / "exp")

    assert resolved == root
    assert len(overrides) == 1
    text = overrides[0].read_text()
    assert "dataset_kind: research_panel" in text
    assert "universe: stable" in text


def test_volatile_mode_loads_membership_and_filters_inactive_symbols(tmp_path: Path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", rows=3)
    _panel(root, "ETHUSDT", rows=3)
    membership = _volatile_manifest(root)
    profile = resolve_data_profile(
        universe="volatile",
        data_root=root,
        membership_path=membership,
    )
    preflight_research_data_profile(profile)

    loaded = load_volatile_research_panel(root, "binance", membership, "1m")

    assert loaded.loc[loaded["ts"].eq(pd.Timestamp("2021-01-01 00:00", tz="UTC")), "symbol"].tolist() == ["BTCUSDT"]
    assert loaded.loc[loaded["ts"].eq(pd.Timestamp("2021-01-01 00:01", tz="UTC")), "symbol"].tolist() == ["ETHUSDT"]


def test_missing_research_data_files_fail_loudly(tmp_path: Path) -> None:
    root = tmp_path / "research_data"
    stable_manifest = _stable_manifest(root, ["BTCUSDT"])
    profile = resolve_data_profile(universe="stable", data_root=root, stable_manifest=stable_manifest)

    with pytest.raises(FileNotFoundError, match="research panel missing"):
        preflight_research_data_profile(profile)


def test_no_lookahead_preflight_rejects_future_source_ts(tmp_path: Path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT")
    stable_manifest = _stable_manifest(root, ["BTCUSDT"])
    path = root / "canonical" / "binance" / "BTCUSDT" / "timeframe=1m" / "research_panel.parquet"
    df = pd.read_parquet(path)
    df.loc[0, "funding_source_ts"] = pd.Timestamp("2021-01-01 00:01", tz="UTC")
    df.to_parquet(path, index=False)
    profile = resolve_data_profile(universe="stable", data_root=root, stable_manifest=stable_manifest)

    with pytest.raises(ValueError, match="funding_source_ts"):
        preflight_research_data_profile(profile)


def test_daemon_builds_research_data_profile_pipeline_command(tmp_path: Path) -> None:
    payload = {
        "hypothesis": "research/hypotheses/example.yaml",
        "name": "l1_h7c",
    }
    config = {
        "data_root": "research_data",
        "data_kind": "research_panel",
        "exchange": "binance",
        "timeframe": "1m",
    }
    merged = merge_payload_with_defaults(payload, config, cli_max_workers=6)
    cmd = build_pipeline_command(tmp_path / "research.sqlite", merged)

    assert "--data-root" in cmd
    assert "research_data" in cmd
    assert "--data-kind" in cmd
    assert "research_panel" in cmd
    assert "/home/omenka/research_data/bt/curated/stable_data_1m_canonical" not in cmd
    assert "--stable-data" not in cmd
    assert "--membership-path" in cmd
    assert "research_data/manifests/volatile_universe_membership.parquet" in cmd


def test_daemon_builds_research_memory_command_after_pipeline(tmp_path: Path) -> None:
    payload = {
        "hypothesis": "research/hypotheses/example.yaml",
        "name": "l1_h7c",
        "phase": "tier2",
        "outputs_root": "outputs",
    }
    merged = merge_payload_with_defaults(payload, {}, cli_max_workers=6)
    cmd = build_research_memory_command(tmp_path / "research.sqlite", merged, {})

    assert "orchestrator/research_memory.py" in " ".join(cmd)
    assert "--write-db" in cmd
    assert "--outputs-root" in cmd
    assert "outputs" in cmd
    assert "--verdicts-dir" in cmd
    assert "research/verdicts/tier2" in cmd
    assert "--state-findings-dir" in cmd
    assert "research/state_findings/tier2" in cmd


def test_daemon_ignores_stale_curated_paths_in_research_panel_mode(tmp_path: Path) -> None:
    payload = {
        "hypothesis": "research/hypotheses/example.yaml",
        "name": "l1_h7c",
        "stable_data": "/home/omenka/research_data/bt/curated/stable_data_1m_canonical",
        "vol_data": "/home/omenka/research_data/bt/curated/vol_data_1m_canonical",
    }
    config = {
        "data_mode": "research_panel",
        "data_root": "research_data",
        "data_kind": "research_panel",
        "exchange": "binance",
        "timeframe": "1m",
    }
    merged = merge_payload_with_defaults(payload, config, cli_max_workers=6)
    cmd = build_pipeline_command(tmp_path / "research.sqlite", merged)

    assert merged["stable_data"] is None
    assert merged["vol_data"] is None
    assert "--stable-data" not in cmd
    assert "--vol-data" not in cmd
    assert "--data-root" in cmd
    assert "research_data" in cmd


def test_daemon_legacy_curated_mode_requires_explicit_opt_in(tmp_path: Path) -> None:
    payload = {
        "hypothesis": "research/hypotheses/example.yaml",
        "name": "l1_h7c",
        "data_mode": "legacy_curated",
        "stable_data": "/home/omenka/research_data/bt/curated/stable_data_1m_canonical",
        "vol_data": "/home/omenka/research_data/bt/curated/vol_data_1m_canonical",
    }
    merged = merge_payload_with_defaults(payload, {}, cli_max_workers=6)
    cmd = build_pipeline_command(tmp_path / "research.sqlite", merged)

    assert "--stable-data" in cmd
    assert "/home/omenka/research_data/bt/curated/stable_data_1m_canonical" in cmd
    assert "--vol-data" in cmd
    assert "/home/omenka/research_data/bt/curated/vol_data_1m_canonical" in cmd


def test_data_profile_config_contains_expected_mapping(tmp_path: Path) -> None:
    profile = resolve_data_profile(universe="stable", data_root="research_data")
    path = write_data_profile_config(profile, tmp_path / "data.yaml")

    assert "stable_manifest" in path.read_text()
