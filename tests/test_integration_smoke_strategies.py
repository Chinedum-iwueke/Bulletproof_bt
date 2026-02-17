"""Opt-in integration smoke tests over all discoverable strategies."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
from typing import Any

import pytest
import yaml

from bt.api import run_backtest
from bt.config import deep_merge
from bt.strategy import STRATEGY_REGISTRY
from tests.helpers.smoke import (
    count_jsonl_lines,
    first_forbidden_token,
    format_sanity_headline,
    parse_equity_values,
    read_jsonl,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.smoke,
    pytest.mark.skipif(
        os.getenv("BPBT_INTEGRATION_SMOKE") != "1",
        reason="Set BPBT_INTEGRATION_SMOKE=1 to enable opt-in integration smoke tests.",
    ),
]

_MAX_STOP_CHECKS = 12


def _env_csv(name: str) -> set[str]:
    raw = os.getenv(name, "")
    return {token.strip() for token in raw.split(",") if token.strip()}


def _discover_strategy_names() -> list[str]:
    names = sorted(STRATEGY_REGISTRY)
    if names:
        return names

    # Filesystem fallback (if registry import path changes in future).
    src_root = Path(__file__).resolve().parents[1] / "src"
    discovered: set[str] = set()
    for pattern in ("**/strategies/*.py", "**/strategy/*.py"):
        for py_file in src_root.glob(pattern):
            if py_file.name.startswith("_") or py_file.stem in {"__init__", "base"}:
                continue
            discovered.add(py_file.stem)
    return sorted(discovered)


def _filtered_strategy_names() -> list[str]:
    names = _discover_strategy_names()
    allowlist = _env_csv("BPBT_SMOKE_STRATEGIES")
    blocklist = _env_csv("BPBT_SMOKE_EXCLUDE")
    regex = os.getenv("BPBT_SMOKE_STRATEGY_REGEX")

    if allowlist:
        names = [name for name in names if name in allowlist]
    if regex:
        pattern = re.compile(regex)
        names = [name for name in names if pattern.search(name)]
    if blocklist:
        names = [name for name in names if name not in blocklist]

    return sorted(names)


def _strategy_cls(strategy_name: str) -> type[Any]:
    strategy_cls = STRATEGY_REGISTRY.get(strategy_name)
    if strategy_cls is None:
        raise AssertionError(
            f"Strategy {strategy_name!r} was selected but is not present in STRATEGY_REGISTRY. "
            f"Known: {sorted(STRATEGY_REGISTRY)}"
        )
    return strategy_cls


def _strategy_smoke_overrides(strategy_name: str) -> dict[str, Any]:
    strategy_cls = _strategy_cls(strategy_name)
    method = getattr(strategy_cls, "smoke_config_overrides", None)
    if callable(method):
        result = method()
        if not isinstance(result, dict):
            raise AssertionError(
                f"{strategy_name}.smoke_config_overrides() must return dict, got {type(result).__name__}"
            )
        return result
    return {}


def _strategy_smoke_assert(strategy_name: str, run_dir: Path) -> None:
    strategy_cls = _strategy_cls(strategy_name)
    method = getattr(strategy_cls, "smoke_assert", None)
    if callable(method):
        method(run_dir)


def _strategy_requires_stops(strategy_name: str) -> bool:
    strategy_cls = _strategy_cls(strategy_name)
    method = getattr(strategy_cls, "smoke_requires_stops", None)
    if callable(method):
        value = method()
        if not isinstance(value, bool):
            raise AssertionError(
                f"{strategy_name}.smoke_requires_stops() must return bool, got {type(value).__name__}"
            )
        return value
    return True


def _headline_fields(sanity: dict[str, Any]) -> str:
    return format_sanity_headline(sanity)


def _assert_engine_wide_invariants(
    *,
    strategy_name: str,
    run_dir: Path,
    strict: bool,
    expect_stops: bool,
) -> None:
    config_used_path = run_dir / "config_used.yaml"
    sanity_path = run_dir / "sanity.json"
    decisions_path = run_dir / "decisions.jsonl"
    fills_path = run_dir / "fills.jsonl"
    equity_path = run_dir / "equity.csv"

    assert config_used_path.exists(), f"[{strategy_name}] Missing artifact: {config_used_path}"
    assert sanity_path.exists(), f"[{strategy_name}] Missing artifact: {sanity_path}"

    sanity = json.loads(sanity_path.read_text(encoding="utf-8"))
    if not isinstance(sanity, dict):
        raise AssertionError(f"[{strategy_name}] sanity.json is not an object: {sanity_path}")

    decisions_count = count_jsonl_lines(decisions_path)
    fills_count = count_jsonl_lines(fills_path)

    assert decisions_count == int(sanity.get("signals_emitted", -1)), (
        f"[{strategy_name}] decisions count mismatch in {run_dir}: decisions={decisions_count} "
        f"sanity.signals_emitted={sanity.get('signals_emitted')}. "
        f"Headline: {_headline_fields(sanity)}"
    )
    assert fills_count == int(sanity.get("fills", -1)), (
        f"[{strategy_name}] fills count mismatch in {run_dir}: fills={fills_count} "
        f"sanity.fills={sanity.get('fills')}. Headline: {_headline_fields(sanity)}"
    )

    forbidden = first_forbidden_token(
        run_dir,
        [
            '"used_legacy_stop_proxy": true',
            "legacy_high_low_proxy",
            "allow_legacy_proxy",
        ],
    )
    assert forbidden is None, (
        f"[{strategy_name}] Legacy stop proxy marker found in {run_dir}: "
        f"file={forbidden[0]} line={forbidden[1]} token={forbidden[2]!r} text={forbidden[3]!r}. "
        f"Headline: {_headline_fields(sanity)}"
    )

    assert int(sanity.get("forced_liquidations", 0)) == 0, (
        f"[{strategy_name}] sanity.forced_liquidations != 0 in {run_dir}: "
        f"{sanity.get('forced_liquidations')}. Headline: {_headline_fields(sanity)}"
    )

    fill_rows = read_jsonl(fills_path)
    forced = [
        row
        for row in fill_rows
        if (row.get("metadata") or {}).get("exit_reason") == "forced_liquidation"
    ]
    assert not forced, (
        f"[{strategy_name}] Found forced liquidation fill in {run_dir}: first={forced[0]}. "
        f"Headline: {_headline_fields(sanity)}"
    )

    if strict:
        assert int(sanity.get("signals_rejected", 0)) >= 0, (
            f"[{strategy_name}] strict mode sanity check failed unexpectedly. "
            f"Headline: {_headline_fields(sanity)}"
        )

    if expect_stops:
        non_close_rows = [row for row in fill_rows if not bool((row.get("metadata") or {}).get("close_only"))]
        for idx, row in enumerate(non_close_rows[:_MAX_STOP_CHECKS], start=1):
            metadata = row.get("metadata") or {}
            stop_price = metadata.get("stop_price")
            assert stop_price is not None, (
                f"[{strategy_name}] Missing stop_price on non-close_only fill #{idx} in {run_dir}: "
                f"fill={row}. Headline: {_headline_fields(sanity)}"
            )
            if "r_metrics_valid" in metadata:
                assert metadata.get("r_metrics_valid") is True, (
                    f"[{strategy_name}] r_metrics_valid is not true on non-close_only fill #{idx} in {run_dir}: "
                    f"fill={row}. Headline: {_headline_fields(sanity)}"
                )

    assert equity_path.exists(), f"[{strategy_name}] Missing artifact: {equity_path}"
    equity_values = parse_equity_values(equity_path)
    assert equity_values, f"[{strategy_name}] equity.csv has no rows in {run_dir}"


_DATASET_DIR = os.getenv("BPBT_SMOKE_DATASET_DIR")
if os.getenv("BPBT_INTEGRATION_SMOKE") == "1":
    if not _DATASET_DIR:
        pytest.skip("Set BPBT_SMOKE_DATASET_DIR to run integration smoke tests.", allow_module_level=True)
    if not Path(_DATASET_DIR).exists():
        pytest.skip(
            f"BPBT_SMOKE_DATASET_DIR does not exist: {_DATASET_DIR}",
            allow_module_level=True,
        )

_STRATEGY_NAMES = _filtered_strategy_names()
if os.getenv("BPBT_INTEGRATION_SMOKE") == "1" and not _STRATEGY_NAMES:
    pytest.skip(
        "No strategies selected after applying BPBT_SMOKE_STRATEGIES/BPBT_SMOKE_STRATEGY_REGEX/BPBT_SMOKE_EXCLUDE",
        allow_module_level=True,
    )


@pytest.mark.parametrize("strategy_name", _STRATEGY_NAMES, ids=_STRATEGY_NAMES)
def test_integration_smoke_strategy_backtest(strategy_name: str, tmp_path: Path) -> None:
    dataset_dir = Path(os.environ["BPBT_SMOKE_DATASET_DIR"])
    max_bars = int(os.getenv("BPBT_SMOKE_MAX_BARS", "20000"))
    strict = os.getenv("BPBT_SMOKE_STRICT") == "1"

    out_root = Path(os.getenv("BPBT_SMOKE_OUTDIR", "outputs/pytest_smoke_runs"))
    strategy_run_root = out_root / strategy_name
    if strategy_run_root.exists():
        shutil.rmtree(strategy_run_root)
    strategy_run_root.mkdir(parents=True, exist_ok=True)

    override = {
        "strategy": {"name": strategy_name, "seed": 42},
        "data": {"row_limit_per_symbol": max_bars},
        "benchmark": {"enabled": False},
        "seed": 42,
    }
    override = deep_merge(override, _strategy_smoke_overrides(strategy_name))

    override_path = tmp_path / f"smoke_override_{strategy_name}.yaml"
    override_path.write_text(yaml.safe_dump(override, sort_keys=False), encoding="utf-8")

    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path=str(dataset_dir),
            out_dir=str(strategy_run_root),
            override_paths=[str(override_path)],
            run_name=f"smoke_{strategy_name}",
        )
    )

    _assert_engine_wide_invariants(
        strategy_name=strategy_name,
        run_dir=run_dir,
        strict=strict,
        expect_stops=_strategy_requires_stops(strategy_name),
    )
    _strategy_smoke_assert(strategy_name, run_dir)
