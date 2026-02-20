from __future__ import annotations

import json
from pathlib import Path

import yaml

from bt.api import run_backtest


TIER3_PRESET = {
    "maker_fee": 0.0,
    "taker_fee": 0.0008,
    "slippage_bps": 5.0,
    "delay_bars": 1,
    "spread_bps": 3.0,
}


def _write_config(path: Path, *, execution: dict | None = None) -> None:
    config: dict[str, object] = {
        "initial_cash": 100000.0,
        "max_leverage": 5.0,
        "signal_delay_bars": 1,
        "strategy": {"name": "coinflip", "p_trade": 0.0, "cooldown_bars": 0, "seed": 7},
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
        "htf_timeframes": ["15m"],
    }
    if execution is not None:
        config["execution"] = execution
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _load_run_status(run_dir: Path) -> dict[str, object]:
    return json.loads((run_dir / "run_status.json").read_text(encoding="utf-8"))


def _scrub_nondeterministic(payload: dict[str, object]) -> dict[str, object]:
    scrubbed = dict(payload)
    for key in ("run_id", "created_at", "timestamp", "started_at", "finished_at"):
        scrubbed.pop(key, None)
    return scrubbed


def test_run_status_execution_metadata_appears_with_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "engine_defaults.yaml"
    _write_config(config_path)

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name="defaults",
        )
    )
    payload = _load_run_status(run_dir)

    assert "execution_profile" in payload
    assert payload["spread_mode"] == "none"
    assert payload["intrabar_mode"] == "worst_case"

    effective_execution = payload.get("effective_execution")
    assert isinstance(effective_execution, dict)
    for key in ("maker_fee", "taker_fee", "slippage_bps", "delay_bars", "spread_bps"):
        assert key in effective_execution


def test_run_status_execution_metadata_matches_tier3_profile(tmp_path: Path) -> None:
    config_path = tmp_path / "engine_tier3.yaml"
    _write_config(config_path, execution={"profile": "tier3"})

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name="tier3",
        )
    )
    payload = _load_run_status(run_dir)

    assert payload["execution_profile"] == "tier3"
    assert payload["effective_execution"] == TIER3_PRESET
    assert payload["spread_mode"] == "none"
    assert "spread_bps" not in payload


def test_run_status_execution_metadata_with_fixed_bps_spread_includes_spread_bps(tmp_path: Path) -> None:
    config_path = tmp_path / "engine_custom_fixed_spread.yaml"
    _write_config(
        config_path,
        execution={
            "profile": "custom",
            "maker_fee": 0.0,
            "taker_fee": 0.0008,
            "slippage_bps": 5.0,
            "delay_bars": 1,
            "spread_bps": 2.5,
            "spread_mode": "fixed_bps",
        },
    )

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name="custom_fixed_spread",
        )
    )
    payload = _load_run_status(run_dir)
    assert payload["execution_profile"] == "custom"
    assert payload["spread_mode"] == "fixed_bps"
    assert payload["spread_bps"] == 2.5


def test_run_status_tier_fixed_spread_without_explicit_bps_uses_tier_value(tmp_path: Path) -> None:
    config_path = tmp_path / "engine_tier3_fixed_spread.yaml"
    _write_config(config_path, execution={"profile": "tier3", "spread_mode": "fixed_bps"})

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name="tier3_fixed_spread",
        )
    )
    payload = _load_run_status(run_dir)

    assert payload["execution_profile"] == "tier3"
    assert payload["spread_mode"] == "fixed_bps"
    assert payload["spread_bps"] == 3.0


def test_run_status_execution_metadata_is_deterministic(tmp_path: Path) -> None:
    config_path = tmp_path / "engine_deterministic.yaml"
    _write_config(config_path, execution={"profile": "tier3"})

    run_a = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out_a"),
            run_name="deterministic_a",
        )
    )
    run_b = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out_b"),
            run_name="deterministic_b",
        )
    )

    payload_a = _scrub_nondeterministic(_load_run_status(run_a))
    payload_b = _scrub_nondeterministic(_load_run_status(run_b))
    assert payload_a == payload_b
