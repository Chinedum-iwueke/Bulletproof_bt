"""Tests for stress suite execution."""
from __future__ import annotations

from pathlib import Path
import math

import pandas as pd

from bt.validation.stress import StressScenario, run_stress_suite


def _write_bars_csv(path: Path, n_bars: int = 50) -> None:
    ts_index = pd.date_range("2024-01-01", periods=n_bars, freq="D", tz="UTC")
    rows = []
    for idx, ts in enumerate(ts_index):
        base = 100 + idx
        rows.append(
            {
                "ts": ts,
                "symbol": "AAA",
                "open": base,
                "high": base + 1,
                "low": base - 1,
                "close": base + 0.5,
                "volume": 1000.0 + idx,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _base_config() -> dict[str, float | int | None]:
    return {
        "min_history_bars": 1,
        "lookback_bars": 1,
        "min_avg_volume": 0.0,
        "lag_bars": 0,
        "seed": 101,
        "p_trade": 0.0,
        "cooldown_bars": 0,
        "max_positions": 1,
        "risk_per_trade_pct": 0.0001,
        "max_notional_per_symbol": None,
        "maker_fee_bps": 1.0,
        "taker_fee_bps": 2.0,
        "slippage_k": 0.01,
        "atr_pct_cap": 0.20,
        "impact_cap": 0.05,
        "signal_delay_bars": 1,
        "initial_cash": 100000.0,
        "max_leverage": 5.0,
    }


def test_stress_suite_outputs_and_summary(tmp_path: Path) -> None:
    data_path = tmp_path / "bars.csv"
    _write_bars_csv(data_path)

    scenarios = [
        StressScenario(name="baseline"),
        StressScenario(name="harsh_costs", fee_mult=2.0, slippage_mult=2.0, seed_offset=1),
        StressScenario(
            name="delay_and_drop",
            add_delay_bars=1,
            drop_fill_prob=0.1,
            seed_offset=2,
        ),
    ]

    output_root = tmp_path / "runs_one"
    result = run_stress_suite(
        data_path=data_path,
        config=_base_config(),
        scenarios=scenarios,
        output_root=output_root,
        seed=123,
    )

    assert "summary" in result
    assert "results" in result
    assert len(result["results"]) == len(scenarios)
    assert set(result["summary"].keys()) == {scenario.name for scenario in scenarios}

    output_root_second = tmp_path / "runs_two"
    repeat = run_stress_suite(
        data_path=data_path,
        config=_base_config(),
        scenarios=scenarios,
        output_root=output_root_second,
        seed=123,
    )

    for scenario in scenarios:
        left = result["summary"][scenario.name]
        right = repeat["summary"][scenario.name]
        assert left["final_equity"] == right["final_equity"]
        assert left["n_trades"] == right["n_trades"]
        assert left["n_fills"] == right["n_fills"]


def test_stress_suite_invariants(tmp_path: Path) -> None:
    data_path = tmp_path / "bars.csv"
    _write_bars_csv(data_path)

    scenarios = [
        StressScenario(name="baseline"),
        StressScenario(name="delay_and_drop", add_delay_bars=1, drop_fill_prob=0.2),
    ]

    result = run_stress_suite(
        data_path=data_path,
        config=_base_config(),
        scenarios=scenarios,
        output_root=tmp_path / "runs",
        seed=77,
    )

    for scenario in scenarios:
        run_dir = Path(result["summary"][scenario.name]["run_dir"])
        equity_path = run_dir / "equity.csv"
        equity_df = pd.read_csv(equity_path)
        assert "equity" in equity_df.columns
        for value in equity_df["equity"].tolist():
            assert value == value
            assert math.isfinite(value)
