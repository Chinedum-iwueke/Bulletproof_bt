from __future__ import annotations

from pathlib import Path

import yaml

from bt.api import run_backtest
from bt.strategy.context_view import FrozenDict, StrategyContextView


def test_ctx_write_raises_typeerror() -> None:
    ctx = StrategyContextView({"a": 1, "nested": {"b": 2}, "lst": [1, {"c": 3}]})
    try:
        ctx["a"] = 2
        assert False, "expected TypeError"
    except TypeError as e:
        assert "read-only" in str(e).lower()


def test_ctx_nested_write_raises() -> None:
    ctx = StrategyContextView({"nested": {"b": 2}})
    nested = ctx["nested"]
    try:
        nested["b"] = 9
        assert False, "expected TypeError"
    except TypeError as e:
        assert "read-only" in str(e).lower()


def test_ctx_list_is_tuple_and_nested_dict_frozen() -> None:
    ctx = StrategyContextView({"lst": [1, {"c": 3}]})
    lst = ctx["lst"]
    assert isinstance(lst, tuple)
    assert lst[0] == 1
    assert isinstance(lst[1], dict) is False
    assert isinstance(lst[1], FrozenDict)


def test_existing_strategies_still_run_smoke(tmp_path: Path) -> None:
    run_coinflip = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "coinflip_out"),
            run_name="coinflip_smoke",
        )
    )
    assert (run_coinflip / "performance.json").exists()

    htf_cfg = {
        "initial_cash": 100000.0,
        "max_leverage": 2.0,
        "min_history_bars": 1,
        "lookback_bars": 1,
        "min_avg_volume": 0.0,
        "lag_bars": 0,
        "signal_delay_bars": 1,
        "risk": {"mode": "r_fixed", "r_per_trade": 0.005},
        "strategy": {
            "name": "volfloor_donchian",
            "timeframe": "1h",
            "donchian_entry_lookback": 5,
            "donchian_exit_lookback": 3,
            "adx_min": 1.0,
            "vol_floor_pct": 0.0,
            "atr_period": 3,
            "vol_lookback_bars": 5,
        },
        "htf_resampler": {"timeframes": ["1h"], "strict": True},
    }
    cfg_path = tmp_path / "engine_volfloor.yaml"
    cfg_path.write_text(yaml.safe_dump(htf_cfg, sort_keys=False), encoding="utf-8")

    run_volfloor = Path(
        run_backtest(
            config_path=str(cfg_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "volfloor_out"),
            run_name="volfloor_smoke",
        )
    )
    assert (run_volfloor / "performance.json").exists()
