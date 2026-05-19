from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.experiments.dataset_builder import extract_experiment_dataset


def test_dataset_extraction_preserves_known_enrichment_prefixes(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    run = exp / "runs" / "run_1"
    run.mkdir(parents=True)
    (run / "config_used.yaml").write_text("strategy:\n  name: fixture\n", encoding="utf-8")
    (run / "performance.json").write_text(json.dumps({"net_pnl": 1.0, "trade_count": 1}), encoding="utf-8")
    (run / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    pd.DataFrame(
        [
            {
                "entry_ts": "2025-01-01T00:00:00Z",
                "exit_ts": "2025-01-01T00:10:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "qty": 1.0,
                "entry_price": 100.0,
                "exit_price": 101.0,
                "pnl": 1.0,
                "pnl_price": 1.0,
                "pnl_net": 0.9,
                "r_multiple_net": 0.9,
                "identity_hypothesis_id": "h1",
                "entry_state_funding_rate": 0.0001,
                "entry_decision_reason_code": "fixture",
                "execution_slippage_bps": 2.0,
                "risk_initial_r": 100.0,
                "path_mfe_r": 1.2,
                "counterfactual_hold_3bars_r": 0.4,
                "label_profitable_after_costs": True,
            }
        ]
    ).to_csv(run / "trades.csv", index=False)

    extract_experiment_dataset(experiment_root=exp, overwrite=True)
    trades = pd.read_parquet(exp / "research_data" / "trades_dataset.parquet")

    for column in [
        "identity_hypothesis_id",
        "entry_state_funding_rate",
        "entry_decision_reason_code",
        "execution_slippage_bps",
        "risk_initial_r",
        "path_mfe_r",
        "counterfactual_hold_3bars_r",
        "label_profitable_after_costs",
    ]:
        assert column in trades.columns
