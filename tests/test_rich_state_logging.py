from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.experiments.dataset_builder import extract_experiment_dataset
from bt.logging.trade_enrichment import enrich_trade_row


def test_trade_enrichment_preserves_rich_entry_state_fields() -> None:
    row = {
        "r_net": 1.0,
        "entry_state_funding_raw": 0.001,
        "entry_state_oi_accel_pctile": 0.9,
        "entry_state_basis_pctile": 0.8,
        "entry_state_csi_source": "enriched",
    }

    enriched = enrich_trade_row(row)

    assert enriched["entry_state_funding_raw"] == 0.001
    assert enriched["entry_state_oi_accel_pctile"] == 0.9
    assert enriched["entry_state_csi_source"] == "enriched"


def test_extract_experiment_dataset_preserves_rich_state_fields(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    run = exp / "runs" / "run_1"
    run.mkdir(parents=True)
    (run / "config_used.yaml").write_text("strategy:\n  name: fixture\n", encoding="utf-8")
    (run / "performance.json").write_text(json.dumps({"net_pnl": 1.0, "trade_count": 1}), encoding="utf-8")
    pd.DataFrame(
        [
            {
                "entry_ts": "2025-01-01T00:00:00Z",
                "exit_ts": "2025-01-01T00:05:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "qty": 1,
                "entry_price": 100,
                "exit_price": 101,
                "pnl": 1,
                "pnl_net": 1,
                "r_multiple_net": 1,
                "entry_state_funding_raw": 0.001,
                "entry_state_oi_accel_pctile": 0.9,
                "entry_state_basis_pctile": 0.8,
                "entry_state_csi_source": "enriched",
            }
        ]
    ).to_csv(run / "trades.csv", index=False)

    extract_experiment_dataset(experiment_root=exp, overwrite=True)
    out = pd.read_parquet(exp / "research_data" / "trades_dataset.parquet")

    assert "entry_state_funding_raw" in out.columns
    assert "entry_state_oi_accel_pctile" in out.columns
    assert "entry_state_basis_pctile" in out.columns
    assert "entry_state_csi_source" in out.columns
