from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.analysis.ev_by_bucket import analyze_structural_buckets, write_structural_bucket_artifacts
from orchestrator.state_discovery.finding_ranker import classify_and_rank_findings
from orchestrator.state_discovery.state_bucket_analyzer import analyze_single_state_variables


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "r_net": [0.5, 0.4, -0.2, 0.8, 0.7, -0.1],
            "entry_state_csi_pctile": [0.8, 0.82, 0.2, 0.9, 0.88, 0.3],
            "entry_state_vol_pctile": [0.6] * 6,
            "entry_state_tr_over_atr": [1.2] * 6,
            "entry_state_tr_over_atr_pctile": [0.7] * 6,
            "entry_state_spread_proxy_pctile": [0.4] * 6,
            "entry_state_funding_pctile": [0.95, 0.9, 0.2, 0.92, 0.91, 0.1],
            "entry_state_oi_accel_pctile": [0.9, 0.85, 0.2, 0.95, 0.8, 0.1],
            "entry_state_basis_pctile": [0.8, 0.82, 0.3, 0.88, 0.86, 0.2],
        }
    )


def test_structural_buckets_write_rich_outputs(tmp_path: Path) -> None:
    result = analyze_structural_buckets(_trades(), min_trades=1)
    paths = write_structural_bucket_artifacts(result, tmp_path)

    assert "ev_by_bucket_funding" in paths
    assert "ev_by_bucket_oi_accel" in paths
    assert "ev_by_bucket_basis" in paths
    assert (tmp_path / "ev_by_bucket_joint_csi_funding.csv").exists()


def test_state_discovery_identifies_rich_state_findings() -> None:
    metrics, missing = analyze_single_state_variables(_trades(), min_bucket_trades=1)
    ranked = classify_and_rank_findings(metrics, min_trades=1, include_negative_findings=True)

    assert "entry_state_funding_pctile" not in missing
    assert ranked["state_variable"].str.contains("funding|oi|basis", regex=True).any()
    assert ranked["finding_type"].str.contains("FUNDING|OI|BASIS", regex=True).any()


def test_state_discovery_handles_duplicate_quantile_edges() -> None:
    trades = pd.DataFrame(
        {
            "r_net": [0.1, 0.2, -0.1, 0.3],
            "entry_state_oi_level": [0.0001, 0.0001, 0.0001, 0.0002],
        }
    )

    metrics, missing = analyze_single_state_variables(trades, min_bucket_trades=1)

    assert "entry_state_oi_level" not in missing
    assert not metrics.empty
