from __future__ import annotations
import json, subprocess, sys
from pathlib import Path
import pandas as pd

def _run(root: Path, run_id: str, df: pd.DataFrame) -> None:
    d=root/"runs"/run_id; d.mkdir(parents=True, exist_ok=True); df.to_csv(d/"trades.csv", index=False)

def test_per_run_outputs_and_flags(tmp_path: Path) -> None:
    root=tmp_path/"exp"
    _run(root,"r1",pd.DataFrame({"run_id":["r1"]*30,"r_net":[0.2]*30,"r_gross":[0.25]*30,"mfe_r":[1.0]*30,"counterfactual_exit_efficiency_realized_over_mfe":[0.2]*30,"cost_drag_r":[0.03]*30,"entry_state_csi_pctile":[0.9]*30,"entry_state_vol_pctile":[0.2]*30,"entry_state_spread_proxy_pctile":[0.1]*30,"entry_state_tr_over_atr":[1.1]*30}))
    (root/"summaries").mkdir(parents=True, exist_ok=True)
    proc=subprocess.run([sys.executable,"scripts/post_run_analysis.py","--experiment-root",str(root)],capture_output=True,text=True)
    assert proc.returncode==0, proc.stderr
    a=root/"runs"/"r1"/"analysis"
    assert (a/"ev_by_bucket.csv").exists()
    payload=json.loads((a/"structural_diagnostics_summary.json").read_text())
    assert payload["recommendation_flags"]["needs_exit_refinement"] is True
    assert (root/"summaries"/"run_structural_summary.csv").exists()

def test_no_trades_and_missing_fields(tmp_path: Path) -> None:
    root=tmp_path/"exp"; (root/"runs"/"r2").mkdir(parents=True)
    _run(root,"r3",pd.DataFrame({"r_net":[0.1,-0.2]}))
    subprocess.run([sys.executable,"scripts/post_run_analysis.py","--experiment-root",str(root)],check=True)
    p2=json.loads((root/"runs"/"r2"/"analysis"/"structural_diagnostics_summary.json").read_text())
    assert p2["status"]=="no_trades"
    p3=json.loads((root/"runs"/"r3"/"analysis"/"structural_diagnostics_summary.json").read_text())
    assert "csi" in p3["missing_fields"]
