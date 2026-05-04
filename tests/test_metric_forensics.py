import json
from pathlib import Path
from orchestrator.forensics.audit_run_metrics import audit_run

def test_contradiction_detected(tmp_path: Path):
    run=tmp_path/'r1'; run.mkdir()
    (run/'performance.json').write_text(json.dumps({'initial_equity':100000,'final_equity':79217,'net_pnl':0.0,'gross_pnl':5.0,'ev_r_net':4840.339,'win_rate_r':1.0,'ev_by_bucket':{'all':0.0},'costs':{'fees_total':2719,'slippage_total':1359,'spread_total':453,'commission_total':0}}))
    (run/'trades.csv').write_text('pnl_net,r_multiple_net,risk_amount\n-10,0.2,100\n-20,-0.4,100\n')
    out=audit_run(run)
    assert out['status']=='FAILED'
    assert 'equity_vs_net_pnl_mismatch' in out['errors']
    assert 'win_rate_r_mismatch' in out['errors']
