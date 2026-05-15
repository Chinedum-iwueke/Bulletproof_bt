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


def test_pnl_denominated_ev_by_bucket_all_is_not_a_false_failure(tmp_path: Path):
    run=tmp_path/'r2'; run.mkdir()
    (run/'performance.json').write_text(json.dumps({
        'initial_equity':100000,
        'final_equity':99970,
        'net_pnl':-30.0,
        'gross_pnl':-30.0,
        'ev_net':-15.0,
        'ev_r_net':-0.15,
        'win_rate_r':0.5,
        'ev_by_bucket':{'all':-15.0},
        'costs':{'fees_total':0,'slippage_total':0,'spread_total':0,'commission_total':0},
    }))
    (run/'trades.csv').write_text('pnl_net,r_multiple_net,risk_amount\n-40,-0.4,100\n10,0.1,100\n')
    out=audit_run(run)
    assert out['metrics_valid'] is True
    assert out['status']=='PASSED'
    assert out['errors']==[]
    assert out['reconciliation']['bucket_ev_pnl_diff']==0.0


def test_ev_by_bucket_all_still_fails_when_it_matches_neither_unit(tmp_path: Path):
    run=tmp_path/'r3'; run.mkdir()
    (run/'performance.json').write_text(json.dumps({
        'initial_equity':100000,
        'final_equity':99970,
        'net_pnl':-30.0,
        'gross_pnl':-30.0,
        'ev_net':-15.0,
        'ev_r_net':-0.15,
        'win_rate_r':0.5,
        'ev_by_bucket':{'all':123.0},
        'costs':{'fees_total':0,'slippage_total':0,'spread_total':0,'commission_total':0},
    }))
    (run/'trades.csv').write_text('pnl_net,r_multiple_net,risk_amount\n-40,-0.4,100\n10,0.1,100\n')
    out=audit_run(run)
    assert out['metrics_valid'] is False
    assert 'ev_by_bucket_all_mismatch' in out['errors']
