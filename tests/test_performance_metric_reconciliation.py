import pandas as pd
from pathlib import Path
from bt.metrics.performance import compute_performance

def test_equity_drop_reconciles_not_zero(tmp_path: Path):
    run=tmp_path/'run'; run.mkdir()
    pd.DataFrame({'equity':[100000,79217]}).to_csv(run/'equity.csv',index=False)
    pd.DataFrame({'pnl_net':[-20783.0],'pnl_price':[-20000.0],'fees_total':[783.0],'r_multiple_net':[-1.0],'risk_amount':[20783.0]}).to_csv(run/'trades.csv',index=False)
    rep=compute_performance(run)
    assert round(rep.net_pnl,2)==-20783.0
    assert round(rep.final_equity-rep.initial_equity,2)==-20783.0
