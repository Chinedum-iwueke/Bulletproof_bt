#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
from orchestrator.forensics.audit_run_metrics import audit_run

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--experiment-root',required=True); args=ap.parse_args()
    root=Path(args.experiment_root)
    rows=[]
    for run in sorted((root/'runs').glob('*')):
        if run.is_dir():
            out=audit_run(run)
            rec=out.get('reconciliation',{})
            rows.append({
                'run_id':out['run_id'],'metrics_valid':out['metrics_valid'],'equity_net_pnl_diff':rec.get('equity_net_pnl_diff'),'trade_net_pnl_diff':rec.get('trade_net_pnl_diff'),'mean_r_net_diff':rec.get('mean_r_net_diff'),'win_rate_r_diff':rec.get('win_rate_r_diff'),'bucket_ev_diff':rec.get('bucket_ev_diff'),'suspicious_r_count':rec.get('suspicious_r_count'),'zero_risk_count':rec.get('zero_risk_count'),'likely_failure_source':';'.join(out.get('errors',[])+out.get('suspicious',[])),'report_path':f"research/audits/run_metric_audit_{run.name}.json"
            })
    sdir=root/'summaries'; sdir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(sdir/'performance_validation_summary.csv',index=False)
    (sdir/'performance_validation_summary.json').write_text(json.dumps(rows,indent=2),encoding='utf-8')

if __name__=='__main__': main()
