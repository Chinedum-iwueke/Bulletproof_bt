#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import sys
import pandas as pd
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for path in (PROJECT_ROOT, SRC_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
from bt.analytics.postmortem import run_postmortem_for_experiment
from bt.analytics.run_summary import summarize_experiment_runs
from bt.analysis.ev_by_bucket import analyze_structural_buckets, write_structural_bucket_artifacts
from orchestrator.forensics.audit_run_metrics import audit_run

def build_parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser()
    p.add_argument("--experiment-root", required=True); p.add_argument("--runs-glob", default="runs/*")
    p.add_argument("--completed-only", action="store_true", default=False); p.add_argument("--include-diagnostics", action="store_true", default=False)
    p.add_argument("--skip-existing", action="store_true", default=False); p.add_argument("--enable-structural-buckets", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--bucket-min-trades", type=int, default=10); p.add_argument("--bucket-output-prefix", default="summaries")
    p.add_argument("--per-run-structural-buckets", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--per-run-bucket-min-trades", type=int, default=5); p.add_argument("--per-run-min-trades", type=int, default=20)
    p.add_argument("--per-run-ready-min-ev", type=float, default=0.05); p.add_argument("--per-run-cost-fragile-ratio", type=float, default=0.5)
    p.add_argument("--force-structural-buckets", action="store_true", default=False)
    p.add_argument("--force", action="store_true", default=False)
    p.add_argument("--jobs", type=int, default=1, help="Reserved for per-run diagnostics parallelism; default keeps memory bounded.")
    return p

def _trade_df(run_dir: Path, all_df: pd.DataFrame) -> pd.DataFrame:
    for nm in ("trades.csv","trades.parquet"):
        p=run_dir/nm
        if p.exists():
            return pd.read_csv(p) if p.suffix==".csv" else pd.read_parquet(p)
    rid=run_dir.name
    if not all_df.empty and "run_id" in all_df.columns:
        return all_df[all_df["run_id"].astype(str)==rid].copy()
    return pd.DataFrame()


def _series(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.Series:
    for c in cols:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")

def _summ(run_id:str, df:pd.DataFrame, rows:pd.DataFrame|None, args)->dict:
    r=_series(df,("r_net","realized_r_net","r_multiple_net"))
    rg=_series(df,("r_gross","realized_r_gross","r_multiple_gross"))
    mfe=_series(df,("path_mfe_r","mfe_r"))
    ee=_series(df,("counterfactual_exit_efficiency_realized_over_mfe",))
    cost=_series(df,("cost_drag_r",))
    allrows=rows if rows is not None else pd.DataFrame()
    best=allrows[allrows.get("bucket_key")!="overall_all_trades"].sort_values("ev_r_net", ascending=False).head(3) if not allrows.empty else pd.DataFrame()
    worst=allrows[allrows.get("bucket_key")!="overall_all_trades"].sort_values("ev_r_net", ascending=True).head(3) if not allrows.empty else pd.DataFrame()
    tails=(allrows[allrows["tail_5r_count"]>0].sort_values("tail_5r_count",ascending=False).head(3) if (not allrows.empty and "tail_5r_count" in allrows.columns) else pd.DataFrame())
    costk=(allrows[(allrows["ev_r_gross"]>0)&(allrows["ev_r_net"]<=0)].head(3) if (not allrows.empty and {"ev_r_gross","ev_r_net"}.issubset(allrows.columns)) else pd.DataFrame())
    exitf=(allrows[(allrows["avg_mfe_r"]>0)&(allrows["avg_exit_efficiency"]<0.35)].head(3) if (not allrows.empty and {"avg_mfe_r","avg_exit_efficiency"}.issubset(allrows.columns)) else pd.DataFrame())
    n=int(len(df)); ev=float(r.mean()) if n else 0.0; evg=float(rg.mean()) if rg.notna().any() else None
    sample_too_small = n < args.per_run_min_trades
    one_bucket_dependency = (not best.empty and int(best.iloc[0].get('n_trades',0)) < max(5, int(0.2*n)))
    cost_fragile = (evg is not None and evg>0 and ev<=0) or (abs(ev)>0 and (abs(float(cost.mean() or 0))/abs(ev))>args.per_run_cost_fragile_ratio)
    needs_state_filter = (ev<=0 and not best.empty and float(best.iloc[0].get("ev_r_net",0))>args.per_run_ready_min_ev)
    needs_exit_refinement = (float(mfe.mean() or 0)>0.5 and float(ee.mean() or 0)<0.35) or (not tails.empty and ev<=args.per_run_ready_min_ev)
    ready = (ev>args.per_run_ready_min_ev and n>=args.per_run_min_trades and (not sample_too_small) and (not one_bucket_dependency) and (not cost_fragile))
    return {"run_id":run_id,"n_trades":n,"overall":{"ev_r_net":ev,"ev_r_gross":evg,"win_rate":float((r>0).mean()) if n else 0.0,"median_r_net":float(r.median()) if n else 0.0,"max_r":float(r.max()) if n else 0.0,"min_r":float(r.min()) if n else 0.0,"tail_3r_count":int((r>=3).sum()),"tail_5r_count":int((r>=5).sum()),"tail_10r_count":int((r>=10).sum()),"avg_mfe_r":float(mfe.mean()) if mfe.notna().any() else None,"avg_mae_r":float(_series(df,('path_mae_r','mae_r')).mean()) if n else None,"avg_exit_efficiency":float(ee.mean()) if ee.notna().any() else None,"avg_cost_drag_r":float(cost.mean()) if cost.notna().any() else None},"best_buckets":best.to_dict('records'),"worst_buckets":worst.to_dict('records'),"tail_generation_buckets":tails.to_dict('records'),"cost_killed_buckets":costk.to_dict('records'),"exit_failure_buckets":exitf.to_dict('records'),"recommendation_flags":{"ready_for_tier3_candidate":ready,"needs_state_filter":needs_state_filter,"needs_exit_refinement":needs_exit_refinement,"cost_fragile":cost_fragile,"sample_too_small":sample_too_small,"one_bucket_dependency":one_bucket_dependency}}

def main()->None:
    args=build_parser().parse_args(); root=Path(args.experiment_root); summary_path=root/"summaries"/"run_summary.csv"
    if not (args.skip_existing and summary_path.exists()): summarize_experiment_runs(root,runs_glob=args.runs_glob,completed_only=args.completed_only)
    if args.include_diagnostics: run_postmortem_for_experiment(root)
    trades_path=root/"research_data"/"trades_dataset.parquet"; all_df=pd.read_parquet(trades_path) if trades_path.exists() else pd.DataFrame()
    if args.enable_structural_buckets and (not all_df.empty): write_structural_bucket_artifacts(analyze_structural_buckets(all_df,min_trades=args.bucket_min_trades), root/args.bucket_output_prefix)
    run_rows=[]
    for run_dir in sorted((root/"runs").glob("*")):
        if not run_dir.is_dir(): continue
        analysis=run_dir/"analysis"; summary_json=analysis/"structural_diagnostics_summary.json"
        if args.skip_existing and summary_json.exists() and not (args.force or args.force_structural_buckets): continue
        analysis.mkdir(parents=True, exist_ok=True)
        rdf=_trade_df(run_dir, all_df)
        if rdf.empty:
            payload={"run_id":run_dir.name,"status":"no_trades","n_trades":0,"message":"No trades available for per-run structural bucket diagnostics."}
            summary_json.write_text(json.dumps(payload,indent=2),encoding="utf-8"); run_rows.append(payload|{"ev_r_net":None}); continue
        res=analyze_structural_buckets(rdf,min_trades=args.per_run_bucket_min_trades); paths=write_structural_bucket_artifacts(res, analysis)
        all_rows=pd.concat([v for v in res.outputs.values() if isinstance(v,pd.DataFrame)],ignore_index=True)
        payload=_summ(run_dir.name, rdf, all_rows, args)
        payload["missing_fields"]=res.missing_fields; payload["under_instrumented"]=bool(res.missing_fields); payload["schema_coverage_path"]=str(analysis/"trade_schema_coverage.json")
        (analysis/"trade_schema_coverage.json").write_text(json.dumps({"missing_fields":res.missing_fields},indent=2),encoding='utf-8')
        summary_json.write_text(json.dumps(payload,indent=2),encoding='utf-8')
        (analysis/"structural_diagnostics_summary.md").write_text(f"# Structural Diagnostics for {run_dir.name}\n\n## Overall Performance\n\n{json.dumps(payload['overall'],indent=2)}\n\n## Best Buckets\n\n{len(payload['best_buckets'])} buckets\n\n## Weak / Avoid Buckets\n\n{len(payload['worst_buckets'])} buckets\n\n## Tail Generation\n\n{len(payload['tail_generation_buckets'])} buckets\n\n## Cost Drag\n\n{len(payload['cost_killed_buckets'])} buckets\n\n## Exit Efficiency\n\n{len(payload['exit_failure_buckets'])} buckets\n\n## Schema Coverage\n\n{payload['missing_fields']}\n\n## Recommendation Flags\n\n{payload['recommendation_flags']}\n",encoding='utf-8')
        (analysis/"performance_by_bucket.csv").write_text((analysis/"ev_by_bucket.csv").read_text(encoding='utf-8'),encoding='utf-8')
        validation=audit_run(run_dir)
        (analysis/'performance_validation.json').write_text(json.dumps(validation,indent=2),encoding='utf-8')
        (analysis/'performance_validation.md').write_text(f"# Performance Validation {run_dir.name}\n\nStatus: **{validation['status']}**\n\n```json\n{json.dumps(validation,indent=2)}\n```\n",encoding='utf-8')
        perf_path=run_dir/'performance.json'
        if perf_path.exists():
            try:
                perf=json.loads(perf_path.read_text(encoding='utf-8'))
                perf['metrics_valid']=bool(validation.get('metrics_valid',False))
                if not perf['metrics_valid']:
                    perf['metric_validation_errors']=validation.get('errors',[])
                    perf['metric_validation_report']='analysis/performance_validation.json'
                else:
                    perf.pop('metric_validation_errors', None)
                    perf.pop('metric_validation_report', None)
                perf_path.write_text(json.dumps(perf,indent=2),encoding='utf-8')
            except Exception:
                pass
        run_rows.append({"run_id":run_dir.name,"n_trades":payload['n_trades'],"ev_r_net":payload['overall']['ev_r_net'],"best_bucket_type":payload['best_buckets'][0]['bucket'] if payload['best_buckets'] else None,"best_bucket":payload['best_buckets'][0]['bucket_key'] if payload['best_buckets'] else None,"best_bucket_ev_r_net":payload['best_buckets'][0].get('ev_r_net') if payload['best_buckets'] else None,"best_bucket_n_trades":payload['best_buckets'][0].get('n_trades') if payload['best_buckets'] else None,"worst_bucket_type":payload['worst_buckets'][0]['bucket'] if payload['worst_buckets'] else None,"worst_bucket":payload['worst_buckets'][0]['bucket_key'] if payload['worst_buckets'] else None,"worst_bucket_ev_r_net":payload['worst_buckets'][0].get('ev_r_net') if payload['worst_buckets'] else None,"tail_bucket_type":payload['tail_generation_buckets'][0]['bucket'] if payload['tail_generation_buckets'] else None,"tail_bucket":payload['tail_generation_buckets'][0]['bucket_key'] if payload['tail_generation_buckets'] else None,"tail_5r_count":payload['tail_generation_buckets'][0].get('tail_5r_count') if payload['tail_generation_buckets'] else 0,"avg_exit_efficiency":payload['overall']['avg_exit_efficiency'],"avg_cost_drag_r":payload['overall']['avg_cost_drag_r'],**payload['recommendation_flags'],"structural_summary_path":str(summary_json)})
    if run_rows: pd.DataFrame(run_rows).to_csv(root/"summaries"/"run_structural_summary.csv", index=False)

if __name__=="__main__": main()
