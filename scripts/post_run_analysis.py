#!/usr/bin/env python3
"""Unified post-run summary + diagnostics CLI."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.analytics.postmortem import run_postmortem_for_experiment
from bt.analytics.run_summary import summarize_experiment_runs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate post-run run_summary + diagnostics artifacts.")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--runs-glob", default="runs/*")
    parser.add_argument("--completed-only", action="store_true", default=False)
    parser.add_argument("--include-diagnostics", action="store_true", default=False)
    parser.add_argument("--skip-existing", action="store_true", default=False)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    root = Path(args.experiment_root)
    summary_path = root / "summaries" / "run_summary.csv"

    if args.skip_existing and summary_path.exists():
        print(f"Skipping summary generation because --skip-existing was set and file exists: {summary_path}")
    else:
        summary_df, warnings_df = summarize_experiment_runs(
            root,
            runs_glob=args.runs_glob,
            completed_only=args.completed_only,
        )
        print(f"Wrote run summary rows={len(summary_df)} warnings={len(warnings_df)} -> {summary_path}")

    if args.include_diagnostics:
        outputs = run_postmortem_for_experiment(root)
        print(f"Diagnostics outputs generated={len(outputs)}")


if __name__ == "__main__":
    main()
