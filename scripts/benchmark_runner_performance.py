#!/usr/bin/env python3
"""Benchmark the hypothesis runner on a small manifest slice.

This script measures orchestration overhead without changing backtest semantics:
it builds a normal hypothesis grid, truncates the manifest to N rows, runs the
same parallel runner, then records wall-clock time and artifact sizes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.experiments.manifest import read_manifest_csv, write_manifest_csv
from bt.experiments.resource_controls import memory_snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark runner wall time and output footprint.")
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--data", default=None, help="Legacy dataset path for benchmark input.")
    parser.add_argument("--data-root", default=None, help="Research data root for research_panel mode.")
    parser.add_argument("--data-kind", default=None)
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--universe", default="stable", choices=["stable", "volatile"])
    parser.add_argument("--membership-path", default="research_data/manifests/volatile_universe_membership.parquet")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--phase", default="tier2")
    parser.add_argument("--runs", type=int, default=4)
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--local-config", default="configs/local/engine.lab.yaml")
    parser.add_argument("--experiment-root", default=None)
    return parser.parse_args()


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _run(cmd: list[str]) -> float:
    started = time.monotonic()
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)
    return time.monotonic() - started


def main() -> int:
    args = parse_args()
    if not args.data and not (args.data_root and args.data_kind == "research_panel"):
        raise SystemExit("provide --data or --data-root with --data-kind research_panel")

    audit_dir = PROJECT_ROOT / "research" / "audits"
    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = _timestamp()
    experiment_root = Path(args.experiment_root) if args.experiment_root else PROJECT_ROOT / "outputs" / "benchmarks" / f"runner_{stamp}"
    experiment_root.mkdir(parents=True, exist_ok=True)

    build_seconds = _run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_hypothesis_grid.py"),
            "--hypothesis",
            args.hypothesis,
            "--experiment-root",
            str(experiment_root),
            "--phase",
            args.phase,
        ]
    )
    manifest = max((experiment_root / "manifests").glob("*.csv"), key=lambda p: p.stat().st_mtime)
    rows = read_manifest_csv(manifest)[: max(args.runs, 1)]
    write_manifest_csv(rows, manifest)

    run_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "run_parallel_hypothesis_grid.py"),
        "--experiment-root",
        str(experiment_root),
        "--manifest",
        str(manifest),
        "--config",
        args.config,
        "--local-config",
        args.local_config,
        "--phase",
        args.phase,
        "--max-workers",
        str(args.max_workers),
        "--skip-completed",
    ]
    if args.data:
        run_cmd.extend(["--data", args.data])
    else:
        run_cmd.extend(
            [
                "--data-root",
                args.data_root,
                "--data-kind",
                args.data_kind,
                "--exchange",
                args.exchange,
                "--universe",
                args.universe,
                "--timeframe",
                args.timeframe,
            ]
        )
        if args.universe == "volatile":
            run_cmd.extend(["--membership-path", args.membership_path])

    mem_before = memory_snapshot()
    run_started = time.monotonic()
    run_result = subprocess.run(run_cmd, cwd=PROJECT_ROOT, text=True)
    run_seconds = time.monotonic() - run_started
    mem_after = memory_snapshot()

    post_seconds = 0.0
    extract_seconds = 0.0
    if run_result.returncode == 0:
        post_seconds = _run(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "post_run_analysis.py"),
                "--experiment-root",
                str(experiment_root),
                "--runs-glob",
                "runs/*",
                "--skip-existing",
            ]
        )
        extract_seconds = _run(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "extract_experiment_dataset.py"),
                "--experiment-root",
                str(experiment_root),
                "--runs-glob",
                "runs/*",
                "--skip-existing",
            ]
        )

    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "hypothesis": args.hypothesis,
        "experiment_root": str(experiment_root),
        "phase": args.phase,
        "runs": len(rows),
        "max_workers": args.max_workers,
        "return_code": run_result.returncode,
        "build_seconds": build_seconds,
        "run_seconds": run_seconds,
        "avg_run_wall_seconds": run_seconds / max(len(rows), 1),
        "post_run_analysis_seconds": post_seconds,
        "extraction_seconds": extract_seconds,
        "artifact_size_bytes": _dir_size(experiment_root),
        "memory_before": mem_before.__dict__ if mem_before else None,
        "memory_after": mem_after.__dict__ if mem_after else None,
    }

    json_path = audit_dir / f"runner_benchmark_{stamp}.json"
    md_path = audit_dir / f"runner_benchmark_{stamp}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(
        "\n".join(
            [
                f"# Runner Benchmark {stamp}",
                "",
                f"- Hypothesis: `{args.hypothesis}`",
                f"- Runs: {len(rows)}",
                f"- Max workers: {args.max_workers}",
                f"- Return code: {run_result.returncode}",
                f"- Build seconds: {build_seconds:.2f}",
                f"- Run seconds: {run_seconds:.2f}",
                f"- Avg wall seconds per manifest row: {payload['avg_run_wall_seconds']:.2f}",
                f"- Post-run analysis seconds: {post_seconds:.2f}",
                f"- Extraction seconds: {extract_seconds:.2f}",
                f"- Artifact size bytes: {payload['artifact_size_bytes']}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(json_path)
    print(md_path)
    return run_result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
