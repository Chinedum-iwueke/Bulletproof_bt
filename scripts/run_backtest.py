"""CLI entrypoint for v1 backtests."""
from __future__ import annotations

import argparse
from pathlib import Path

from bt.api import run_backtest
from bt.logging.run_contract import validate_run_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Run backtest (v1).")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--data", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--local-config")
    args = parser.parse_args()

    override_paths = list(args.override)
    if args.local_config:
        override_paths.append(args.local_config)

    run_dir = run_backtest(
        config_path=args.config,
        data_path=args.data,
        out_dir="outputs/runs",
        override_paths=override_paths or None,
        run_name=args.run_id,
    )

    validate_run_artifacts(Path(run_dir))

    print(f"run_dir: {run_dir}")


if __name__ == "__main__":
    main()
