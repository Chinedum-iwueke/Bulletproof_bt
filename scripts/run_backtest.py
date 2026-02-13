"""CLI entrypoint for v1 backtests."""
from __future__ import annotations

import argparse

from bt.api import run_backtest


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

    print(f"run_dir: {run_dir}")


if __name__ == "__main__":
    main()
