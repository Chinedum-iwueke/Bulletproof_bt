"""CLI entrypoint for v1 backtests."""
from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="Run backtest (scaffold).")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.parse_args()
    print("Scaffold OK")


if __name__ == "__main__":
    main()
