"""Summarize grid status CSV."""
from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from bt.experiments.parallel_grid import cli_grid_status


if __name__ == "__main__":
    raise SystemExit(cli_grid_status())
