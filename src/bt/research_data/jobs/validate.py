"""Validation job for research data files."""
from __future__ import annotations

import pandas as pd

from bt.research_data.jobs.daily_validation import run_daily_validation
from bt.research_data.storage import ResearchDataStore


def validate_all(exchange: str, store: ResearchDataStore | None = None, timeframe: str = "1m") -> pd.DataFrame:
    del timeframe
    if exchange == "all":
        return run_daily_validation(store=store, exchange=None)
    return run_daily_validation(store=store, exchange=exchange)
