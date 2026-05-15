"""Stable and volatile universe construction."""
from __future__ import annotations

import pandas as pd

from bt.research_data.schemas import VOLATILE_UNIVERSE_COLUMNS, normalize_frame
from bt.research_data.time import utc_ts


def build_volatile_universe_from_ohlcv(
    bars: pd.DataFrame,
    exchange: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    rebalance_freq: str = "2h",
    lookback: str = "24h",
    top_gainers: int = 20,
    top_losers: int = 10,
    min_age_days: int = 30,
    min_median_dollar_volume_7d: float = 5_000_000,
    stale_after: str = "5min",
) -> pd.DataFrame:
    """Build historical top mover membership using only bars at or before each rebalance."""
    if bars.empty:
        return pd.DataFrame(columns=VOLATILE_UNIVERSE_COLUMNS)
    data = normalize_frame(bars).sort_values(["symbol", "ts"]).copy()
    first_seen = data.groupby("symbol")["ts"].min()
    data = data[(data["ts"] >= utc_ts(start) - pd.Timedelta(days=8)) & (data["ts"] <= utc_ts(end))]
    data["dollar_volume"] = data["quote_volume"].where(data["quote_volume"].notna(), data["close"] * data["volume"])
    rows: list[dict[str, object]] = []
    rebalances = pd.date_range(utc_ts(start), utc_ts(end), freq=rebalance_freq, inclusive="left")
    lookback_td = pd.Timedelta(lookback)
    stale_td = pd.Timedelta(stale_after)
    for rebalance_id, ts in enumerate(rebalances):
        scores: list[dict[str, object]] = []
        for symbol, group in data.groupby("symbol", sort=False):
            history = group[group["ts"] <= ts]
            if history.empty:
                continue
            if ts - first_seen.loc[symbol] < pd.Timedelta(days=min_age_days):
                continue
            latest = history.iloc[-1]
            if ts - latest["ts"] > stale_td or latest["close"] <= 0:
                continue
            prior = history[history["ts"] <= ts - lookback_td]
            if prior.empty:
                continue
            lookback_row = prior.iloc[-1]
            if lookback_row["close"] <= 0:
                continue
            volume_window = history[history["ts"] > ts - pd.Timedelta(days=7)]
            median_dollar_volume_7d = float(volume_window["dollar_volume"].median()) if not volume_window.empty else 0.0
            if median_dollar_volume_7d < min_median_dollar_volume_7d:
                continue
            ret = float(latest["close"] / lookback_row["close"] - 1.0)
            scores.append({"symbol": symbol, "score": ret})
        ranked = pd.DataFrame(scores)
        if ranked.empty:
            continue
        gainers = ranked.sort_values("score", ascending=False).head(top_gainers).reset_index(drop=True)
        losers = ranked.sort_values("score", ascending=True).head(top_losers).reset_index(drop=True)
        for rank_type, selected in (("gainer", gainers), ("loser", losers)):
            for rank, row in enumerate(selected.itertuples(index=False), start=1):
                rows.append(
                    {
                        "ts": ts,
                        "exchange": exchange,
                        "symbol": row.symbol,
                        "universe": "volatile_data_1m_canonical",
                        "rank_type": rank_type,
                        "rank": rank,
                        "score": row.score,
                        "rebalance_id": rebalance_id,
                        "lookback": lookback,
                        "rebalance_freq": rebalance_freq,
                    }
                )
    return normalize_frame(pd.DataFrame(rows), VOLATILE_UNIVERSE_COLUMNS)
