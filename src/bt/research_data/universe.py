"""Stable and volatile universe construction."""
from __future__ import annotations

import pandas as pd

from bt.research_data.instruments import native_to_canonical_symbol
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
    start_ts = utc_ts(start)
    end_ts = utc_ts(end)
    first_seen = data.groupby("symbol")["ts"].min()
    data = data[(data["ts"] >= start_ts - pd.Timedelta(days=8)) & (data["ts"] <= end_ts)]
    data["dollar_volume"] = data["quote_volume"].where(data["quote_volume"].notna(), data["close"] * data["volume"])
    rebalances = pd.date_range(start_ts, end_ts, freq=rebalance_freq, inclusive="left")
    lookback_td = pd.Timedelta(lookback)
    stale_td = pd.Timedelta(stale_after)
    grid = pd.DataFrame({"ts": rebalances})
    grid["prior_ts"] = grid["ts"] - lookback_td
    grid["rebalance_id"] = range(len(grid))
    score_frames: list[pd.DataFrame] = []
    for symbol, group in data.groupby("symbol", sort=False):
        group = group.sort_values("ts")
        close_frame = group[["ts", "close"]].dropna()
        if close_frame.empty:
            continue

        latest = pd.merge_asof(
            grid[["ts", "rebalance_id"]],
            close_frame.rename(columns={"ts": "source_ts", "close": "latest_close"}),
            left_on="ts",
            right_on="source_ts",
            direction="backward",
        )
        prior = pd.merge_asof(
            grid[["prior_ts"]].rename(columns={"prior_ts": "ts"}),
            close_frame.rename(columns={"ts": "prior_source_ts", "close": "prior_close"}),
            left_on="ts",
            right_on="prior_source_ts",
            direction="backward",
        )
        latest["prior_close"] = prior["prior_close"]
        latest["prior_source_ts"] = prior["prior_source_ts"]

        daily_dollar_volume = group.set_index("ts")["dollar_volume"].resample("1D").sum()
        daily_median = daily_dollar_volume.rolling(7, min_periods=1).median().reset_index()
        daily_median["asof_day"] = daily_median["ts"] + pd.Timedelta(days=1)
        volume = pd.merge_asof(
            grid[["ts"]],
            daily_median[["asof_day", "dollar_volume"]].rename(
                columns={"asof_day": "volume_source_ts", "dollar_volume": "median_dollar_volume_7d"}
            ),
            left_on="ts",
            right_on="volume_source_ts",
            direction="backward",
        )
        latest["median_dollar_volume_7d"] = volume["median_dollar_volume_7d"]

        first = first_seen.loc[symbol]
        valid = (
            latest["latest_close"].gt(0)
            & latest["prior_close"].gt(0)
            & latest["source_ts"].notna()
            & latest["prior_source_ts"].notna()
            & latest["ts"].sub(latest["source_ts"]).le(stale_td)
            & latest["ts"].sub(first).ge(pd.Timedelta(days=min_age_days))
            & latest["median_dollar_volume_7d"].ge(min_median_dollar_volume_7d)
        )
        selected = latest.loc[valid, ["ts", "rebalance_id", "latest_close", "prior_close"]].copy()
        if selected.empty:
            continue
        selected["symbol"] = symbol
        selected["score"] = selected["latest_close"] / selected["prior_close"] - 1.0
        score_frames.append(selected[["ts", "rebalance_id", "symbol", "score"]])

    if not score_frames:
        return pd.DataFrame(columns=VOLATILE_UNIVERSE_COLUMNS)
    scores = pd.concat(score_frames, ignore_index=True)
    rows: list[pd.DataFrame] = []
    if top_gainers > 0:
        gainers = scores.sort_values(["ts", "score"], ascending=[True, False]).groupby("ts", sort=False).head(top_gainers).copy()
        gainers["rank_type"] = "gainer"
        gainers["rank"] = gainers.groupby("ts")["score"].rank(method="first", ascending=False).astype(int)
        rows.append(gainers)
    if top_losers > 0:
        losers = scores.sort_values(["ts", "score"], ascending=[True, True]).groupby("ts", sort=False).head(top_losers).copy()
        losers["rank_type"] = "loser"
        losers["rank"] = losers.groupby("ts")["score"].rank(method="first", ascending=True).astype(int)
        rows.append(losers)
    if not rows:
        return pd.DataFrame(columns=VOLATILE_UNIVERSE_COLUMNS)
    membership = pd.concat(rows, ignore_index=True)
    membership["exchange"] = exchange
    membership["canonical_symbol"] = membership["symbol"].map(lambda symbol: native_to_canonical_symbol(str(symbol)))
    membership["universe"] = "volatile_data_1m_canonical"
    membership["lookback"] = lookback
    membership["rebalance_freq"] = rebalance_freq
    membership = membership.drop_duplicates(["ts", "symbol", "rank_type"], keep="last")
    return normalize_frame(membership, VOLATILE_UNIVERSE_COLUMNS)
