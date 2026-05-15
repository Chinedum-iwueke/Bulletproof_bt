"""Causal joins for canonical research panels."""
from __future__ import annotations

import pandas as pd

from bt.research_data.schemas import RESEARCH_PANEL_COLUMNS, normalize_frame
from bt.research_data.validation import assert_causal_sources


def _asof_feature(
    panel: pd.DataFrame,
    feature: pd.DataFrame,
    value_cols: list[str],
    source_col: str,
) -> pd.DataFrame:
    if feature.empty:
        panel[source_col] = pd.NaT
        for col in value_cols:
            panel[col] = pd.NA
        return panel
    feature = feature.sort_values("ts").copy()
    feature[source_col] = feature["ts"]
    cols = ["ts", source_col, *value_cols]
    joined = pd.merge_asof(
        panel.sort_values("ts"),
        feature[cols].sort_values("ts"),
        on="ts",
        direction="backward",
        allow_exact_matches=True,
    )
    return joined


def build_research_panel(
    ohlcv: pd.DataFrame,
    mark: pd.DataFrame,
    index: pd.DataFrame,
    funding: pd.DataFrame,
    oi: pd.DataFrame,
    liquidations: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a symbol panel with exact candle joins and backward event joins."""
    if ohlcv.empty:
        return pd.DataFrame(columns=RESEARCH_PANEL_COLUMNS)
    panel = normalize_frame(ohlcv).sort_values("ts").reset_index(drop=True)
    for exact, cols in (
        (mark, ["ts", "mark_open", "mark_high", "mark_low", "mark_close"]),
        (index, ["ts", "index_open", "index_high", "index_low", "index_close"]),
    ):
        if exact.empty:
            for col in cols:
                if col != "ts":
                    panel[col] = pd.NA
            continue
        panel = panel.merge(normalize_frame(exact)[cols], on="ts", how="left", validate="one_to_one")
    panel = _asof_feature(panel, normalize_frame(funding), ["funding_rate"], "funding_source_ts")
    panel = _asof_feature(panel, normalize_frame(oi), ["open_interest"], "oi_source_ts")
    panel["oi_change_1"] = panel["open_interest"].diff()
    panel["oi_change_pct_1"] = panel["open_interest"].pct_change(fill_method=None)
    panel["premium_mark_vs_index"] = (panel["mark_close"] - panel["index_close"]) / panel["index_close"]
    panel["basis_close_vs_index"] = (panel["close"] - panel["index_close"]) / panel["index_close"]
    if liquidations is not None and not liquidations.empty:
        liq_cols = [
            "ts",
            "liq_buy_qty",
            "liq_sell_qty",
            "liq_buy_notional",
            "liq_sell_notional",
            "liq_event_count",
        ]
        panel = panel.merge(normalize_frame(liquidations)[liq_cols], on="ts", how="left", validate="one_to_one")
    columns = list(RESEARCH_PANEL_COLUMNS)
    if liquidations is not None and not liquidations.empty:
        columns.extend(
            [
                "liq_buy_qty",
                "liq_sell_qty",
                "liq_buy_notional",
                "liq_sell_notional",
                "liq_event_count",
            ]
        )
    panel = normalize_frame(panel, columns)
    assert_causal_sources(panel)
    return panel
