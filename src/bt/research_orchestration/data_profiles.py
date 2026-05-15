"""Resolve and preflight research_data profiles for grid orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

REQUIRED_PANEL_COLUMNS = {"ts", "symbol", "open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class ResearchDataProfile:
    universe: str
    data_kind: str = "research_panel"
    root: Path = Path("research_data")
    exchange: str = "binance"
    timeframe: str = "1m"
    stable_manifest: Path = Path("research_data/manifests/stable_universe.parquet")
    membership_path: Path = Path("research_data/manifests/volatile_universe_membership.parquet")

    def to_config(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "dataset_kind": self.data_kind,
            "root": str(self.root),
            "exchange": self.exchange,
            "universe": self.universe,
            "timeframe": self.timeframe,
        }
        if self.universe == "stable":
            data["stable_manifest"] = str(self.stable_manifest)
        if self.universe == "volatile":
            data["membership_path"] = str(self.membership_path)
        return {"data": data}


def resolve_data_profile(
    *,
    universe: str,
    data_root: str | Path = "research_data",
    data_kind: str = "research_panel",
    exchange: str = "binance",
    timeframe: str = "1m",
    stable_manifest: str | Path | None = None,
    membership_path: str | Path | None = None,
) -> ResearchDataProfile:
    root = Path(data_root)
    stable = Path(stable_manifest) if stable_manifest else root / "manifests" / "stable_universe.parquet"
    membership = Path(membership_path) if membership_path else root / "manifests" / "volatile_universe_membership.parquet"
    if data_kind != "research_panel":
        raise ValueError(f"unsupported data_kind for research data profile: {data_kind}")
    if universe not in {"stable", "volatile"}:
        raise ValueError(f"universe must be stable or volatile, got: {universe}")
    return ResearchDataProfile(
        universe=universe,
        data_kind=data_kind,
        root=root,
        exchange=exchange,
        timeframe=timeframe,
        stable_manifest=stable,
        membership_path=membership,
    )


def write_data_profile_config(profile: ResearchDataProfile, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(profile.to_config(), sort_keys=False), encoding="utf-8")
    return output_path


def preflight_research_data_profile(profile: ResearchDataProfile) -> None:
    if not profile.root.exists():
        raise FileNotFoundError(f"research_data root does not exist: {profile.root}")
    if profile.universe == "stable":
        symbols = _stable_symbols(profile)
    else:
        symbols = _volatile_symbols(profile)
    if not symbols:
        raise ValueError(f"No symbols resolved for research data profile: {profile.universe}")
    for symbol in symbols:
        _validate_panel_file(profile, symbol)


def _stable_symbols(profile: ResearchDataProfile) -> list[str]:
    if not profile.stable_manifest.exists():
        raise FileNotFoundError(f"stable universe manifest missing: {profile.stable_manifest}")
    stable = pd.read_parquet(profile.stable_manifest)
    if stable.empty:
        raise ValueError(f"stable universe manifest is empty: {profile.stable_manifest}")
    native_col = "native_symbol" if "native_symbol" in stable.columns else "symbol"
    filtered = stable[stable["exchange"].eq(profile.exchange)] if "exchange" in stable.columns else stable
    if "available" in filtered.columns:
        filtered = filtered[filtered["available"].astype(bool)]
    if "first_seen_ts" in filtered.columns:
        pd.to_datetime(filtered["first_seen_ts"], utc=True, errors="coerce")
    return filtered[native_col].astype(str).drop_duplicates().tolist()


def _volatile_symbols(profile: ResearchDataProfile) -> list[str]:
    if not profile.membership_path.exists():
        raise FileNotFoundError(f"volatile membership missing: {profile.membership_path}")
    membership = pd.read_parquet(profile.membership_path)
    if membership.empty:
        raise ValueError(f"volatile membership is empty: {profile.membership_path}")
    membership["ts"] = pd.to_datetime(membership["ts"], utc=True, errors="raise")
    filtered = membership[membership["exchange"].eq(profile.exchange)] if "exchange" in membership.columns else membership
    if filtered.duplicated(["ts", "symbol"]).any():
        raise ValueError("volatile membership contains duplicate ts/symbol rows")
    return filtered["symbol"].astype(str).drop_duplicates().tolist()


def _validate_panel_file(profile: ResearchDataProfile, symbol: str) -> None:
    path = profile.root / "canonical" / profile.exchange / symbol / f"timeframe={profile.timeframe}" / "research_panel.parquet"
    if not path.exists():
        raise FileNotFoundError(f"research panel missing for {profile.exchange}/{symbol}: {path}")
    df = pd.read_parquet(path)
    missing = REQUIRED_PANEL_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"research panel missing required columns {sorted(missing)}: {path}")
    ts = pd.to_datetime(df["ts"], utc=True, errors="raise")
    if str(ts.dt.tz) != "UTC":
        raise ValueError(f"research panel timestamps must be UTC: {path}")
    for source_col in ("funding_source_ts", "oi_source_ts"):
        if source_col in df.columns:
            source = pd.to_datetime(df[source_col], utc=True, errors="coerce")
            mask = source.notna()
            if (source[mask] > ts[mask]).any():
                raise ValueError(f"{source_col} exceeds bar ts in {path}")
