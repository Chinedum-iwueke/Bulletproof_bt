from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ProfileName = Literal["tier1", "tier2", "tier3", "custom"]


@dataclass(frozen=True)
class ExecutionProfile:
    name: ProfileName
    maker_fee: float
    taker_fee: float
    slippage_bps: float
    delay_bars: int
    spread_bps: float


_BUILTIN_PROFILES: dict[str, ExecutionProfile] = {
    "tier1": ExecutionProfile(
        name="tier1",
        maker_fee=0.0,
        taker_fee=0.0004,
        slippage_bps=0.5,
        delay_bars=0,
        spread_bps=0.0,
    ),
    "tier2": ExecutionProfile(
        name="tier2",
        maker_fee=0.0,
        taker_fee=0.0006,
        slippage_bps=2.0,
        delay_bars=1,
        spread_bps=1.0,
    ),
    "tier3": ExecutionProfile(
        name="tier3",
        maker_fee=0.0,
        taker_fee=0.0008,
        slippage_bps=5.0,
        delay_bars=1,
        spread_bps=3.0,
    ),
}

_PROFILE_OVERRIDE_FIELDS: tuple[str, ...] = (
    "maker_fee",
    "taker_fee",
    "slippage_bps",
    "delay_bars",
    "spread_bps",
)


def get_builtin_profile(name: ProfileName) -> ExecutionProfile:
    """Return the built-in profile definition."""
    profile = _BUILTIN_PROFILES.get(name)
    if profile is None:
        raise ValueError(f"Invalid execution.profile: expected one of tier1|tier2|tier3|custom, got {name!r}")
    return profile


def _as_float(value: Any, *, key: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {key}: expected a number, got {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"Invalid {key}: expected >= 0, got {parsed!r}")
    return parsed


def _as_non_negative_int(value: Any, *, key: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Invalid {key}: expected int >= 0, got {value!r}")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {key}: expected int >= 0, got {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"Invalid {key}: expected >= 0, got {parsed!r}")
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"Invalid {key}: expected int >= 0, got {value!r}")
    return parsed


def _resolve_legacy_top_level_profile(root: dict[str, Any]) -> ExecutionProfile | None:
    legacy_keys_present = any(
        key in root for key in ("maker_fee_bps", "taker_fee_bps", "signal_delay_bars", "fixed_bps")
    )
    if not legacy_keys_present:
        return None

    base = get_builtin_profile("tier2")
    maker_fee = base.maker_fee
    taker_fee = base.taker_fee
    delay_bars = base.delay_bars
    slippage_bps = base.slippage_bps

    if "maker_fee_bps" in root:
        maker_fee = _as_float(root.get("maker_fee_bps"), key="maker_fee_bps") / 1e4
    if "taker_fee_bps" in root:
        taker_fee = _as_float(root.get("taker_fee_bps"), key="taker_fee_bps") / 1e4
    if "signal_delay_bars" in root:
        delay_bars = _as_non_negative_int(root.get("signal_delay_bars"), key="signal_delay_bars")
    if "fixed_bps" in root:
        slippage_bps = _as_float(root.get("fixed_bps"), key="fixed_bps")

    return ExecutionProfile(
        name="custom",
        maker_fee=maker_fee,
        taker_fee=taker_fee,
        slippage_bps=slippage_bps,
        delay_bars=delay_bars,
        spread_bps=base.spread_bps,
    )


def resolve_execution_profile(config: dict[str, Any]) -> ExecutionProfile:
    """Resolve the effective execution profile from config."""
    root = config if isinstance(config, dict) else {}
    execution_cfg_raw = root.get("execution")
    if execution_cfg_raw is None:
        legacy_profile = _resolve_legacy_top_level_profile(root)
        if legacy_profile is not None:
            return legacy_profile
    if execution_cfg_raw is None:
        execution_cfg: dict[str, Any] = {}
    elif isinstance(execution_cfg_raw, dict):
        execution_cfg = execution_cfg_raw
    else:
        raise ValueError(f"Invalid execution: expected mapping, got {type(execution_cfg_raw).__name__}")

    if "profile" not in execution_cfg:
        legacy_profile = _resolve_legacy_top_level_profile(root)
        if legacy_profile is not None:
            return legacy_profile

    raw_profile = execution_cfg.get("profile", "tier2")
    if raw_profile not in {"tier1", "tier2", "tier3", "custom"}:
        raise ValueError(
            "Invalid execution.profile: expected one of tier1|tier2|tier3|custom, "
            f"got {raw_profile!r}"
        )
    profile_name: ProfileName = raw_profile

    if profile_name != "custom":
        conflicting = [field for field in _PROFILE_OVERRIDE_FIELDS if field in execution_cfg]
        if conflicting:
            raise ValueError(
                f"execution.profile={profile_name} forbids overrides. "
                "Set execution.profile=custom to specify "
                "maker_fee/taker_fee/slippage_bps/delay_bars/spread_bps."
            )
        return get_builtin_profile(profile_name)

    missing = [field for field in _PROFILE_OVERRIDE_FIELDS if field not in execution_cfg]
    if missing:
        missing_keys = ", ".join(f"execution.{field}" for field in missing)
        raise ValueError(
            "execution.profile=custom requires all override keys. "
            f"Missing: {missing_keys}."
        )

    return ExecutionProfile(
        name="custom",
        maker_fee=_as_float(execution_cfg.get("maker_fee"), key="execution.maker_fee"),
        taker_fee=_as_float(execution_cfg.get("taker_fee"), key="execution.taker_fee"),
        slippage_bps=_as_float(execution_cfg.get("slippage_bps"), key="execution.slippage_bps"),
        delay_bars=_as_non_negative_int(execution_cfg.get("delay_bars"), key="execution.delay_bars"),
        spread_bps=_as_float(execution_cfg.get("spread_bps"), key="execution.spread_bps"),
    )
