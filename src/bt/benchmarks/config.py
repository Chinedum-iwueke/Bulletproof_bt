from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from bt.benchmarks.types import DisabledBenchmarkConfig, EnabledBenchmarkConfig, BenchmarkConfig

_SUPPORTED_IDS = {"BTC", "SPY", "XAUUSD", "DXY"}
_SUPPORTED_MODES_ENABLED = {"auto", "manual"}
_SUPPORTED_MODES_ALL = {"auto", "manual", "none"}
_SUPPORTED_SOURCE = {"platform_managed"}
_SUPPORTED_FREQUENCY = {"1d"}
_REQUIRED_ENABLED_FIELDS = {
    "enabled",
    "mode",
    "id",
    "source",
    "library_root",
    "frequency",
    "alignment_policy",
    "comparison_frequency",
    "normalization_basis",
}


class BenchmarkConfigError(ValueError):
    """Raised when benchmark payload is malformed or unsupported."""


def _require_mapping(raw: dict[str, Any] | None) -> dict[str, Any]:
    if raw is None:
        return {"enabled": False, "mode": "none"}
    if not isinstance(raw, dict):
        raise BenchmarkConfigError(f"benchmark must be a mapping (got: {raw!r})")
    return raw


def _require_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BenchmarkConfigError(f"benchmark.{field_name} must be a non-empty string (got: {value!r})")
    return value.strip()


def _reject_unknown_keys(payload: dict[str, Any], allowed_keys: Iterable[str]) -> None:
    unknown_keys = sorted(set(payload) - set(allowed_keys))
    if unknown_keys:
        raise BenchmarkConfigError(
            f"benchmark contains unsupported field(s): {', '.join(unknown_keys)}"
        )


def parse_benchmark_config(raw: dict[str, Any] | None) -> BenchmarkConfig:
    payload = _require_mapping(raw)

    enabled = payload.get("enabled", False)
    if not isinstance(enabled, bool):
        raise BenchmarkConfigError(f"benchmark.enabled must be a bool (got: {enabled!r})")

    mode = payload.get("mode", "none" if not enabled else None)
    if mode not in _SUPPORTED_MODES_ALL:
        raise BenchmarkConfigError(
            "benchmark.mode must be one of {'auto', 'manual', 'none'} "
            f"(got: {mode!r})"
        )

    if not enabled:
        _reject_unknown_keys(payload, {"enabled", "mode"})
        if mode != "none":
            raise BenchmarkConfigError(
                "benchmark.mode must be 'none' when benchmark.enabled=false"
            )
        return DisabledBenchmarkConfig(enabled=False, mode="none")

    missing = sorted(_REQUIRED_ENABLED_FIELDS - set(payload))
    if missing:
        raise BenchmarkConfigError(
            "benchmark missing required field(s) when enabled: " + ", ".join(missing)
        )

    if mode not in _SUPPORTED_MODES_ENABLED:
        raise BenchmarkConfigError(
            "benchmark.mode must be one of {'auto', 'manual'} when benchmark.enabled=true "
            f"(got: {mode!r})"
        )

    benchmark_id = _require_str(payload.get("id"), "id")
    if benchmark_id not in _SUPPORTED_IDS:
        raise BenchmarkConfigError(
            f"benchmark.id must be one of {sorted(_SUPPORTED_IDS)} (got: {benchmark_id!r})"
        )

    source = _require_str(payload.get("source"), "source")
    if source not in _SUPPORTED_SOURCE:
        raise BenchmarkConfigError(
            "benchmark.source must be 'platform_managed' "
            f"(got: {source!r})"
        )

    frequency = _require_str(payload.get("frequency"), "frequency")
    if frequency not in _SUPPORTED_FREQUENCY:
        raise BenchmarkConfigError(
            f"benchmark.frequency must be '1d' (got: {frequency!r})"
        )

    comparison_frequency = _require_str(payload.get("comparison_frequency"), "comparison_frequency")
    if comparison_frequency not in _SUPPORTED_FREQUENCY:
        raise BenchmarkConfigError(
            f"benchmark.comparison_frequency must be '1d' (got: {comparison_frequency!r})"
        )

    library_root_str = _require_str(payload.get("library_root"), "library_root")
    library_root = Path(library_root_str)
    if not library_root.is_absolute():
        raise BenchmarkConfigError(
            f"benchmark.library_root must be an absolute path (got: {library_root_str!r})"
        )

    library_revision = payload.get("library_revision")
    if library_revision is not None:
        library_revision = _require_str(library_revision, "library_revision")
    alignment_policy = _require_str(payload.get("alignment_policy"), "alignment_policy")
    normalization_basis = _require_str(payload.get("normalization_basis"), "normalization_basis")

    return EnabledBenchmarkConfig(
        enabled=True,
        mode=mode,
        id=benchmark_id,
        source=source,
        library_root=library_root,
        library_revision=library_revision,
        frequency=frequency,
        alignment_policy=alignment_policy,
        comparison_frequency=comparison_frequency,
        normalization_basis=normalization_basis,
    )
