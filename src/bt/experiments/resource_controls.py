"""Resource controls for long-running experiment workers.

These helpers only throttle orchestration. They do not change data, strategy,
indicator, or execution semantics.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
import time


@dataclass(frozen=True)
class MemorySnapshot:
    available_gb: float
    total_gb: float
    used_gb: float
    source: str


def memory_snapshot() -> MemorySnapshot | None:
    try:
        import psutil  # type: ignore
    except Exception:
        psutil = None

    if psutil is not None:
        vm = psutil.virtual_memory()
        return MemorySnapshot(
            available_gb=float(vm.available) / (1024**3),
            total_gb=float(vm.total) / (1024**3),
            used_gb=float(vm.used) / (1024**3),
            source="psutil",
        )

    try:
        values: dict[str, float] = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                key, rest = line.split(":", 1)
                values[key] = float(rest.strip().split()[0]) * 1024.0
        total = values.get("MemTotal")
        available = values.get("MemAvailable")
        if total is None or available is None:
            return None
        return MemorySnapshot(
            available_gb=available / (1024**3),
            total_gb=total / (1024**3),
            used_gb=(total - available) / (1024**3),
            source="proc_meminfo",
        )
    except Exception:
        return None


def resolve_auto_workers(
    *,
    requested_max_workers: int,
    use_auto: bool,
    reserve_ram_gb: float = 8.0,
    min_free_ram_gb: float = 6.0,
    max_ram_per_worker_gb: float | None = None,
    estimated_ram_per_worker_gb: float = 2.0,
    cpu_count: int | None = None,
    snapshot: MemorySnapshot | None = None,
) -> int:
    if requested_max_workers <= 0:
        raise ValueError("requested_max_workers must be positive")
    if not use_auto:
        return requested_max_workers

    cpus = cpu_count if cpu_count is not None else (os.cpu_count() or 1)
    cpu_bound = max(cpus - 2, 1)
    mem = snapshot if snapshot is not None else memory_snapshot()
    if mem is None:
        return max(1, min(requested_max_workers, cpu_bound))

    per_worker = max_ram_per_worker_gb or estimated_ram_per_worker_gb
    usable = max(mem.available_gb - reserve_ram_gb, 0.0)
    memory_bound = int(usable // per_worker)
    if mem.available_gb < min_free_ram_gb:
        memory_bound = 1
    return max(1, min(requested_max_workers, cpu_bound, memory_bound))


def wait_for_memory(*, min_free_ram_gb: float, poll_seconds: float = 5.0, logger=None) -> MemorySnapshot | None:
    while True:
        snap = memory_snapshot()
        if snap is None or snap.available_gb >= min_free_ram_gb:
            return snap
        if logger is not None:
            logger(
                "memory backpressure: available_gb=%.2f below min_free_ram_gb=%.2f; sleeping %.1fs"
                % (snap.available_gb, min_free_ram_gb, poll_seconds)
            )
        time.sleep(poll_seconds)
