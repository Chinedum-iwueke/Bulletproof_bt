"""Indicator registry and factory."""
from __future__ import annotations

from typing import Callable

from bt.indicators.base import Indicator

INDICATOR_REGISTRY: dict[str, type] = {}


def register(name: str) -> Callable[[type], type]:
    key = name.strip().lower()

    def _decorator(cls: type) -> type:
        INDICATOR_REGISTRY[key] = cls
        return cls

    return _decorator


def make_indicator(name: str, **kwargs: object) -> Indicator:
    key = name.strip().lower()
    if key not in INDICATOR_REGISTRY:
        raise KeyError(f"unknown indicator: {name}")
    return INDICATOR_REGISTRY[key](**kwargs)
