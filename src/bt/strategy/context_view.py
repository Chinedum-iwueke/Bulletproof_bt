"""Read-only strategy context wrappers."""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

_READONLY_ERROR = "Strategy context is read-only. Mutations are forbidden."


def _freeze(value: Any) -> Any:
    if isinstance(value, FrozenDict):
        return value
    if isinstance(value, dict):
        return FrozenDict(value)
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value


class FrozenDict(Mapping[str, Any]):
    """Mapping wrapper that recursively freezes nested mutable containers."""

    def __init__(self, data: Mapping[str, Any] | dict[str, Any]) -> None:
        self._data = {key: _freeze(value) for key, value in dict(data).items()}

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"FrozenDict({self._data!r})"

    def __setitem__(self, key: str, value: Any) -> None:
        raise TypeError(_READONLY_ERROR)

    def __delitem__(self, key: str) -> None:
        raise TypeError(_READONLY_ERROR)

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(_READONLY_ERROR)

    def pop(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(_READONLY_ERROR)

    def clear(self) -> None:
        raise TypeError(_READONLY_ERROR)

    def setdefault(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(_READONLY_ERROR)


class StrategyContextView(Mapping[str, Any]):
    """Thin read-only context view exposed to strategies."""

    def __init__(self, data: Mapping[str, Any]) -> None:
        self._raw = FrozenDict(data)

    @property
    def raw(self) -> FrozenDict:
        return self._raw

    def __getitem__(self, key: str) -> Any:
        return self._raw[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._raw)

    def __len__(self) -> int:
        return len(self._raw)

    def __repr__(self) -> str:
        return f"StrategyContextView({self._raw!r})"

    def __setitem__(self, key: str, value: Any) -> None:
        raise TypeError(_READONLY_ERROR)

    def __delitem__(self, key: str) -> None:
        raise TypeError(_READONLY_ERROR)

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(_READONLY_ERROR)

    def pop(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(_READONLY_ERROR)

    def clear(self) -> None:
        raise TypeError(_READONLY_ERROR)

    def setdefault(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(_READONLY_ERROR)
