from __future__ import annotations

from typing import Callable

TaskBuilder = Callable[..., "object"]

_registry: dict[tuple[str, str], TaskBuilder] = {}


def register(stage: str, type_: str) -> Callable[[TaskBuilder], TaskBuilder]:
    def decorator(fn: TaskBuilder) -> TaskBuilder:
        key = (stage, type_)
        if key in _registry:
            raise ValueError(
                f"Task builder already registered for stage={stage!r}, type={type_!r}: "
                f"{_registry[key].__module__}.{_registry[key].__name__}"
            )
        _registry[key] = fn
        return fn

    return decorator


def get_builder(stage: str, type_: str) -> TaskBuilder:
    try:
        return _registry[(stage, type_)]
    except KeyError as exc:
        available = sorted(t for s, t in _registry if s == stage)
        raise KeyError(
            f"No builder registered for stage={stage!r}, type={type_!r}. "
            f"Available types for {stage}: {available}"
        ) from exc


def all_builders() -> dict[tuple[str, str], TaskBuilder]:
    return dict(_registry)
