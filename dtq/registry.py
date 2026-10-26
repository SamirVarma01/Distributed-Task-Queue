import inspect
from typing import Any, Callable

_TASKS: dict[str, Callable[..., Any]] = {}


def task(name: str | None = None):
    """Register a function as a task. It is called with the task payload as kwargs."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        _TASKS[name or fn.__name__] = fn
        return fn

    return decorator


def get(name: str) -> Callable[..., Any] | None:
    return _TASKS.get(name)


def names() -> list[str]:
    return sorted(_TASKS)


def check_payload(fn: Callable[..., Any], payload: dict[str, Any]) -> None:
    """Raise TypeError if the payload's fields don't fit fn's parameters (missing or
    unexpected arguments). Such a task can never succeed, so it is not worth retrying."""
    inspect.signature(fn).bind(**payload)
