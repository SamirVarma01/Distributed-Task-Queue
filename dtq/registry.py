import inspect
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable

_TASKS: dict[str, Callable[..., Any]] = {}


@dataclass(frozen=True)
class TaskContext:
    id: str
    # This execution's attempt number. Each claim increments it, so a task can use it as a
    # fencing token: a later attempt always has a higher number than a stale one.
    attempt: int


# Set by the worker while a task function runs.
current_task: ContextVar[TaskContext | None] = ContextVar("current_task", default=None)


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
