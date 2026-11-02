import inspect
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable

from . import config

_TASKS: dict[str, Callable[..., Any]] = {}
_ON_DEAD: dict[str, Callable[..., None]] = {}
_UNCANCELLABLE: set[str] = set()
_TIMEOUTS: dict[str, float | None] = {}

DEFAULT_TIMEOUT: Any = object()  # task(timeout=...) left out: use DEFAULT_TASK_TIMEOUT_SECONDS


@dataclass(frozen=True)
class TaskContext:
    id: str
    # This execution's attempt number. Each claim increments it, so a task can use it
    # as a fencing token: a later attempt always has a higher number than a stale one.
    attempt: int


# Set by the worker while a task function runs.
current_task: ContextVar[TaskContext | None] = ContextVar("current_task", default=None)


def task(name: str | None = None, *, on_dead: Callable[..., None] | None = None,
         cancellable: bool = True, timeout: float | None = DEFAULT_TIMEOUT):
    """Register a function as a task. It is called with the task payload as kwargs.

    timeout is how many seconds an attempt may run before the worker kills it. A
    timed-out attempt is a failed one: it is retried with backoff and counts toward
    max_retries. A task with a timeout runs in the worker's child process (dtq/runner.py),
    so it must be a module-level function and return something picklable. Left out, it
    is DEFAULT_TASK_TIMEOUT_SECONDS (no timeout when that is unset); None means no
    timeout even when a default is set.

    on_dead(session, task, error) is called when the task is marked dead (retries used
    up, a poison task, a bad payload), inside the transaction that marks it, so what
    it records commits together with the dead status. If it raises, the task is not
    marked dead: its message stays pending and is retried later.

    cancellable=False makes POST /tasks/{id}/cancel refuse the task, for tasks that
    something else schedules and tracks, which would be stranded if one were cancelled."""

    if timeout is not DEFAULT_TIMEOUT and timeout is not None and not timeout > 0:
        raise ValueError(f"timeout must be a positive number of seconds or None, not {timeout!r}")

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        key = name or fn.__name__
        _TASKS[key] = fn
        if on_dead:
            _ON_DEAD[key] = on_dead
        else:
            _ON_DEAD.pop(key, None)
        if cancellable:
            _UNCANCELLABLE.discard(key)
        else:
            _UNCANCELLABLE.add(key)
        if timeout is DEFAULT_TIMEOUT:
            _TIMEOUTS.pop(key, None)
        else:
            _TIMEOUTS[key] = timeout
        return fn

    return decorator


def get(name: str) -> Callable[..., Any] | None:
    return _TASKS.get(name)


def on_dead(name: str) -> Callable[..., None] | None:
    return _ON_DEAD.get(name)


def timeout(name: str) -> float | None:
    """Seconds the task may run per attempt, or None for no limit."""
    return _TIMEOUTS[name] if name in _TIMEOUTS else config.DEFAULT_TASK_TIMEOUT_SECONDS


def uncancellable() -> list[str]:
    return sorted(_UNCANCELLABLE)


def names() -> list[str]:
    return sorted(_TASKS)


def check_payload(fn: Callable[..., Any], payload: dict[str, Any]) -> None:
    """Raise TypeError if the payload's fields don't fit fn's parameters (missing or
    unexpected arguments). Such a task can never succeed, so it is not worth retrying."""
    inspect.signature(fn).bind(**payload)
