"""Tasks for tests/test_timeouts.py. Tasks with a timeout run in the worker's child
process, which imports this module by name, so it imports nothing from the test suite
(tests/conftest.py replaces Redis and MySQL as it is imported)."""

import os
import time

from dtq.registry import current_task, task


@task(timeout=0.5)
def timed_overruns(seconds: float = 30) -> None:
    time.sleep(seconds)


@task(timeout=10)
def timed_where_it_ran(seconds: float = 0) -> dict:
    time.sleep(seconds)
    context = current_task.get()
    return {"pid": os.getpid(), "task_id": context.id, "attempt": context.attempt}


@task(timeout=10)
def timed_raises() -> None:
    raise ValueError("bad input")


@task(timeout=10)
def timed_exits() -> None:
    os._exit(3)


@task(timeout=10)
def timed_returns_nan() -> float:
    return float("nan")


@task()
def no_timeout_of_its_own(seconds: float = 30) -> None:
    time.sleep(seconds)


@task(timeout=None)
def never_times_out() -> int:
    return os.getpid()
