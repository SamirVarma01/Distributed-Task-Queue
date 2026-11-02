"""Per-task timeouts: a task with a timeout runs in the worker's child process
(dtq/runner.py), which is killed if an attempt overruns."""

import os
import signal
import threading
import time

import pytest

from dtq import config, registry
from dtq.db import TaskStatus
from dtq.registry import task as register_task
from dtq.runner import TaskProcess

from . import timeout_tasks  # noqa: F401  (registers the tasks)
from .conftest import get_task, pending_count


@pytest.fixture(autouse=True)
def fast_config(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE_SECONDS", 0)
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", float("inf"))
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", float("inf"))


@pytest.fixture
def worker(make_worker):
    return make_worker()


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_task_within_its_timeout_succeeds_in_the_child_process(submit, worker):
    first, second = submit("timed_where_it_ran"), submit("timed_where_it_ran")
    worker.step()
    worker.step()

    a, b = get_task(first), get_task(second)
    assert (a.status, b.status) == (TaskStatus.SUCCEEDED, TaskStatus.SUCCEEDED)
    assert a.result == {"pid": worker.task_process.pid, "task_id": first, "attempt": 1}
    assert a.result["pid"] != os.getpid()
    assert b.result["pid"] == a.result["pid"]  # one child, reused


def test_task_that_overruns_its_timeout_is_killed_and_retried(submit, worker):
    task_id = submit("timed_overruns", max_retries=2)
    worker.task_process.wait_ready()
    child = worker.task_process.pid

    started = time.monotonic()
    worker.step()
    assert time.monotonic() - started < 5  # cut off at 0.5 s, not left to sleep for 30
    task = get_task(task_id)
    assert (task.status, task.attempts, task.error) == (TaskStatus.RETRYING, 1, "TimeoutError: exceeded 0.5s")
    assert not alive(child)
    assert worker.task_process.pid not in (None, child)  # already replaced


def test_timeouts_count_toward_max_retries(submit, worker):
    task_id = submit("timed_overruns", max_retries=1)
    while worker.step():
        pass

    task = get_task(task_id)
    assert (task.status, task.attempts, task.error) == (TaskStatus.DEAD, 2, "TimeoutError: exceeded 0.5s")


def test_heartbeat_keeps_the_message_while_the_child_runs(submit, worker, make_worker, monkeypatch):
    monkeypatch.setattr(config, "VISIBILITY_TIMEOUT_MS", 300)
    monkeypatch.setattr(config, "HEARTBEAT_SECONDS", 0.05)
    task_id = submit("timed_where_it_ran", {"seconds": 1.5})
    other = make_worker("other")

    running = threading.Thread(target=worker.step)
    running.start()
    reclaimed = []
    while running.is_alive():  # another worker tries to take it over the whole time
        reclaimed.append(other.broker.reclaim(other.id, "default", config.VISIBILITY_TIMEOUT_MS))
        time.sleep(0.05)
    running.join()

    assert len(reclaimed) > 20 and not any(reclaimed)
    task = get_task(task_id)
    assert (task.status, task.attempts, task.worker_id) == (TaskStatus.SUCCEEDED, 1, "w1")


def test_default_timeout_applies_to_tasks_without_their_own(submit, worker, monkeypatch):
    monkeypatch.setattr(config, "DEFAULT_TASK_TIMEOUT_SECONDS", 0.5)
    assert (registry.timeout("no_timeout_of_its_own"), registry.timeout("timed_where_it_ran"),
            registry.timeout("never_times_out")) == (0.5, 10, None)

    slow = submit("no_timeout_of_its_own", max_retries=0)
    worker.step()
    assert (get_task(slow).status, get_task(slow).error) == (TaskStatus.DEAD, "TimeoutError: exceeded 0.5s")

    exempt = submit("never_times_out")
    worker.step()
    assert get_task(exempt).result == os.getpid()  # timeout=None: ran in the worker process itself


def test_exception_in_the_child_is_recorded_with_its_traceback(submit, worker):
    task_id = submit("timed_raises", max_retries=0)
    worker.step()

    task = get_task(task_id)
    assert task.status == TaskStatus.DEAD
    assert task.error.startswith("Traceback (most recent call last):")
    assert "timeout_tasks.py" in task.error and task.error.rstrip().endswith("ValueError: bad input")


def test_child_that_dies_fails_the_attempt_and_is_replaced(submit, worker):
    dies = submit("timed_exits", max_retries=0)
    worker.step()
    task = get_task(dies)
    assert (task.status, task.error) == (TaskStatus.DEAD, "the task's process exited with code 3")

    after = submit("timed_where_it_ran")
    worker.step()
    assert get_task(after).status == TaskStatus.SUCCEEDED


def test_result_mysql_json_cannot_hold_is_stored_as_repr(submit, worker):
    task_id = submit("timed_returns_nan")
    worker.step()
    assert get_task(task_id).result == "nan"


def test_child_that_cannot_start_leaves_the_message_pending(submit, worker, monkeypatch):
    """Like a crash: the task isn't failed for it; its message is reclaimed later."""
    def broken(self):
        raise RuntimeError("the task process didn't start")

    monkeypatch.setattr(TaskProcess, "wait_ready", broken)
    task_id = submit("timed_where_it_ran")
    with pytest.raises(RuntimeError):
        worker.step()

    task = get_task(task_id)
    assert (task.status, task.error) == (TaskStatus.RUNNING, None)
    assert pending_count(worker.broker, "default") == 1


def test_worker_stops_its_child_when_it_exits(worker, monkeypatch):
    monkeypatch.setattr(signal, "signal", lambda *args: None)  # leave pytest's handlers alone
    worker.task_process.wait_ready()
    child = worker.task_process.pid

    worker.stopping = True
    worker.run()
    assert not alive(child) and worker.task_process.pid is None


@pytest.mark.parametrize("timeout", [0, -1])
def test_timeout_must_be_positive(timeout):
    with pytest.raises(ValueError):
        register_task("bad_timeout", timeout=timeout)
