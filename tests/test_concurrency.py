"""Per-worker concurrency (docs/worker-concurrency-design.md): one worker runs up to
`concurrency` tasks at once, each in a slot thread, and takes a message only while a
slot is free. These tests run workers with run(), in a thread, as the worker process
does."""

import os
import signal
import threading
import time

import pytest

from dtq import config
from dtq.db import TaskStatus
from dtq.registry import task as register_task
from dtq.worker import Worker

from . import timeout_tasks  # noqa: F401  (registers the timed tasks)
from .conftest import get_task, pending_count

gate = threading.Event()  # slot_gated tasks run until a test sets it
_lock = threading.Lock()
running = {"now": 0, "max": 0}


class _Running:
    def __enter__(self):
        with _lock:
            running["now"] += 1
            running["max"] = max(running["max"], running["now"])

    def __exit__(self, *exc):
        with _lock:
            running["now"] -= 1


@register_task()
def slot_sleep(seconds: float) -> None:
    with _Running():
        time.sleep(seconds)


@register_task()
def slot_gated() -> None:
    with _Running():
        gate.wait(30)


@pytest.fixture(autouse=True)
def fast_config(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE_SECONDS", 0)
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", float("inf"))
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", float("inf"))
    gate.clear()
    running.update(now=0, max=0)


@pytest.fixture
def run_worker(make_worker, monkeypatch):
    """Start a worker's run() loop in a thread. At the end, release any gated tasks,
    ask the worker to stop, and wait for it."""
    monkeypatch.setattr(signal, "signal", lambda *args: None)  # run() is called off the main thread
    started = []

    def start(concurrency: int, name: str = "w1") -> tuple[Worker, threading.Thread]:
        w = make_worker(name, concurrency)
        thread = threading.Thread(target=w.run)
        thread.start()
        started.append((w, thread))
        return w, thread

    yield start
    gate.set()
    for w, thread in started:
        w.stopping = True
        thread.join(30)


def wait_until(condition, timeout: float = 10):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {condition.__name__}")
        time.sleep(0.01)


def statuses(ids: list[str]) -> list[TaskStatus]:
    return [get_task(i).status for i in ids]


def all_succeeded(ids):
    def succeeded():
        return statuses(ids) == [TaskStatus.SUCCEEDED] * len(ids)
    return succeeded


def test_n_slots_run_n_tasks_at_once(submit, run_worker):
    ids = [submit("slot_sleep", {"seconds": 0.5}) for _ in range(4)]
    started = time.monotonic()
    run_worker(concurrency=4)

    wait_until(all_succeeded(ids))
    assert running["max"] == 4
    assert time.monotonic() - started < 1.5  # one at a time would take 2 s


def test_worker_holds_no_more_messages_than_it_has_slots(submit, run_worker):
    ids = [submit("slot_gated") for _ in range(5)]
    w, _ = run_worker(concurrency=2)

    wait_until(lambda: running["now"] == 2)
    time.sleep(0.3)  # several turns of the dispatcher's loop: it must not take more
    assert pending_count(w.broker, "default") == 2
    assert sorted(statuses(ids)) == sorted([TaskStatus.RUNNING] * 2 + [TaskStatus.QUEUED] * 3)

    gate.set()
    wait_until(all_succeeded(ids))
    assert running["max"] == 2 and pending_count(w.broker, "default") == 0


def test_a_stalled_slot_does_not_block_the_others(submit, run_worker):
    stalled = submit("slot_gated")
    run_worker(concurrency=2)
    wait_until(lambda: running["now"] == 1)

    quick = [submit("add", {"a": i, "b": 1}) for i in range(5)]
    wait_until(all_succeeded(quick))
    assert get_task(stalled).status == TaskStatus.RUNNING

    gate.set()
    wait_until(all_succeeded([stalled]))


def test_heartbeat_covers_every_in_flight_message(submit, run_worker, make_worker, monkeypatch):
    monkeypatch.setattr(config, "VISIBILITY_TIMEOUT_MS", 300)
    monkeypatch.setattr(config, "HEARTBEAT_SECONDS", 0.05)
    ids = [submit("slot_gated") for _ in range(3)]
    run_worker(concurrency=3)
    wait_until(lambda: running["now"] == 3)

    other = make_worker("other")
    reclaimed = []
    deadline = time.monotonic() + 1.2  # four visibility timeouts
    while time.monotonic() < deadline:
        reclaimed.append(other.broker.reclaim(other.id, "default", config.VISIBILITY_TIMEOUT_MS))
        time.sleep(0.05)
    assert not any(reclaimed)

    gate.set()
    wait_until(all_succeeded(ids))
    assert [get_task(i).attempts for i in ids] == [1, 1, 1]


def test_shutdown_finishes_in_flight_tasks_and_takes_no_more(submit, run_worker):
    ids = [submit("slot_sleep", {"seconds": 0.5}) for _ in range(5)]
    w, thread = run_worker(concurrency=3)
    wait_until(lambda: running["now"] == 3)

    w._request_stop()  # what SIGTERM does
    thread.join(10)
    assert not thread.is_alive()
    assert sorted(statuses(ids)) == sorted([TaskStatus.SUCCEEDED] * 3 + [TaskStatus.QUEUED] * 2)
    assert pending_count(w.broker, "default") == 0


def test_each_slot_has_its_own_child_process(submit, run_worker):
    ids = [submit("timed_where_it_ran", {"seconds": 1}) for _ in range(2)]
    run_worker(concurrency=2)

    wait_until(all_succeeded(ids), timeout=20)
    pids = {get_task(i).result["pid"] for i in ids}
    assert len(pids) == 2 and os.getpid() not in pids


def test_a_timeout_kills_only_its_own_slots_child(submit, run_worker):
    slow = submit("timed_where_it_ran", {"seconds": 1.5})
    overrun = submit("timed_overruns", max_retries=0)
    run_worker(concurrency=2)

    wait_until(lambda: get_task(slow).status == TaskStatus.SUCCEEDED, timeout=20)
    task = get_task(overrun)
    assert (task.status, task.error) == (TaskStatus.DEAD, "TimeoutError: exceeded 0.5s")


def test_reclaim_takes_one_stale_message_per_free_slot(submit, make_worker, monkeypatch):
    ids = [submit("add", {"a": i, "b": 1}) for i in range(3)]
    crashed = make_worker("crashed")
    for _ in ids:
        crashed.broker.read(crashed.id, config.QUEUES)  # takes all three, then "dies"
    monkeypatch.setattr(config, "VISIBILITY_TIMEOUT_MS", 0)
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", 0)
    survivor = make_worker("survivor")

    for i in range(3):
        assert survivor.step() is True
        assert pending_count(survivor.broker, "default") == 3 - (i + 1)  # took one, finished it
    assert survivor.step() is False
    assert {get_task(i).worker_id for i in ids} == {"survivor"}


@pytest.mark.parametrize("concurrency", [0, -1])
def test_concurrency_must_be_at_least_one(concurrency):
    with pytest.raises(ValueError):
        Worker(concurrency)
