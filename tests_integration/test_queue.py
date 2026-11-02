"""The task lifecycle against real MySQL and Redis: the paths tests/test_worker.py checks
with SQLite and fakeredis, here with the real Lua scripts, consumer groups and row locks,
and in one test a worker process killed with SIGKILL."""

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from sqlalchemy import select

from dtq import api, config
from dtq.broker import DELAYED_KEY, stream_key
from dtq.db import SessionLocal, Task, TaskStatus
from dtq.registry import task as register_task

from .conftest import wait_until

pytestmark = pytest.mark.integration

attempt_times: list[float] = []


@register_task()
def fails_every_time() -> None:
    attempt_times.append(time.monotonic())
    raise RuntimeError("always fails")


def get_task(task_id: str) -> Task:
    with SessionLocal() as s:
        return s.get(Task, task_id)


def executions(key: str) -> int:
    """How many times record_execution(key) has run."""
    return api.broker.r.llen(f"dtq:chaos:{key}")


def messages_left(queue: str = "default") -> tuple[int, int]:
    """(pending, in the stream at all): both 0 once every message is acked and deleted."""
    r = api.broker.r
    return r.xpending(stream_key(queue), config.CONSUMER_GROUP)["pending"], r.xlen(stream_key(queue))


def test_submitted_task_runs_and_its_message_is_acked_and_deleted(submit, make_worker):
    task_id = submit("add", {"a": 2, "b": 3})
    w = make_worker()

    assert w.step() is True
    task = get_task(task_id)
    assert (task.status, task.result, task.attempts, task.worker_id) == (TaskStatus.SUCCEEDED, 5, 1, "w1")
    assert task.created_at <= task.enqueued_at <= task.started_at <= task.finished_at
    assert messages_left() == (0, 0)


def test_failing_task_backs_off_exponentially_then_goes_dead(submit, make_worker, monkeypatch):
    base = 0.2
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE_SECONDS", base)
    attempt_times.clear()
    task_id = submit("fails_every_time", max_retries=3)
    w = make_worker()

    def dead():
        w.step(block_ms=20)
        return get_task(task_id).status == TaskStatus.DEAD

    wait_until(dead, timeout=15, interval=0)
    task = get_task(task_id)
    assert task.attempts == 4 and len(attempt_times) == 4
    assert "RuntimeError: always fails" in task.error
    gaps = [b - a for a, b in zip(attempt_times, attempt_times[1:])]
    for n, gap in enumerate(gaps):  # retry n waits base * 2**n, with ±20% jitter
        assert base * 2**n * 0.8 <= gap <= base * 2**n * 1.2 + 0.5, gaps
    assert messages_left() == (0, 0) and w.broker.r.zcard(DELAYED_KEY) == 0


def test_cancelled_task_never_runs_and_its_message_is_dropped(client, submit, make_worker):
    task_id = submit("record_execution", {"key": "cancelled"})
    resp = client.post(f"/tasks/{task_id}/cancel")
    assert resp.status_code == 200 and resp.json()["status"] == "cancelled"

    make_worker().step()
    task = get_task(task_id)
    assert (task.status, task.attempts) == (TaskStatus.CANCELLED, 0)
    assert executions("cancelled") == 0
    assert messages_left() == (0, 0)


def test_duplicate_message_is_skipped_by_the_conditional_claim(submit, make_worker):
    task_id = submit("record_execution", {"key": "dup"})
    api.broker.enqueue("default", task_id)  # a second message for the same task
    w = make_worker()

    while w.step():
        pass
    task = get_task(task_id)
    assert (task.status, task.attempts) == (TaskStatus.SUCCEEDED, 1)
    assert executions("dup") == 1
    assert messages_left() == (0, 0)


def test_two_workers_claiming_the_same_task_at_once_run_it_once(submit, make_worker):
    """Each worker holds a message for the same task and both claim it at the same
    moment. MySQL locks the row for the first conditional UPDATE; the second then finds
    the task already running and matches nothing."""
    workers = [make_worker("w1"), make_worker("w2")]
    for i in range(20):
        task_id = submit("record_execution", {"key": f"race-{i}"})
        api.broker.enqueue("default", task_id)
        held = [w.broker.read(w.id, ["default"]) for w in workers]
        barrier = Barrier(len(workers))

        def handle(w, msg):
            barrier.wait()
            w.handle(*msg)

        with ThreadPoolExecutor(len(workers)) as pool:
            list(pool.map(handle, workers, held))  # re-raises a worker's exception
        assert executions(f"race-{i}") == 1
        assert get_task(task_id).attempts == 1
    assert messages_left() == (0, 0)


def test_reconciler_sends_a_task_the_api_crashed_before_sending(client, make_worker, monkeypatch):
    class Crash(Exception):
        pass

    def crash(*args, **kwargs):
        raise Crash("the API process died between the INSERT and the XADD")

    w = make_worker()
    with monkeypatch.context() as m:
        m.setattr(api.broker, "send", crash)
        with pytest.raises(Crash):
            client.post("/tasks", json={"name": "record_execution", "payload": {"key": "lost"}})
    with SessionLocal() as s:
        [task] = s.scalars(select(Task)).all()
    assert (task.status, task.enqueued_at) == (TaskStatus.QUEUED, None)
    assert messages_left() == (0, 0)

    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(config, "RECONCILE_AFTER_SECONDS", 0)
    assert w.step() is True
    task = get_task(task.id)
    assert task.status == TaskStatus.SUCCEEDED and task.enqueued_at is not None
    assert executions("lost") == 1
    assert messages_left() == (0, 0)


def test_task_of_a_killed_worker_process_is_reclaimed_and_finished(submit, worker_process, tmp_path):
    """SIGKILL a worker process mid-task. Its message stays pending under it, with no
    more heartbeats; once it has been idle for the visibility timeout, another worker
    process takes it over with XAUTOCLAIM and runs the task again."""
    env = {"VISIBILITY_TIMEOUT_MS": 2000, "HEARTBEAT_SECONDS": 0.5, "RECLAIM_INTERVAL_SECONDS": 0.5}
    task_id = submit("record_execution", {"key": "crash", "ms": 3000})
    first = worker_process("first", **env)

    wait_until(lambda: executions("crash") == 1)  # started its 3 s of work
    first.kill()
    first.wait()
    assert get_task(task_id).status == TaskStatus.RUNNING
    assert messages_left() == (1, 1)

    second = worker_process("second", **env)

    def succeeded():
        task = get_task(task_id)
        return task if task.status == TaskStatus.SUCCEEDED else None

    task = wait_until(succeeded, timeout=60)
    assert task.attempts == 2 and task.worker_id.endswith(f"-{second.pid}")
    assert executions("crash") == 2
    assert messages_left() == (0, 0)
    assert f"reclaimed {task_id} from a dead worker" in (tmp_path / "second.log").read_text()
