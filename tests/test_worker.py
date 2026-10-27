from datetime import timedelta

import pytest
from sqlalchemy import update

from dtq import config
from dtq.db import SessionLocal, Task, TaskStatus, utcnow
from dtq.registry import task as register_task

from .conftest import get_task, pending_count


@pytest.fixture(autouse=True)
def fast_config(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE_SECONDS", 0)
    # Only reclaim or reconcile when a test asks for it.
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", float("inf"))
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", float("inf"))


def insert_task(**fields) -> str:
    """Insert a row directly, as if the API crashed before sending it to Redis."""
    with SessionLocal() as s:
        task = Task(name="add", queue="default", payload={"a": 1, "b": 1}, **fields)
        s.add(task)
        s.commit()
        return task.id


def drain(worker) -> None:
    while worker.step():
        pass


def test_runs_task_and_acks(submit, make_worker):
    task_id = submit("add", {"a": 2, "b": 3})
    w = make_worker()

    assert w.step() is True
    task = get_task(task_id)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.result == 5
    assert task.attempts == 1
    assert w.broker.r.xlen("dtq:stream:default") == 0
    assert pending_count(w.broker, "default") == 0
    assert w.step() is False


def test_failed_task_retries_then_goes_dead(submit, make_worker):
    task_id = submit("flaky", {"fail_rate": 1.0}, max_retries=2)
    w = make_worker()

    w.step()
    task = get_task(task_id)
    assert task.status == TaskStatus.RETRYING
    assert w.broker.r.zcard("dtq:delayed") == 1

    drain(w)
    task = get_task(task_id)
    assert task.status == TaskStatus.DEAD
    assert task.attempts == 3
    assert "random failure" in task.error


def test_higher_priority_queue_runs_first(submit, make_worker):
    low = submit("add", {"a": 1, "b": 1}, queue="low")
    high = submit("add", {"a": 1, "b": 1}, queue="high")
    w = make_worker()

    w.step()
    assert get_task(high).status == TaskStatus.SUCCEEDED
    assert get_task(low).status == TaskStatus.QUEUED


def test_read_takes_one_message_at_a_time(submit, make_worker):
    """A worker must never hold messages it isn't working on: they get no heartbeat,
    so another worker would reclaim them and run them a second time."""
    for q in config.QUEUES:
        submit("add", {"a": 1, "b": 1}, queue=q)
    w = make_worker()

    assert w.broker.read(w.id, config.QUEUES) is not None
    assert sum(pending_count(w.broker, q) for q in config.QUEUES) == 1


def test_cancelled_task_is_skipped(client, submit, make_worker):
    task_id = submit("add", {"a": 1, "b": 1})
    client.post(f"/tasks/{task_id}/cancel")
    w = make_worker()

    w.step()
    task = get_task(task_id)
    assert task.status == TaskStatus.CANCELLED
    assert task.attempts == 0
    assert pending_count(w.broker, "default") == 0


def test_crashed_workers_message_is_reclaimed(submit, make_worker, monkeypatch):
    task_id = submit("add", {"a": 2, "b": 2})
    crashed = make_worker("crashed")
    crashed.broker.read(crashed.id, config.QUEUES)  # takes the message, then "dies"

    monkeypatch.setattr(config, "VISIBILITY_TIMEOUT_MS", 0)
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", 0)
    survivor = make_worker("survivor")
    survivor.step()

    task = get_task(task_id)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.worker_id == "survivor"
    assert pending_count(survivor.broker, "default") == 0


def test_message_stays_pending_when_recording_the_result_fails(submit, make_worker, monkeypatch):
    """If MySQL or Redis fails while a task's outcome is being saved, the message must
    not be acked, so the task is recovered instead of being stuck in 'running'."""
    task_id = submit("add", {"a": 1, "b": 1})
    w = make_worker()

    def db_down(*_, **__):
        raise ConnectionError("mysql is down")

    with monkeypatch.context() as m:
        m.setattr(w, "_finish", db_down)
        with pytest.raises(ConnectionError):
            w.step()
    assert get_task(task_id).status == TaskStatus.RUNNING
    assert pending_count(w.broker, "default") == 1

    monkeypatch.setattr(config, "VISIBILITY_TIMEOUT_MS", 0)
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", 0)
    make_worker("w2").step()
    task = get_task(task_id)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.attempts == 2
    assert pending_count(w.broker, "default") == 0


def test_task_that_keeps_crashing_its_worker_goes_dead(submit, make_worker):
    task_id = submit("add", {"a": 1, "b": 1}, max_retries=1)
    # Simulate two earlier attempts that each died with their worker.
    with SessionLocal() as s:
        s.execute(update(Task).where(Task.id == task_id).values(attempts=2))
        s.commit()

    make_worker().step()
    task = get_task(task_id)
    assert task.status == TaskStatus.DEAD
    assert "worker crashes" in task.error


def test_bad_payload_goes_dead_without_retrying(make_worker):
    task_id = insert_task(enqueued_at=utcnow())
    with SessionLocal() as s:
        s.execute(update(Task).where(Task.id == task_id).values(payload={"a": 1}))
        s.commit()
    w = make_worker()
    w.broker.enqueue("default", task_id)

    w.step()
    task = get_task(task_id)
    assert task.status == TaskStatus.DEAD
    assert "bad payload" in task.error
    assert w.broker.r.zcard("dtq:delayed") == 0


def test_fresh_message_for_running_task_is_dropped(make_worker):
    """A duplicate message (e.g. from the reconciler) must not start a second run."""
    task_id = insert_task(status=TaskStatus.RUNNING, attempts=1, enqueued_at=utcnow())
    w = make_worker()
    w.broker.enqueue("default", task_id)

    w.step()
    task = get_task(task_id)
    assert task.status == TaskStatus.RUNNING
    assert task.attempts == 1
    assert pending_count(w.broker, "default") == 0


def test_reconciler_resends_tasks_that_never_reached_redis(make_worker, monkeypatch):
    old = utcnow() - timedelta(minutes=5)
    lost = insert_task(created_at=old)
    lost_delayed = insert_task(created_at=old, run_at=utcnow() + timedelta(hours=1))
    being_submitted = insert_task()  # too recent: the API may still be sending it
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", 0)
    w = make_worker()

    w.step()
    assert get_task(lost).status == TaskStatus.SUCCEEDED
    assert get_task(lost_delayed).enqueued_at is not None
    assert w.broker.r.zscore("dtq:delayed", f"default|{lost_delayed}") is not None
    assert get_task(being_submitted).enqueued_at is None
    assert w.broker.r.xlen("dtq:stream:default") == 0


def test_benchmark_tasks(submit, make_worker):
    noop = submit("noop")
    sleep = submit("sleep_ms", {"ms": 1})
    drain(make_worker())

    assert get_task(noop).status == TaskStatus.SUCCEEDED
    assert get_task(sleep).status == TaskStatus.SUCCEEDED


def test_task_costs_three_statements_two_commits_and_no_rollbacks(submit, make_worker, monkeypatch):
    """Claim = UPDATE + SELECT + COMMIT, result = UPDATE + COMMIT. Guards against the
    extra transaction and pool-reset ROLLBACKs that used to cost 4 round trips a task."""
    from sqlalchemy import event

    from dtq.db import engine

    submit("noop")
    w = make_worker()
    calls = {"statements": [], "commit": 0, "rollback": 0}
    real_commit, real_rollback = engine.dialect.do_commit, engine.dialect.do_rollback

    def commit(conn):
        calls["commit"] += 1
        real_commit(conn)

    def rollback(conn):
        calls["rollback"] += 1
        real_rollback(conn)

    def record(conn, cursor, statement, *_):
        calls["statements"].append(statement.split()[0])

    monkeypatch.setattr(engine.dialect, "do_commit", commit)
    monkeypatch.setattr(engine.dialect, "do_rollback", rollback)
    event.listen(engine, "before_cursor_execute", record)
    try:
        assert w.step() is True
    finally:
        event.remove(engine, "before_cursor_execute", record)

    assert calls == {"statements": ["UPDATE", "SELECT", "UPDATE"], "commit": 2, "rollback": 0}


def test_blocking_read_keeps_highest_priority_and_hands_back_the_rest(submit, make_worker):
    """A blocking XREADGROUP over several streams returns one message from each stream
    that has one. The worker must keep only the highest-priority message and hand the
    others back, so it never holds a message it isn't working on."""
    low = submit("noop", queue="low")
    default = submit("noop", queue="default")
    high = submit("noop", queue="high")
    w = make_worker()

    stream, msg_id, fields = w.broker._read_blocking(w.id, config.QUEUES, block_ms=100)
    assert fields["task_id"] == high
    assert sum(pending_count(w.broker, q) for q in config.QUEUES) == 1
    assert w.broker.r.xlen("dtq:stream:low") == 1
    assert w.broker.r.xlen("dtq:stream:default") == 1

    w.handle(stream, msg_id, fields)
    assert w.step(block_ms=100) is True
    assert get_task(default).status == TaskStatus.SUCCEEDED
    assert get_task(low).status == TaskStatus.QUEUED
    assert w.step(block_ms=100) is True
    assert get_task(low).status == TaskStatus.SUCCEEDED
    assert sum(pending_count(w.broker, q) for q in config.QUEUES) == 0


def test_step_with_blocking_read_still_takes_priority_order(submit, make_worker):
    low = submit("noop", queue="low")
    high = submit("noop", queue="high")
    w = make_worker()

    assert w.step(block_ms=100) is True
    assert get_task(high).status == TaskStatus.SUCCEEDED
    assert get_task(low).status == TaskStatus.QUEUED
    assert w.step(block_ms=100) is True
    assert w.step(block_ms=100) is False


def reclaim_and_run(worker, monkeypatch):
    """Let worker take over every pending message, as if their owners had stalled."""
    monkeypatch.setattr(config, "VISIBILITY_TIMEOUT_MS", 0)
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", 0)
    worker.step()


def test_stalled_worker_cannot_overwrite_the_result_of_the_worker_that_took_over(submit, make_worker, monkeypatch):
    task_id = submit("noop")
    stalled, survivor = make_worker("stalled"), make_worker("survivor")
    stalled.broker.read(stalled.id, config.QUEUES)
    stale = stalled._claim(task_id, reclaimed=False)  # attempt 1, then the worker stalls

    reclaim_and_run(survivor, monkeypatch)  # attempt 2 succeeds
    assert get_task(task_id).status == TaskStatus.SUCCEEDED

    stalled._on_failure(stale, "late failure from attempt 1")
    task = get_task(task_id)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.error is None
    assert stalled.broker.r.zcard("dtq:delayed") == 0  # no retry scheduled

    stalled._finish(stale, TaskStatus.SUCCEEDED, result="from attempt 1")
    assert get_task(task_id).result is None


def test_stalled_worker_cannot_fail_a_task_another_worker_is_running(submit, make_worker, monkeypatch):
    task_id = submit("noop")
    stalled, survivor = make_worker("stalled"), make_worker("survivor")
    stalled.broker.read(stalled.id, config.QUEUES)
    stale = stalled._claim(task_id, reclaimed=False)
    survivor._claim(task_id, reclaimed=True)  # survivor's attempt 2 is now running

    stalled._on_failure(stale, "late failure from attempt 1")
    task = get_task(task_id)
    assert (task.status, task.attempts, task.worker_id) == (TaskStatus.RUNNING, 2, "survivor")
    assert stalled.broker.r.zcard("dtq:delayed") == 0


def owner(broker, stream: str, msg_id: str) -> str | None:
    pending = broker.r.xpending_range(stream, config.CONSUMER_GROUP, msg_id, msg_id, 1)
    return pending[0]["consumer"] if pending else None


def test_stalled_worker_cannot_heartbeat_or_ack_a_reclaimed_message(submit, make_worker):
    submit("noop")
    stalled, survivor = make_worker("stalled"), make_worker("survivor")
    stream, msg_id, _ = stalled.broker.read(stalled.id, config.QUEUES)
    assert survivor.broker.reclaim(survivor.id, "default", min_idle_ms=0) is not None

    assert stalled.broker.touch(stalled.id, stream, msg_id) is False
    assert stalled.broker.ack(stalled.id, stream, msg_id) is False
    assert owner(survivor.broker, stream, msg_id) == "survivor"

    assert survivor.broker.touch(survivor.id, stream, msg_id) is True
    assert survivor.broker.ack(survivor.id, stream, msg_id) is True
    assert pending_count(survivor.broker, "default") == 0
    assert survivor.broker.r.xlen(stream) == 0


def test_stalled_worker_does_not_delete_the_message_another_worker_is_running(submit, make_worker):
    """If the stalled worker acked the message the survivor is running and the survivor
    then crashed, nothing would be left to deliver the task: stuck in 'running' forever."""
    task_id = submit("noop")
    stalled, survivor = make_worker("stalled"), make_worker("survivor")
    stream, msg_id, fields = stalled.broker.read(stalled.id, config.QUEUES)
    survivor_msg = survivor.broker.reclaim(survivor.id, "default", min_idle_ms=0)
    survivor._claim(task_id, reclaimed=True)  # survivor is running the task

    stalled.handle(stream, msg_id, fields)  # its fresh claim is refused, so it acks
    assert owner(survivor.broker, stream, msg_id) == "survivor"

    survivor.handle(*survivor_msg, reclaimed=True)  # e.g. after the survivor crashed
    assert get_task(task_id).status == TaskStatus.SUCCEEDED
    assert pending_count(survivor.broker, "default") == 0


@register_task("returns_nan")
def returns_nan() -> dict:
    return {"score": float("nan")}


def test_result_mysql_json_cannot_hold_is_stored_as_repr(submit, make_worker):
    """MySQL's JSON type rejects NaN/Infinity (error 3140), so saving such a result
    failed on every attempt until the task went dead with a misleading error."""
    task_id = submit("returns_nan")
    make_worker().step()

    task = get_task(task_id)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.result == "{'score': nan}"


def test_record_execution_records_every_run(submit, make_worker, monkeypatch):
    """scripts/chaos.py counts duplicate executions from these records."""
    task_id = submit("record_execution", {"key": "k1"})
    w = make_worker()
    w.step()
    # Run it again, as a redelivery after a crash would.
    with SessionLocal() as s:
        s.execute(update(Task).where(Task.id == task_id).values(status=TaskStatus.QUEUED))
        s.commit()
    w.broker.enqueue("default", task_id)
    w.step()

    entries = w.broker.r.lrange("dtq:chaos:k1", 0, -1)
    assert len(entries) == 2
    when, host = entries[0].split()
    assert float(when) > 0 and host
