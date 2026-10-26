from datetime import timedelta

import pytest
from sqlalchemy import update

from dtq import config
from dtq.db import SessionLocal, Task, TaskStatus, utcnow

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
