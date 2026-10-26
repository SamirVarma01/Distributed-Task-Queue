import redis

from dtq import api
from dtq.db import TaskStatus

from .conftest import get_task


def test_submit_creates_row_and_enqueues(client, submit):
    task_id = submit("add", {"a": 2, "b": 3}, queue="high")

    task = get_task(task_id)
    assert task.status == TaskStatus.QUEUED
    assert task.payload == {"a": 2, "b": 3}
    assert api.broker.r.xlen("dtq:stream:high") == 1


def test_submit_rejects_unknown_task_and_queue(client):
    assert client.post("/tasks", json={"name": "nope"}).status_code == 400
    assert client.post("/tasks", json={"name": "add", "queue": "nope"}).status_code == 400


def test_delayed_submit_waits_in_delayed_set(submit):
    submit("add", {"a": 1, "b": 1}, delay_seconds=60)

    assert api.broker.r.zcard("dtq:delayed") == 1
    assert api.broker.r.xlen("dtq:stream:default") == 0


def test_submit_marks_task_dead_when_redis_is_down(client, monkeypatch):
    def fail(*_):
        raise redis.ConnectionError("down")

    monkeypatch.setattr(api.broker, "enqueue", fail)
    resp = client.post("/tasks", json={"name": "add", "payload": {"a": 1, "b": 1}})

    assert resp.status_code == 503
    [task] = client.get("/tasks").json()
    assert task["status"] == "dead"
    assert "enqueue failed" in task["error"]


def test_get_and_list_tasks(client, submit):
    task_id = submit("add", {"a": 1, "b": 1}, queue="low")

    assert client.get(f"/tasks/{task_id}").json()["id"] == task_id
    assert client.get("/tasks/missing").status_code == 404
    assert [t["id"] for t in client.get("/tasks?queue=low").json()] == [task_id]
    assert client.get("/tasks?queue=high").json() == []


def test_cancel(client, submit):
    queued = submit("add", {"a": 1, "b": 1})
    assert client.post(f"/tasks/{queued}/cancel").json()["status"] == "cancelled"
    assert client.post(f"/tasks/{queued}/cancel").status_code == 409
    assert client.post("/tasks/missing/cancel").status_code == 404


def test_stats(client, submit):
    submit("add", {"a": 1, "b": 1})
    submit("add", {"a": 1, "b": 1}, queue="high")

    stats = client.get("/stats").json()
    assert stats["tasks"]["queued"] == 2
    assert stats["broker"]["queues"]["default"] == {"waiting": 1, "in_flight": 0}
    assert stats["broker"]["queues"]["high"] == {"waiting": 1, "in_flight": 0}


def test_submit_rejects_payload_that_does_not_fit_the_task(client):
    missing = client.post("/tasks", json={"name": "add", "payload": {"a": 1}})
    unexpected = client.post("/tasks", json={"name": "add", "payload": {"a": 1, "b": 2, "c": 3}})

    assert missing.status_code == 400
    assert unexpected.status_code == 400
    assert client.get("/tasks").json() == []


def test_submit_records_enqueued_at_and_run_at(client, submit):
    now_id = submit("add", {"a": 1, "b": 1})
    later_id = submit("add", {"a": 1, "b": 1}, delay_seconds=60)

    now, later = get_task(now_id), get_task(later_id)
    assert now.enqueued_at is not None and now.run_at is None
    assert later.enqueued_at is not None and later.run_at > later.created_at
