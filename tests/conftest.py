"""Test setup: fakeredis stands in for Redis and a temporary SQLite file for MySQL, so
the suite runs without Docker. Both must be in place before any dtq module is imported,
because dtq reads its config and creates its engine and API broker at import time."""

import os
import tempfile

import fakeredis
import pytest
import redis

os.environ["MYSQL_URL"] = f"sqlite:///{tempfile.mkdtemp()}/dtq-test.db"
os.environ["QUEUES"] = "high,default,low"
# A task with a timeout runs in a child process, which can't see fakeredis. Only the
# timeout tests' own tasks have one.
os.environ.pop("DEFAULT_TASK_TIMEOUT_SECONDS", None)

FAKE_REDIS = fakeredis.FakeServer()
# Every Broker connects through Redis.from_url; point it at the shared fake server.
redis.Redis.from_url = lambda url, **kw: fakeredis.FakeRedis(server=FAKE_REDIS, **kw)

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import delete  # noqa: E402

from dtq import api, config  # noqa: E402
from dtq.agents.demo import SinkEntry  # noqa: E402
from dtq.agents.models import AgentRun, AgentStep  # noqa: E402
from dtq.db import Base, SessionLocal, Task, engine  # noqa: E402
from dtq.worker import Worker  # noqa: E402

Base.metadata.create_all(engine)


@pytest.fixture(autouse=True)
def clean_state():
    api.broker.r.flushall()
    with SessionLocal() as s:
        for model in (Task, AgentStep, AgentRun, SinkEntry):
            s.execute(delete(model))
        s.commit()


@pytest.fixture
def client():
    return TestClient(api.app)


@pytest.fixture
def make_worker():
    """Create workers with distinct consumer names (in one process they would share
    hostname-pid). Most tests drive them with step() instead of run(). Each is closed
    at the end, stopping its heartbeat thread and child processes."""
    workers = []

    def make(name: str = "w1", concurrency: int = 1) -> Worker:
        w = Worker(concurrency)
        w.id = name
        for q in config.QUEUES:
            w.broker.ensure_group(q)
        workers.append(w)
        return w

    yield make
    for w in workers:
        w.close()


@pytest.fixture
def submit(client):
    def submit(name: str, payload: dict | None = None, **kw) -> str:
        resp = client.post("/tasks", json={"name": name, "payload": payload or {}, **kw})
        assert resp.status_code == 202, resp.text
        return resp.json()["id"]

    return submit


def get_task(task_id: str) -> Task:
    with SessionLocal() as s:
        return s.get(Task, task_id)


def pending_count(broker, queue: str) -> int:
    return broker.r.xpending(f"dtq:stream:{queue}", config.CONSUMER_GROUP)["pending"]
