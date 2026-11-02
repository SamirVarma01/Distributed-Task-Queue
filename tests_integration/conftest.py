"""Integration tests: the queue against real MySQL and Redis, started in Docker with
testcontainers. Run them with `pytest tests_integration` (Docker must be running).

MySQL is built from sql/Dockerfile, the image docker compose uses, so sql/schema.sql
creates the tables here exactly as it does there.

dtq reads its config and creates its engine and API broker at import time, so the
containers must be up, and their addresses in the environment, before any dtq module is
imported: pytest_configure starts them, and the fixtures import dtq lazily. It is also
why these tests live outside tests/, whose conftest swaps in SQLite and fakeredis at
import time for the same reason; the two can't share a process."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_containers = []


def pytest_configure(config):
    from testcontainers.community.mysql import MySqlContainer
    from testcontainers.community.redis import RedisContainer
    from testcontainers.core.image import DockerImage

    try:
        image = DockerImage(path=ROOT / "sql", tag="dtq-mysql-integration").build()
        mysql = MySqlContainer(str(image), dialect="pymysql", username="dtq", password="dtq", dbname="dtq").start()
        _containers.append(mysql)
        redis = RedisContainer("redis:7.4-alpine").start()
        _containers.append(redis)
    except Exception as exc:  # most often: Docker isn't running
        pytest.exit(f"could not start MySQL and Redis in Docker: {exc}", returncode=2)

    os.environ["MYSQL_URL"] = mysql.get_connection_url()
    os.environ["REDIS_URL"] = f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"
    os.environ["QUEUES"] = "high,default,low"


def pytest_unconfigure(config):
    for container in reversed(_containers):
        container.stop()


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    from sqlalchemy import delete

    from dtq import api, config
    from dtq.agents.demo import SinkEntry
    from dtq.agents.models import AgentRun, AgentStep
    from dtq.db import SessionLocal, Task

    api.broker.r.flushall()
    with SessionLocal() as s:
        for model in (Task, AgentStep, AgentRun, SinkEntry):
            s.execute(delete(model))
        s.commit()
    # In-process workers only reclaim or reconcile when a test asks for it.
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", float("inf"))
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", float("inf"))


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    from dtq import api

    return TestClient(api.app)


@pytest.fixture
def submit(client):
    def submit(name: str, payload: dict | None = None, **kw) -> str:
        resp = client.post("/tasks", json={"name": name, "payload": payload or {}, **kw})
        assert resp.status_code == 202, resp.text
        return resp.json()["id"]

    return submit


@pytest.fixture
def make_worker():
    """Create in-process workers with distinct consumer names (in one process they
    would share hostname-pid). They are driven with step() instead of run()."""
    from dtq import config
    from dtq.worker import Worker

    def make(name: str = "w1"):
        w = Worker()
        w.id = name
        for q in config.QUEUES:
            w.broker.ensure_group(q)
        return w

    return make


@pytest.fixture
def worker_process(tmp_path):
    """Start `python -m dtq.worker` as its own process, as docker compose would, with
    extra environment variables. Every process still running is SIGKILLed at the end."""
    procs = []

    def start(name: str, **env) -> subprocess.Popen:
        with open(tmp_path / f"{name}.log", "w") as log:  # the child keeps its own copy of the handle
            proc = subprocess.Popen([sys.executable, "-m", "dtq.worker"], cwd=ROOT, stdout=log,
                                    stderr=subprocess.STDOUT, env={**os.environ, **{k: str(v) for k, v in env.items()}})
        procs.append(proc)
        return proc

    yield start
    for proc in procs:
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            proc.wait()


def wait_until(condition, timeout: float = 30, interval: float = 0.05):
    """Poll condition() until it returns something truthy, and return that."""
    deadline = time.monotonic() + timeout
    while not (result := condition()):
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {condition.__name__}")
        time.sleep(interval)
    return result
