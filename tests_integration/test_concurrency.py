"""A worker process running several tasks at once (WORKER_CONCURRENCY), killed with
SIGKILL: every message it held must be reclaimed and its task finished."""

import socket

import pytest

from dtq import api, config
from dtq.broker import stream_key
from dtq.db import SessionLocal, Task, TaskStatus

from .conftest import wait_until

pytestmark = pytest.mark.integration
HOST = socket.gethostname()  # worker processes are named <hostname>-<pid>


def executions(key: str) -> int:
    return api.broker.r.llen(f"dtq:chaos:{key}")


def test_killing_a_worker_with_several_tasks_in_flight_reclaims_them_all(submit, worker_process, tmp_path):
    env = {"WORKER_CONCURRENCY": 3, "VISIBILITY_TIMEOUT_MS": 2000, "HEARTBEAT_SECONDS": 0.5,
           "RECLAIM_INTERVAL_SECONDS": 0.5}
    ids = [submit("record_execution", {"key": f"slot-{i}", "ms": 3000}) for i in range(3)]
    first = worker_process("first", **env)

    wait_until(lambda: all(executions(f"slot-{i}") == 1 for i in range(3)))  # all three running at once
    first.kill()
    first.wait()
    pending = api.broker.r.xpending(stream_key("default"), config.CONSUMER_GROUP)
    assert pending["pending"] == 3 and list(pending["consumers"]) == [{"name": f"{HOST}-{first.pid}", "pending": 3}]

    second = worker_process("second", **env)

    def all_succeeded():
        with SessionLocal() as s:
            tasks = [s.get(Task, i) for i in ids]
        return tasks if all(t.status == TaskStatus.SUCCEEDED for t in tasks) else None

    tasks = wait_until(all_succeeded, timeout=60)
    assert [t.attempts for t in tasks] == [2, 2, 2]
    assert {t.worker_id for t in tasks} == {f"{HOST}-{second.pid}"}
    assert [executions(f"slot-{i}") for i in range(3)] == [2, 2, 2]
    assert (tmp_path / "second.log").read_text().count("from a dead worker") == 3

