"""Timeouts in real worker processes (`python -m dtq.worker`), with
DEFAULT_TASK_TIMEOUT_SECONDS set, so every task runs in the worker's child process."""

import pytest

from dtq.agents.models import AgentRun, RunStatus
from dtq.db import SessionLocal, Task, TaskStatus

from .conftest import wait_until

pytestmark = pytest.mark.integration


def test_worker_process_kills_a_task_that_overruns_the_default_timeout(submit, worker_process):
    task_id = submit("sleep", {"seconds": 60}, max_retries=1)
    worker_process("worker", DEFAULT_TASK_TIMEOUT_SECONDS=1, RETRY_BACKOFF_BASE_SECONDS=0.1)

    def dead():
        with SessionLocal() as s:
            task = s.get(Task, task_id)
        return task if task.status == TaskStatus.DEAD else None

    task = wait_until(dead, timeout=60)
    assert (task.attempts, task.error) == (2, "TimeoutError: exceeded 1s")


def test_agent_steps_run_in_the_child_with_their_own_connections(client, worker_process, tmp_path):
    """agent_step reads and writes MySQL and sends its next steps to Redis; in the child
    it does so over connections the child opened itself."""
    resp = client.post("/runs", json={"goal": "x", "model": {"provider": "scripted", "name": "bench-agent"},
                                      "tools": ["echo"], "max_steps": 10})
    run_id = resp.json()["id"]
    worker_process("worker", DEFAULT_TASK_TIMEOUT_SECONDS=30)

    def finished():
        with SessionLocal() as s:
            run = s.get(AgentRun, run_id)
        return run if run.finished_at else None

    run = wait_until(finished, timeout=60)
    assert (run.status, run.llm_calls) == (RunStatus.SUCCEEDED, 3)
    assert "started task process" in (tmp_path / "worker.log").read_text()
