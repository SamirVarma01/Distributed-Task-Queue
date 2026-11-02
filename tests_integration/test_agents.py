"""Agent runs on real MySQL. SQLite, which the unit tests use, takes one lock for the
whole database on every write, so concurrent steps never really race there. InnoDB locks
rows: here the parallel tool calls of a turn finish on different workers at the same
time, and the conditional updates on the run row must still start each next turn once."""

import threading
from collections import Counter

import pytest
from sqlalchemy import select

from dtq.agents.models import AgentRun, AgentStep, RunStatus, StepKind, StepStatus
from dtq.db import SessionLocal

from .conftest import wait_until

pytestmark = pytest.mark.integration


def test_parallel_tool_calls_on_four_workers_start_each_turn_once(client, make_worker):
    # bench-agent: a turn of two parallel echo calls, a turn of one, then the answer.
    run_ids = []
    for i in range(30):
        resp = client.post("/runs", json={"goal": f"run {i}", "model": {"provider": "scripted", "name": "bench-agent"},
                                          "tools": ["echo"], "max_steps": 10})
        assert resp.status_code == 202, resp.text
        run_ids.append(resp.json()["id"])

    stop, errors = threading.Event(), []

    def work(w):
        try:
            while not stop.is_set():
                w.step(block_ms=20)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(make_worker(f"w{i}"),)) for i in range(4)]
    for t in threads:
        t.start()

    def finished():
        with SessionLocal() as s:
            runs = s.scalars(select(AgentRun).where(AgentRun.id.in_(run_ids))).all()
        return errors or all(r.finished_at for r in runs)

    try:
        wait_until(finished, timeout=60)
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert not errors

    with SessionLocal() as s:
        runs = s.scalars(select(AgentRun).where(AgentRun.id.in_(run_ids))).all()
        steps = s.scalars(select(AgentStep).where(AgentStep.run_id.in_(run_ids))).all()
    assert Counter(r.status for r in runs) == {RunStatus.SUCCEEDED: 30}
    assert all(r.llm_calls == 3 and r.calls_remaining == 0 for r in runs)
    assert Counter(s.status for s in steps) == {StepStatus.SUCCEEDED: 6 * 30}
    llm_steps_per_turn = Counter((s.run_id, s.turn) for s in steps if s.kind == StepKind.LLM)
    assert set(llm_steps_per_turn.values()) == {1} and len(llm_steps_per_turn) == 3 * 30
