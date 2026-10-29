import pytest
import redis
from sqlalchemy import func, select, update

from dtq.agents import executor
from dtq.agents.models import AgentRun, RunStatus
from dtq.agents.states import ALLOWED_FROM, TERMINAL
from dtq.db import SessionLocal, TaskStatus

from .conftest import get_task

MODEL = {"provider": "scripted", "name": "two-turns"}


@pytest.fixture
def submit_run(client):
    def submit(**kw) -> dict:
        resp = client.post("/runs", json={"goal": "summarize the queue", "model": MODEL, **kw})
        assert resp.status_code == 202, resp.text
        return resp.json()

    return submit


def set_status(run_id: str, status: RunStatus):
    with SessionLocal() as s:
        s.execute(update(AgentRun).where(AgentRun.id == run_id).values(status=status))
        s.commit()


def test_submit_creates_a_queued_run_and_enqueues_its_first_llm_step(client, submit_run):
    run = submit_run(tools=["echo"], queue="high", max_steps=5, max_tokens=1000)

    assert run["status"] == "queued"
    assert run["model"] == {**MODEL, "system": "", "max_output_tokens": 1024}
    assert run["tools"] == ["echo"]
    assert run["queue"] == "high"
    assert (run["max_steps"], run["max_tokens"], run["max_cost_micros"], run["max_wall_seconds"]) == (5, 1000, None, None)
    assert (run["llm_calls"], run["input_tokens"], run["output_tokens"], run["cost_micros"]) == (0, 0, 0, 0)
    assert (run["turn"], run["calls_remaining"], run["next_step_no"]) == (0, 0, 1)
    assert client.get(f"/runs/{run['id']}").json() == run

    [step] = client.get(f"/runs/{run['id']}/steps").json()
    assert (step["step_no"], step["turn"], step["kind"], step["status"]) == (0, 0, "llm", "pending")
    assert step["input"] == {"model": "two-turns", "max_output_tokens": 1024}
    assert step["attempt"] is None

    task = get_task(executor.step_task_id(run["id"], 0))
    assert (task.name, task.queue, task.payload, task.max_retries) == (
        "agent_step", "high", {"run_id": run["id"], "step_no": 0}, run["step_max_retries"])
    assert task.status == TaskStatus.QUEUED and task.enqueued_at is not None
    assert executor.broker.r.xlen("dtq:stream:high") == 1


def test_submit_fails_the_run_when_redis_is_down(client, monkeypatch):
    def down(*_):
        raise redis.ConnectionError("down")

    monkeypatch.setattr(executor.broker, "send_many", down)
    resp = client.post("/runs", json={"goal": "g", "model": MODEL})

    assert resp.status_code == 503
    with SessionLocal() as s:
        run = s.scalars(select(AgentRun)).one()
    assert run.status == RunStatus.FAILED
    assert run.error.startswith("enqueue failed")
    assert get_task(executor.step_task_id(run.id, 0)).status == TaskStatus.DEAD


def test_step_tasks_cannot_be_cancelled_directly(client, submit_run):
    run_id = submit_run()["id"]

    resp = client.post(f"/tasks/{executor.step_task_id(run_id, 0)}/cancel")

    assert resp.status_code == 409
    assert resp.json()["detail"] == "agent_step tasks cannot be cancelled"


@pytest.mark.parametrize("body,status", [
    ({"queue": "nope"}, 400),
    ({"goal": ""}, 422),
    ({"model": {"provider": "scripted"}}, 422),
    ({"max_steps": 0}, 422),
    ({"max_tokens": 0}, 422),
    ({"max_cost_micros": -1}, 422),
    ({"max_wall_seconds": 0}, 422),
    ({"step_max_retries": 21}, 422),
])
def test_submit_rejects_invalid_runs(client, body, status):
    resp = client.post("/runs", json={"goal": "g", "model": MODEL, **body})

    assert resp.status_code == status
    with SessionLocal() as s:
        assert s.scalar(select(func.count()).select_from(AgentRun)) == 0


def test_unknown_run_is_404(client):
    assert client.get("/runs/missing").status_code == 404
    assert client.get("/runs/missing/steps").status_code == 404
    assert client.post("/runs/missing/cancel").status_code == 404


@pytest.mark.parametrize("status", sorted(ALLOWED_FROM[RunStatus.CANCELLED]), ids=lambda s: s.value)
def test_cancel_an_unfinished_run(client, submit_run, status):
    run_id = submit_run()["id"]
    set_status(run_id, status)

    resp = client.post(f"/runs/{run_id}/cancel")

    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert resp.json()["finished_at"] is not None
    assert client.post(f"/runs/{run_id}/cancel").status_code == 409


@pytest.mark.parametrize("status", sorted(TERMINAL), ids=lambda s: s.value)
def test_cancel_a_finished_run_is_a_conflict(client, submit_run, status):
    run_id = submit_run()["id"]
    set_status(run_id, status)

    resp = client.post(f"/runs/{run_id}/cancel")

    assert resp.status_code == 409
    assert status.value in resp.json()["detail"]
    assert client.get(f"/runs/{run_id}").json()["status"] == status.value
