import pytest
from sqlalchemy import func, select, update

from dtq.agents.models import AgentRun, RunStatus
from dtq.agents.states import ALLOWED_FROM, TERMINAL
from dtq.db import SessionLocal

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


def test_submit_creates_a_queued_run_and_its_first_llm_step(client, submit_run):
    run = submit_run(tools=["calculator"], queue="high", max_steps=5, max_tokens=1000)

    assert run["status"] == "queued"
    assert run["model"] == {**MODEL, "system": "", "max_output_tokens": 1024}
    assert run["tools"] == ["calculator"]
    assert run["queue"] == "high"
    assert (run["max_steps"], run["max_tokens"], run["max_cost_micros"], run["max_wall_seconds"]) == (5, 1000, None, None)
    assert (run["llm_calls"], run["input_tokens"], run["output_tokens"], run["cost_micros"]) == (0, 0, 0, 0)
    assert (run["turn"], run["calls_remaining"], run["next_step_no"]) == (0, 0, 1)
    assert client.get(f"/runs/{run['id']}").json() == run

    [step] = client.get(f"/runs/{run['id']}/steps").json()
    assert (step["step_no"], step["turn"], step["kind"], step["status"]) == (0, 0, "llm", "pending")
    assert step["input"] == {"model": "two-turns", "max_output_tokens": 1024}
    assert step["attempt"] is None


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
