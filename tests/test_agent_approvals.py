from datetime import timedelta

import pytest
from sqlalchemy import update

from dtq.agents.models import AgentRun, RunStatus, StepKind, StepStatus
from dtq.db import SessionLocal, utcnow

from .agent_support import (  # noqa: F401  (agent_env is an autouse fixture)
    SPY, Crash, agent_env, assert_paused_on_unknown_outcome, drain, effects, faults, get_run, get_steps,
    reclaim_immediately, start_side_effect_run)


def pause_on_unknown_charge(client, make_worker, monkeypatch, point: str = "before"):
    """Kill a worker in the middle of charge, which has no check, so the worker that
    takes over pauses the run on it. Returns the run and that worker."""
    run_id = start_side_effect_run(client)
    faults[f"charge:{point}"] = Crash()
    with pytest.raises(Crash):
        drain(make_worker("killed"))
    reclaim_immediately(monkeypatch)
    survivor = make_worker("survivor")
    drain(survivor)
    assert_paused_on_unknown_outcome(run_id, 1, "charge")
    return run_id, survivor


def waiting(run_id: str) -> list[int]:
    return [s.step_no for s in get_steps(run_id) if s.status == StepStatus.WAITING]


def decide(client, run_id: str, action: str, **body):
    return client.post(f"/runs/{run_id}/{action}", json=body)


def tool_results(request) -> dict:
    return {m["tool_call_id"]: (m["content"], m["is_error"]) for m in request.messages if m["role"] == "tool"}


def test_approving_an_unknown_outcome_retries_the_call_with_its_key(client, make_worker, monkeypatch):
    run_id, worker = pause_on_unknown_charge(client, make_worker, monkeypatch, point="before")
    [approval] = waiting(run_id)

    resp = decide(client, run_id, "approve", step_no=approval)

    assert resp.status_code == 200
    assert (resp.json()["status"], resp.json()["paused_at"]) == ("running", None)
    retry = get_steps(run_id)[-1]
    assert (retry.kind, retry.status, retry.turn, retry.call_id) == (StepKind.TOOL, StepStatus.PENDING, 0, "call_0_0")
    assert retry.idempotency_key == f"{run_id}:1"  # the original call's key
    assert get_steps(run_id)[approval].output == {"decision": "approve", "note": None}

    drain(worker)
    assert get_run(run_id).status == RunStatus.SUCCEEDED
    assert effects.count(("charge", f"{run_id}:1")) == 1
    assert tool_results(SPY.requests[-1])["call_0_0"] == ({"charged": 5}, False)


def test_approving_with_the_calls_result_records_it_instead_of_retrying(client, make_worker, monkeypatch):
    run_id, worker = pause_on_unknown_charge(client, make_worker, monkeypatch, point="after")
    [approval] = waiting(run_id)
    receipt = {"charged": 5, "receipt": "r-1"}

    assert decide(client, run_id, "approve", step_no=approval, tool_output=receipt, note="saw it").status_code == 200
    assert StepKind.TOOL not in [s.kind for s in get_steps(run_id)[approval:]]  # nothing retried

    drain(worker)
    assert get_run(run_id).status == RunStatus.SUCCEEDED
    assert effects.count(("charge", f"{run_id}:1")) == 1
    assert tool_results(SPY.requests[-1])["call_0_0"] == (receipt, False)
    assert get_steps(run_id)[approval].output == {"decision": "approve", "note": "saw it", "tool_output": receipt}


def test_rejecting_fails_the_run(client, make_worker, monkeypatch):
    run_id, worker = pause_on_unknown_charge(client, make_worker, monkeypatch)
    [approval] = waiting(run_id)

    resp = decide(client, run_id, "reject", step_no=approval, note="charged twice before")

    assert resp.status_code == 200
    run = get_run(run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, f"step {approval} was rejected: charged twice before")
    assert run.finished_at is not None and run.paused_at is None
    assert get_steps(run_id)[approval].output == {"decision": "reject", "note": "charged twice before"}
    drain(worker)
    assert len(SPY.requests) == 1


def test_run_stays_paused_until_every_waiting_step_is_decided(client, make_worker, monkeypatch):
    run_id = start_side_effect_run(client)
    faults["charge:before"] = Crash()
    faults["notify:before"] = TimeoutError("no response")  # check finds nothing: unknown too
    with pytest.raises(Crash):
        drain(make_worker("killed"))
    reclaim_immediately(monkeypatch)
    worker = make_worker("survivor")
    drain(worker)
    first, second = waiting(run_id)
    assert get_run(run_id).calls_remaining == 2

    decide(client, run_id, "approve", step_no=first, tool_output={"charged": 5})
    assert (get_run(run_id).status, get_run(run_id).calls_remaining) == (RunStatus.AWAITING_APPROVAL, 1)

    decide(client, run_id, "approve", step_no=second)  # retry notify
    assert get_run(run_id).status == RunStatus.RUNNING
    drain(worker)
    assert get_run(run_id).status == RunStatus.SUCCEEDED
    assert effects == [("notify", f"{run_id}:2")]
    assert tool_results(SPY.requests[-1]) == {"call_0_0": ({"charged": 5}, False), "call_0_1": ({"sent": True}, False),
                                              "call_0_2": ("value of a", False)}


def test_time_awaiting_approval_is_added_to_paused_seconds(client, make_worker, monkeypatch):
    run_id, _ = pause_on_unknown_charge(client, make_worker, monkeypatch)
    with SessionLocal() as s:
        s.execute(update(AgentRun).where(AgentRun.id == run_id).values(paused_at=utcnow() - timedelta(seconds=30)))
        s.commit()

    decide(client, run_id, "approve", step_no=waiting(run_id)[0])

    run = get_run(run_id)
    assert run.paused_at is None
    assert 30 <= run.paused_seconds < 40


def test_decision_errors(client, make_worker, monkeypatch):
    run_id, worker = pause_on_unknown_charge(client, make_worker, monkeypatch)
    [approval] = waiting(run_id)

    assert decide(client, "missing", "approve", step_no=1).status_code == 404
    assert decide(client, "missing", "reject", step_no=1).status_code == 404
    for step_no in (1, 99):  # a tool step, a step that doesn't exist
        resp = decide(client, run_id, "approve", step_no=step_no)
        assert (resp.status_code, resp.json()["detail"]) == (409, f"step {step_no} isn't waiting for a decision")
    assert decide(client, run_id, "approve").status_code == 422  # no step_no

    decide(client, run_id, "approve", step_no=approval)
    drain(worker)
    resp = decide(client, run_id, "approve", step_no=approval)
    assert (resp.status_code, resp.json()["detail"]) == (409, "run is succeeded, not awaiting approval")


@pytest.mark.parametrize("first,second", [
    ("cancel", "approve"), ("cancel", "reject"), ("approve", "approve"),
    ("approve", "reject"), ("reject", "approve"), ("reject", "reject"),
])
def test_the_first_decision_wins(client, make_worker, monkeypatch, first, second):
    """Each decision and cancel is one transaction that locks the run row first, so
    when two race, whichever commits first wins and the other gets a 409."""
    run_id, worker = pause_on_unknown_charge(client, make_worker, monkeypatch)
    [approval] = waiting(run_id)

    def act(action):
        if action == "cancel":
            return client.post(f"/runs/{run_id}/cancel")
        return decide(client, run_id, action, step_no=approval)

    assert act(first).status_code == 200
    assert act(second).status_code == 409
    drain(worker)

    expected = {"cancel": RunStatus.CANCELLED, "approve": RunStatus.SUCCEEDED, "reject": RunStatus.FAILED}[first]
    assert get_run(run_id).status == expected
    retries = [s for s in get_steps(run_id) if s.kind == StepKind.TOOL and s.call_id == "call_0_0"][1:]
    assert len(retries) == (1 if first == "approve" else 0)


def test_cancel_after_approving_stops_the_retry(client, make_worker, monkeypatch):
    run_id, worker = pause_on_unknown_charge(client, make_worker, monkeypatch, point="before")
    decide(client, run_id, "approve", step_no=waiting(run_id)[0])

    assert client.post(f"/runs/{run_id}/cancel").status_code == 200
    drain(worker)

    assert get_run(run_id).status == RunStatus.CANCELLED
    assert ("charge", f"{run_id}:1") not in effects  # the retry's message found the run cancelled
    assert get_steps(run_id)[-1].status == StepStatus.PENDING


def test_a_step_is_decided_once_even_while_others_keep_the_run_paused(client, make_worker, monkeypatch):
    """With another approval still waiting, the run stays awaiting approval, so only
    the step's own waiting status stops a second click from retrying the call twice."""
    run_id = start_side_effect_run(client)
    faults["charge:before"] = Crash()
    faults["notify:before"] = TimeoutError("no response")
    with pytest.raises(Crash):
        drain(make_worker("killed"))
    reclaim_immediately(monkeypatch)
    drain(make_worker("survivor"))
    first, _ = waiting(run_id)

    assert decide(client, run_id, "approve", step_no=first).status_code == 200
    resp = decide(client, run_id, "approve", step_no=first)

    assert (resp.status_code, resp.json()["detail"]) == (409, f"step {first} isn't waiting for a decision")
    retries = [s for s in get_steps(run_id) if s.kind == StepKind.TOOL and s.call_id == "call_0_0"][1:]
    assert len(retries) == 1
