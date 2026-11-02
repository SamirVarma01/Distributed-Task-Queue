from datetime import datetime, timedelta

import pytest
from sqlalchemy import update

from dtq.agents import budgets
from dtq.agents.models import AgentRun, RunStatus, StepStatus
from dtq.db import SessionLocal, utcnow

from .agent_support import SPY, agent_env, drain, get_run, get_steps, start_run  # noqa: F401

NOW = datetime(2026, 11, 1, 12, 0, 0)


def run(**fields) -> AgentRun:
    defaults = dict(model={"max_output_tokens": 1024}, max_steps=10, llm_calls=0, input_tokens=0, output_tokens=0,
                    cost_micros=0, paused_seconds=0, paused_at=None, started_at=NOW - timedelta(seconds=100),
                    max_tokens=None, max_cost_micros=None, max_wall_seconds=None)
    return AgentRun(**{**defaults, **fields})


@pytest.mark.parametrize("fields,expected", [
    ({}, None),
    ({"llm_calls": 10}, "max_steps: 10 of 10 LLM calls made"),
    ({"max_tokens": 500, "input_tokens": 400, "output_tokens": 99}, None),
    ({"max_tokens": 500, "input_tokens": 400, "output_tokens": 100}, "max_tokens: 500 of 500 tokens used"),
    ({"max_cost_micros": 900, "cost_micros": 899}, None),
    ({"max_cost_micros": 900, "cost_micros": 900}, "max_cost_micros: 900 of 900 micro-dollars spent"),
    ({"max_wall_seconds": 100}, "max_wall_seconds: 100 of 100 seconds used"),
    ({"max_wall_seconds": 100, "paused_seconds": 30}, None),  # time awaiting approval doesn't count
    ({"max_wall_seconds": 100, "paused_at": NOW - timedelta(seconds=5)}, None),  # nor does the current pause
])
def test_exceeded(fields, expected):
    assert budgets.exceeded(run(**fields), NOW) == expected


def test_wall_seconds_excludes_past_and_current_pauses():
    assert budgets.wall_seconds(run(), NOW) == 100
    assert budgets.wall_seconds(run(paused_seconds=30, paused_at=NOW - timedelta(seconds=20)), NOW) == 50
    assert budgets.wall_seconds(run(started_at=None), NOW) == 0


def test_output_limit_is_capped_at_the_tokens_left():
    assert budgets.max_output_tokens(run()) == 1024
    assert budgets.max_output_tokens(run(max_tokens=300, input_tokens=100, output_tokens=10)) == 190


# three-turns uses 110, 220 and 330 tokens, and costs 300, 600 and 900 micro-dollars.

def test_max_steps_stops_the_run_before_the_next_llm_call(client, make_worker):
    run_id = start_run(client, max_steps=2)
    drain(make_worker())

    r = get_run(run_id)
    assert (r.status, r.error) == (RunStatus.BUDGET_EXCEEDED, "budget exceeded: max_steps: 2 of 2 LLM calls made")
    assert r.finished_at is not None
    last = get_steps(run_id)[-1]
    assert (last.step_no, last.status) == (5, StepStatus.FAILED)
    assert last.output == {"error": "not run: budget exceeded (max_steps: 2 of 2 LLM calls made)"}
    assert len(SPY.requests) == 2


def test_max_tokens_caps_each_call_and_stops_the_run(client, make_worker):
    run_id = start_run(client, max_tokens=300)
    drain(make_worker())

    assert [r.max_output_tokens for r in SPY.requests] == [300, 190]
    r = get_run(run_id)
    assert (r.status, r.error) == (RunStatus.BUDGET_EXCEEDED, "budget exceeded: max_tokens: 330 of 300 tokens used")


@pytest.mark.parametrize("budget,status,calls", [(900, RunStatus.BUDGET_EXCEEDED, 2), (901, RunStatus.SUCCEEDED, 3)])
def test_max_cost(client, make_worker, budget, status, calls):
    run_id = start_run(client, max_cost_micros=budget)
    drain(make_worker())

    assert get_run(run_id).status == status
    assert len(SPY.requests) == calls


@pytest.mark.parametrize("paused_seconds,status", [(0, RunStatus.BUDGET_EXCEEDED), (30, RunStatus.SUCCEEDED)])
def test_max_wall_seconds_does_not_count_time_awaiting_approval(client, make_worker, paused_seconds, status):
    run_id = start_run(client, max_wall_seconds=60)
    w = make_worker()
    w.step()  # step 0 starts the run
    with SessionLocal() as s:
        s.execute(update(AgentRun).where(AgentRun.id == run_id).values(
            started_at=utcnow() - timedelta(seconds=61), paused_seconds=paused_seconds))
        s.commit()
    drain(w)

    assert get_run(run_id).status == status


def test_cost_budget_needs_a_priced_model(client):
    resp = client.post("/runs", json={"goal": "g", "model": {"provider": "scripted", "name": "unpriced"},
                                      "max_cost_micros": 1000})

    assert resp.status_code == 400
    assert resp.json()["detail"] == "model 'unpriced' has no price, so a cost budget can't be enforced"
