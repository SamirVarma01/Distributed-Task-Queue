import pytest
from dtq import config
from dtq.agents import demo, executor, scripted
from dtq.agents.history import prompt_sha256
from dtq.agents.models import AgentStep, RunStatus, StepKind, StepStatus
from dtq.agents.tools import ToolError
from dtq.db import SessionLocal, TaskStatus

from .agent_support import (  # noqa: F401  (agent_env is an autouse fixture)
    EXPECTED_STEPS, SPY, Crash, agent_env, assert_paused_on_unknown_outcome, drain, effects, faults, get_run,
    get_steps, looked_up, lookup_hooks, reclaim_immediately, run_as_attempt, sink_rows, start_run,
    start_side_effect_run, summary, turn_of)
from .conftest import get_task, pending_count



def test_multi_turn_run_with_parallel_tool_calls_completes(client, make_worker):
    run_id = start_run(client)
    w = make_worker()
    drain(w)

    run = get_run(run_id)
    assert (run.status, run.result) == (RunStatus.SUCCEEDED, "a, b and c found")
    assert (run.llm_calls, run.input_tokens, run.output_tokens) == (3, 600, 60)
    assert run.cost_micros == 600 * 2 + 60 * 10
    assert (run.turn, run.calls_remaining, run.next_step_no) == (5, 0, 6)
    assert run.started_at and run.finished_at

    steps = get_steps(run_id)
    assert summary(steps) == EXPECTED_STEPS
    assert [s.call_id for s in steps] == [None, "call_0_0", "call_0_1", None, "call_1_0", None]
    assert [s.attempt for s in steps] == [1] * 6
    assert (steps[1].output, steps[1].idempotency_key) == ("value of a", f"{run_id}:1")
    assert (steps[3].input_tokens, steps[3].output_tokens, steps[3].cost_micros) == (200, 20, 600)
    assert looked_up == ["a", "b", "c"]
    assert [turn_of(r) for r in SPY.requests] == [0, 1, 2]
    last_prompt = SPY.requests[-1].messages
    assert [m["content"] for m in last_prompt if m["role"] == "tool"] == ["value of a", "value of b", "value of c"]
    assert steps[5].input["messages_sha256"] == prompt_sha256("", last_prompt)
    assert all(get_task(executor.step_task_id(run_id, n)).status == TaskStatus.SUCCEEDED for n in range(6))
    assert pending_count(w.broker, "default") == 0


@pytest.mark.parametrize("reverse", [False, True])
def test_parallel_calls_finishing_in_either_order_start_the_next_turn_once(client, make_worker, reverse):
    run_id = start_run(client)
    first, second = make_worker("first"), make_worker("second")
    first.step()  # step 0 schedules steps 1 and 2
    held = first.broker.read(first.id, config.QUEUES)  # step 1's message
    if reverse:
        second.step()  # step 2 finishes first
        first.handle(*held)  # step 1 is the turn's last call
    else:
        first.handle(*held)
        second.step()
    drain(first)

    assert summary(get_steps(run_id)) == EXPECTED_STEPS
    assert [m["content"] for m in SPY.requests[1].messages if m["role"] == "tool"] == ["value of a", "value of b"]


def test_duplicate_step_messages_change_nothing(client, make_worker):
    run_id = start_run(client)
    w = make_worker()
    drain(w)

    for n in range(6):
        w.broker.enqueue("default", executor.step_task_id(run_id, n))
    drain(w)

    assert summary(get_steps(run_id)) == EXPECTED_STEPS
    assert len(SPY.requests) == 3
    assert looked_up == ["a", "b", "c"]
    assert pending_count(w.broker, "default") == 0


def test_worker_killed_during_an_llm_call_resumes_at_that_step(client, make_worker, monkeypatch):
    run_id = start_run(client)

    def die_in_second_turn(request):
        if turn_of(request) == 1:
            SPY.before = None
            raise Crash

    SPY.before = die_in_second_turn
    with pytest.raises(Crash):
        drain(make_worker("killed"))
    assert summary(get_steps(run_id))[3] == (3, "llm", 3, "started")

    reclaim_immediately(monkeypatch)
    drain(make_worker("survivor"))

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    steps = get_steps(run_id)
    assert summary(steps) == EXPECTED_STEPS
    assert [s.attempt for s in steps] == [1, 1, 1, 2, 1, 1]
    assert [turn_of(r) for r in SPY.requests] == [0, 1, 1, 2]  # turn 0 was not repeated
    assert looked_up == ["a", "b", "c"]
    # The retried call sent the same prompt as the one that died.
    assert SPY.requests[1].messages == SPY.requests[2].messages
    assert get_run(run_id).llm_calls == 3  # the lost call's usage is never recorded


def test_worker_killed_during_a_tool_call_reruns_only_that_call(client, make_worker, monkeypatch):
    run_id = start_run(client)

    def die():
        raise Crash

    lookup_hooks["b"] = die
    with pytest.raises(Crash):
        drain(make_worker("killed"))

    reclaim_immediately(monkeypatch)
    drain(make_worker("survivor"))

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    steps = get_steps(run_id)
    assert summary(steps) == EXPECTED_STEPS
    assert [s.attempt for s in steps] == [1, 1, 2, 1, 1, 1]
    assert looked_up == ["a", "b", "b", "c"]
    assert len(SPY.requests) == 3


def test_worker_killed_after_recording_before_sending_the_next_steps(client, make_worker, monkeypatch):
    """The next steps' task rows were committed with the outcome, so the reconciler
    sends them. The reclaimed message for the recorded step does nothing."""
    run_id = start_run(client)
    real_send_many = executor.broker.send_many

    def die_once(tasks):
        monkeypatch.setattr(executor.broker, "send_many", real_send_many)
        raise Crash

    monkeypatch.setattr(executor.broker, "send_many", die_once)
    with pytest.raises(Crash):
        make_worker("killed").step()
    assert summary(get_steps(run_id)) == [(0, "llm", 0, "succeeded"), (1, "tool", 0, "pending"),
                                          (2, "tool", 0, "pending")]
    assert get_task(executor.step_task_id(run_id, 1)).enqueued_at is None

    reclaim_immediately(monkeypatch)
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(config, "RECONCILE_AFTER_SECONDS", 0)
    drain(make_worker("survivor"))

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    assert summary(get_steps(run_id)) == EXPECTED_STEPS
    assert get_task(executor.step_task_id(run_id, 0)).result == {
        "step_no": 0, "skipped": "the step has finished, or a later attempt owns it"}
    assert len(SPY.requests) == 3


def test_stale_attempt_cannot_record_over_the_attempt_that_took_over(client, make_worker):
    run_id = start_run(client)

    def later_attempt_takes_over(request):
        SPY.before = None
        assert run_as_attempt(run_id, 0, attempt=2) == {"step_no": 0, "status": "succeeded"}

    SPY.before = later_attempt_takes_over
    make_worker("stale").step()  # attempt 1 is mid-call while attempt 2 finishes the step

    steps = get_steps(run_id)
    assert summary(steps) == [(0, "llm", 0, "succeeded"), (1, "tool", 0, "pending"), (2, "tool", 0, "pending")]
    assert steps[0].attempt == 2
    run = get_run(run_id)
    assert (run.llm_calls, run.calls_remaining, run.next_step_no) == (1, 2, 3)  # scheduled once
    assert get_task(executor.step_task_id(run_id, 0)).result == {
        "step_no": 0, "skipped": "taken over by a later attempt"}


def test_the_highest_attempt_owns_a_step_while_both_are_running(client):
    """Drive the phases directly to interleave attempts that are all mid-step: the
    highest attempt owns the step from the moment it begins."""
    run_id = start_run(client)
    run1, step1, prompt1, _ = executor._begin(run_id, 0, 1)
    run3, step3, prompt3, _ = executor._begin(run_id, 0, 3)  # a reclaim takes the step over

    # An older attempt can't take it back, even though the step is still running.
    assert executor._begin(run_id, 0, 2) == "the step has finished, or a later attempt owns it"
    # The stale attempt finishes its call first, but can't record it.
    outcome = executor._call_llm(run1, step1, prompt1)
    assert executor._record(run1, step1, 1, outcome) is None
    assert get_steps(run_id)[0].status.value == "started"
    # The owner records it and schedules the turn's two calls, once.
    assert len(executor._record(run3, step3, 3, executor._call_llm(run3, step3, prompt3))) == 2
    assert [(s.step_no, s.attempt) for s in get_steps(run_id)] == [(0, 3), (1, None), (2, None)]


def test_a_step_from_an_earlier_turn_never_begins(client, make_worker):
    """Defence in depth: a step of a finished turn can't normally be pending, but if one
    were, A's turn check would still refuse it."""
    run_id = start_run(client)
    w = make_worker()
    for _ in range(4):  # steps 0-3: the run moves on to turn 3
        w.step()
    with SessionLocal() as s:
        s.add(AgentStep(run_id=run_id, step_no=99, turn=0, kind=StepKind.TOOL, status=StepStatus.PENDING,
                        call_id="call_0_0", input={"tool": "lookup", "args": {"key": "stray"}}))
        s.commit()

    assert run_as_attempt(run_id, 99, attempt=1) == {
        "step_no": 99, "skipped": "the run has ended or moved past this step's turn"}
    assert "stray" not in looked_up


scripted.script("always-rate-limited", [scripted.fail("rate limited", retryable=True)])
scripted.script("bad-request", [scripted.fail("invalid request", retryable=False)])


def test_retryable_llm_error_is_retried_then_fails_the_run(client, make_worker):
    run_id = start_run(client, script="always-rate-limited", step_max_retries=1)
    drain(make_worker())

    run = get_run(run_id)
    assert run.status == RunStatus.FAILED
    assert run.error.startswith("step 0: ") and run.error.endswith("rate limited")
    [step] = get_steps(run_id)
    assert (step.status.value, step.attempt) == ("failed", 2)
    assert get_task(executor.step_task_id(run_id, 0)).status == TaskStatus.DEAD
    assert len(SPY.requests) == 2


def test_non_retryable_llm_error_fails_the_run(client, make_worker):
    run_id = start_run(client, script="bad-request")
    drain(make_worker())

    run = get_run(run_id)
    assert (run.status, run.error) == (RunStatus.FAILED, "step 0: invalid request")
    [step] = get_steps(run_id)
    assert (step.status.value, step.output) == ("failed", {"error": "invalid request"})
    assert get_task(executor.step_task_id(run_id, 0)).status == TaskStatus.SUCCEEDED
    assert len(SPY.requests) == 1


scripted.script("tool-errors", [
    scripted.call_tools(("strict", {"n": -1}), ("strict", {"m": 1}), ("nope", {}), ("echo", {"text": "x"}),
                        ("strict", {"n": 2})),
    scripted.answer("recovered"),
])


def test_tool_errors_go_back_to_the_llm(client, make_worker):
    run_id = start_run(client, script="tool-errors")  # echo exists, but isn't one of this run's tools
    drain(make_worker())

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    tool_steps = get_steps(run_id)[1:6]
    assert [s.status.value for s in tool_steps] == ["failed"] * 4 + ["succeeded"]
    assert tool_steps[0].output == {"error": "n must not be negative"}
    assert tool_steps[1].output["error"].startswith("bad arguments for 'strict'")
    assert tool_steps[2].output["error"].startswith("unknown tool 'nope'")
    assert tool_steps[3].output["error"].startswith("unknown tool 'echo'")
    results = [m for m in SPY.requests[1].messages if m["role"] == "tool"]
    assert [m["is_error"] for m in results] == [True, True, True, True, False]
    assert results[4]["content"] == 2


def test_cancel_during_an_llm_call_records_it_but_schedules_nothing(client, make_worker):
    run_id = start_run(client)

    def cancel(request):
        SPY.before = None
        assert client.post(f"/runs/{run_id}/cancel").status_code == 200

    SPY.before = cancel
    drain(make_worker())

    run = get_run(run_id)
    assert run.status == RunStatus.CANCELLED
    assert (run.llm_calls, run.input_tokens, run.calls_remaining) == (1, 100, 0)
    assert summary(get_steps(run_id)) == [(0, "llm", 0, "succeeded")]


def test_queued_step_of_a_cancelled_run_does_nothing(client, make_worker):
    run_id = start_run(client)
    client.post(f"/runs/{run_id}/cancel")
    drain(make_worker())

    assert summary(get_steps(run_id)) == [(0, "llm", 0, "pending")]
    assert SPY.requests == []
    assert get_task(executor.step_task_id(run_id, 0)).result == {
        "step_no": 0, "skipped": "the run has ended or moved past this step's turn"}


def test_demo_script_runs_end_to_end(client, make_worker):
    resp = client.post("/runs", json={"goal": "say hello", "model": {"provider": "scripted", "name": "demo-echo"},
                                      "tools": ["echo"]})
    drain(make_worker())

    run = get_run(resp.json()["id"])
    assert (run.status, run.result) == (RunStatus.SUCCEEDED, "The tools said hello and world.")
    assert [s.output for s in get_steps(run.id)][1:3] == ["hello", "world"]



def test_side_effecting_calls_run_once(client, make_worker):
    run_id = start_side_effect_run(client)
    w = make_worker()
    drain(w)
    for n in range(5):  # re-deliver every step
        w.broker.enqueue("default", executor.step_task_id(run_id, n))
    drain(w)

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    assert effects == [("charge", f"{run_id}:1"), ("notify", f"{run_id}:2")]


@pytest.mark.parametrize("point", ["before", "after"])
def test_interrupted_call_without_check_is_never_repeated(client, make_worker, monkeypatch, point):
    """Whether or not the charge happened before the worker died, the next attempt
    can't tell, so it doesn't charge again: it marks the call unknown and pauses."""
    run_id = start_side_effect_run(client)
    faults[f"charge:{point}"] = Crash()
    with pytest.raises(Crash):
        drain(make_worker("killed"))

    reclaim_immediately(monkeypatch)
    drain(make_worker("survivor"))

    assert effects.count(("charge", f"{run_id}:1")) == (1 if point == "after" else 0)
    assert_paused_on_unknown_outcome(run_id, 1, "charge")
    assert get_steps(run_id)[1].attempt == 2
    assert [s.status for s in get_steps(run_id)[2:4]] == [StepStatus.SUCCEEDED] * 2  # the turn's other calls


def test_check_settles_an_interrupted_call_that_happened(client, make_worker, monkeypatch):
    run_id = start_side_effect_run(client)
    faults["notify:after"] = Crash()
    with pytest.raises(Crash):
        drain(make_worker("killed"))

    reclaim_immediately(monkeypatch)
    drain(make_worker("survivor"))

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    notify_step = get_steps(run_id)[2]
    assert (notify_step.status, notify_step.output, notify_step.attempt) == (StepStatus.SUCCEEDED, {"sent": True}, 2)
    assert effects.count(("notify", f"{run_id}:2")) == 1


def test_check_finding_nothing_leaves_the_outcome_unknown(client, make_worker, monkeypatch):
    """No sign of the call doesn't prove it never happens: a stalled attempt could still
    make it. So the call isn't retried."""
    run_id = start_side_effect_run(client)
    faults["notify:before"] = Crash()
    with pytest.raises(Crash):
        drain(make_worker("killed"))

    reclaim_immediately(monkeypatch)
    drain(make_worker("survivor"))

    assert ("notify", f"{run_id}:2") not in effects
    assert_paused_on_unknown_outcome(run_id, 2, "notify")
    assert get_steps(run_id)[2].output == {"error": executor.INTERRUPTED + "; check found no sign of it"}


def test_side_effecting_call_that_raises_is_not_retried(client, make_worker):
    """A timeout or dropped connection may come after the effect, so it isn't treated
    as a failure DTQ can retry."""
    run_id = start_side_effect_run(client)
    faults["charge:after"] = TimeoutError("no response")
    drain(make_worker())

    assert effects.count(("charge", f"{run_id}:1")) == 1
    assert_paused_on_unknown_outcome(run_id, 1, "charge")
    charge_step = get_steps(run_id)[1]
    assert (charge_step.attempt, charge_step.output) == (1, {"error": "the call raised TimeoutError: no response"})
    assert get_task(executor.step_task_id(run_id, 1)).status == TaskStatus.SUCCEEDED


def test_check_settles_a_side_effecting_call_that_raised(client, make_worker):
    run_id = start_side_effect_run(client)
    faults["notify:after"] = ConnectionResetError("dropped")
    drain(make_worker())

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    assert get_steps(run_id)[2].output == {"sent": True}
    assert effects.count(("notify", f"{run_id}:2")) == 1


def test_tool_error_from_a_side_effecting_tool_is_a_definite_failure(client, make_worker):
    run_id = start_side_effect_run(client)
    faults["charge:before"] = ToolError("card declined")
    drain(make_worker())

    assert get_run(run_id).status == RunStatus.SUCCEEDED
    assert get_steps(run_id)[1].output == {"error": "card declined"}
    assert ("charge", f"{run_id}:1") not in effects


def test_unknown_outcome_after_cancel_does_not_pause_the_run(client, make_worker):
    run_id = start_side_effect_run(client)

    def cancel_then_time_out():
        assert client.post(f"/runs/{run_id}/cancel").status_code == 200
        raise TimeoutError("no response")

    faults["charge:after"] = cancel_then_time_out
    drain(make_worker())

    run = get_run(run_id)
    assert (run.status, run.paused_at) == (RunStatus.CANCELLED, None)
    steps = get_steps(run_id)
    assert steps[1].status == StepStatus.UNKNOWN_OUTCOME
    assert StepKind.APPROVAL not in [s.kind for s in steps]


@pytest.mark.parametrize("point", ["after", "before"])
def test_demo_notification_is_sent_at_most_once_despite_a_crash_and_redelivery(client, make_worker, monkeypatch,
                                                                             point):
    """With the real sink. Killed after sending: the step taken over doesn't send
    again, check finds the notification, and the run finishes (1 row). Killed between
    the intent and the send: the outcome is unknown and the run waits for a person
    instead of sending (0 rows). Either way, re-delivering every step's message
    changes nothing."""
    run_id = start_run(client, script="demo-notify", tools=("calculator", "send_notification"))
    real_deliver = demo._deliver

    def deliver_and_die(key, to, text):
        monkeypatch.setattr(demo, "_deliver", real_deliver)
        if point == "after":
            real_deliver(key, to, text)
        raise Crash

    monkeypatch.setattr(demo, "_deliver", deliver_and_die)
    with pytest.raises(Crash):
        drain(make_worker("killed"))  # steps 0-2, then dies in step 3, send_notification

    reclaim_immediately(monkeypatch)
    survivor = make_worker("survivor")
    drain(survivor)
    for step in get_steps(run_id):
        if step.kind != StepKind.APPROVAL:
            survivor.broker.enqueue("default", executor.step_task_id(run_id, step.step_no))
    drain(survivor)

    send = get_steps(run_id)[3]
    assert (send.input["tool"], send.attempt) == ("send_notification", 2)
    if point == "after":
        assert sink_rows(f"{run_id}:3") == 1
        assert send.status == StepStatus.SUCCEEDED and send.output["sent"] is True
        assert get_run(run_id).status == RunStatus.SUCCEEDED
    else:
        assert sink_rows(f"{run_id}:3") == 0
        assert_paused_on_unknown_outcome(run_id, 3, "send_notification")
