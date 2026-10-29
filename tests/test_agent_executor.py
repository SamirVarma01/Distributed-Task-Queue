import pytest
from sqlalchemy import select

from dtq import config, registry
from dtq.agents import executor, llm, scripted
from dtq.agents.history import prompt_sha256
from dtq.agents.models import AgentRun, AgentStep, RunStatus, StepKind, StepStatus
from dtq.agents.tools import ToolError, tool
from dtq.db import SessionLocal, TaskStatus

from .conftest import get_task, pending_count


class Crash(BaseException):
    """A worker dying mid-step. DTQ's worker doesn't catch it, so the message stays
    pending and the task running, as after a SIGKILL."""


looked_up: list[str] = []  # keys passed to lookup, in call order
lookup_hooks: dict = {}  # key -> function run before lookup returns


@tool(effect="read_only")
def lookup(key: str) -> str:
    """Look a key up."""
    looked_up.append(key)
    if key in lookup_hooks:
        lookup_hooks.pop(key)()
    return f"value of {key}"


@tool(effect="read_only")
def strict(n: int) -> int:
    """Return n, which must not be negative."""
    if n < 0:
        raise ToolError("n must not be negative")
    return n


class Spy:
    """Answers from the scripts, recording every request and running an optional hook
    before answering."""

    def __init__(self):
        self.requests, self.before = [], None

    def complete(self, request):
        self.requests.append(request)
        if self.before:
            self.before(request)
        return llm.get_provider("scripted").complete(request)


SPY = Spy()
llm.register_provider("spy", SPY)

scripted.script("three-turns", [
    scripted.call_tools(("lookup", {"key": "a"}), ("lookup", {"key": "b"}), input_tokens=100, output_tokens=10),
    scripted.call_tools(("lookup", {"key": "c"}), input_tokens=200, output_tokens=20),
    scripted.answer("a, b and c found", input_tokens=300, output_tokens=30),
])
EXPECTED_STEPS = [(0, "llm", 0, "succeeded"), (1, "tool", 0, "succeeded"), (2, "tool", 0, "succeeded"),
                  (3, "llm", 3, "succeeded"), (4, "tool", 3, "succeeded"), (5, "llm", 5, "succeeded")]


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    SPY.requests.clear()
    SPY.before = None
    looked_up.clear()
    lookup_hooks.clear()
    monkeypatch.setitem(llm.PRICES, "three-turns", (2.0, 10.0))
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE_SECONDS", 0)
    # Only reclaim or reconcile when a test asks for it.
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", float("inf"))
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", float("inf"))


def start_run(client, script: str = "three-turns", **kw) -> str:
    resp = client.post("/runs", json={"goal": "find a, b and c", "model": {"provider": "spy", "name": script},
                                      "tools": ["lookup", "strict"], **kw})
    assert resp.status_code == 202, resp.text
    return resp.json()["id"]


def drain(worker) -> None:
    while worker.step():
        pass


def reclaim_immediately(monkeypatch) -> None:
    monkeypatch.setattr(config, "VISIBILITY_TIMEOUT_MS", 0)
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", 0)


def get_run(run_id: str) -> AgentRun:
    with SessionLocal() as s:
        return s.get(AgentRun, run_id)


def get_steps(run_id: str) -> list[AgentStep]:
    with SessionLocal() as s:
        return s.scalars(select(AgentStep).where(AgentStep.run_id == run_id).order_by(AgentStep.step_no)).all()


def summary(steps) -> list[tuple]:
    return [(s.step_no, s.kind.value, s.turn, s.status.value) for s in steps]


def turn_of(request) -> int:
    return sum(1 for m in request.messages if m["role"] == "assistant")


def run_as_attempt(run_id: str, step_no: int, attempt: int) -> dict:
    """Run agent_step directly, as the given DTQ attempt of the step's task."""
    token = registry.current_task.set(registry.TaskContext(executor.step_task_id(run_id, step_no), attempt))
    try:
        return executor.agent_step(run_id, step_no)
    finally:
        registry.current_task.reset(token)


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
    run1, step1, prompt1 = executor._begin(run_id, 0, 1)
    run3, step3, prompt3 = executor._begin(run_id, 0, 3)  # a reclaim takes the step over

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
