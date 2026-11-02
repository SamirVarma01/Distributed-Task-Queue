"""Shared by the agent runtime tests: test tools, a recording LLM provider, scripts and
helpers. A test module imports agent_env to apply the fixture."""

import pytest
from sqlalchemy import select

from dtq import config, registry
from dtq.agents import demo, executor, llm, scripted
from dtq.agents.models import AgentRun, AgentStep, RunStatus, StepKind, StepStatus
from dtq.agents.tools import ToolError, tool
from dtq.db import SessionLocal


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
def agent_env(monkeypatch):
    """Import into a test module to apply there: clears the test tools' and the spy's
    records, prices the three-turns script, and turns off automatic reclaiming and
    reconciling."""
    SPY.requests.clear()
    SPY.before = None
    looked_up.clear()
    lookup_hooks.clear()
    monkeypatch.setitem(llm.PRICES, "three-turns", (2.0, 10.0))
    monkeypatch.setattr(config, "RETRY_BACKOFF_BASE_SECONDS", 0)
    # Only reclaim or reconcile when a test asks for it.
    monkeypatch.setattr(config, "RECLAIM_INTERVAL_SECONDS", float("inf"))
    monkeypatch.setattr(config, "RECONCILE_INTERVAL_SECONDS", float("inf"))
    effects.clear()
    faults.clear()


def start_run(client, script: str = "three-turns", tools=("lookup", "strict"), **kw) -> str:
    resp = client.post("/runs", json={"goal": "find a, b and c", "model": {"provider": "spy", "name": script},
                                      "tools": list(tools), **kw})
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


# Side effects. effects is "the outside world": what the side-effecting tools did.
effects: list[tuple[str, str]] = []
# "<tool>:before" / "<tool>:after" the effect -> an exception to raise there, or a
# function to call there, once.
faults: dict = {}


def fault(point: str) -> None:
    f = faults.pop(point, None)
    if isinstance(f, BaseException):
        raise f
    if f is not None:
        f()


@tool(effect="side_effect")
def charge(amount: int, *, idempotency_key: str) -> dict:
    """Charge an amount. Not safe to repeat, and no way to tell afterwards if it happened."""
    fault("charge:before")
    effects.append(("charge", idempotency_key))
    fault("charge:after")
    return {"charged": amount}


def notified(idempotency_key: str):
    return {"sent": True} if ("notify", idempotency_key) in effects else None


@tool(effect="side_effect", check=notified)
def notify(text: str, *, idempotency_key: str) -> dict:
    """Send a notification. Not safe to repeat, but check can tell if it was sent."""
    fault("notify:before")
    effects.append(("notify", idempotency_key))
    fault("notify:after")
    return {"sent": True}


scripted.script("side-effects", [
    scripted.call_tools(("charge", {"amount": 5}), ("notify", {"text": "paid"}), ("lookup", {"key": "a"})),
    scripted.answer("charged and notified"),
])
SIDE_EFFECT_TOOLS = ("charge", "notify", "lookup")


def start_side_effect_run(client) -> str:
    return start_run(client, script="side-effects", tools=SIDE_EFFECT_TOOLS)


def assert_paused_on_unknown_outcome(run_id: str, step_no: int, tool_name: str):
    steps = get_steps(run_id)
    unknown, approval = steps[step_no], steps[-1]
    assert unknown.status == StepStatus.UNKNOWN_OUTCOME
    assert (approval.kind, approval.status, approval.turn) == (StepKind.APPROVAL, StepStatus.WAITING, unknown.turn)
    assert approval.input["reason"] == "unknown_outcome" and approval.input["step_no"] == step_no
    assert [c["tool"] for c in approval.input["calls"]] == [tool_name]
    run = get_run(run_id)
    assert run.status == RunStatus.AWAITING_APPROVAL and run.paused_at is not None
    # The turn waits on this call, so the next turn hasn't started.
    assert (run.turn, run.calls_remaining) == (unknown.turn, 1)
    assert not [s for s in steps if s.kind == StepKind.LLM and s.turn > unknown.turn]


def sink_rows(idempotency_key: str) -> int:
    with SessionLocal() as s:
        return len(s.scalars(select(demo.SinkEntry).where(demo.SinkEntry.idempotency_key == idempotency_key)).all())
