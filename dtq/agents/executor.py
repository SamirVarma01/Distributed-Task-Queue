"""Executes a run's steps as DTQ tasks (docs/agent-runtime-design.md, sections 4 to 6).

Each step is a DTQ task, agent_step(run_id, step_no), in three phases:
  A. Begin, in one transaction: check the step's turn is still current, then claim the
     step, fenced on the DTQ attempt number.
  B. Execute, with no transaction open: call the LLM or the tool.
  C. Record, in one transaction: save the outcome if this attempt still owns the step,
     and create the steps that follow. Then send their messages.
Because C creates the next steps' task rows in the same transaction that saves the
outcome, DTQ's tasks table acts as an outbox: if the worker dies before sending them,
DTQ's reconciler does."""

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

import redis
from sqlalchemy import and_, or_, select, update

from .. import registry
from ..broker import Broker
from ..db import SessionLocal, Task, utcnow
from . import llm, scripted, tools  # noqa: F401  (scripted registers its provider)
from .history import build_messages, prompt_sha256
from .models import AgentRun, AgentStep, RunStatus, StepKind, StepStatus
from .states import ALLOWED_FROM, transition

log = logging.getLogger("dtq.agents")

AGENT_STEP = "agent_step"
_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "agent-step.dtq")

broker = Broker()


def step_task_id(run_id: str, step_no: int) -> str:
    """Deterministic, so the primary key stops a step from having two task rows."""
    return str(uuid.uuid5(_NAMESPACE, f"{run_id}:{step_no}"))


def llm_step_input(model: dict[str, Any]) -> dict[str, Any]:
    return {"model": model["name"], "max_output_tokens": model["max_output_tokens"]}


def new_step(s, run: AgentRun, step_no: int, kind: StepKind, turn: int, input: dict[str, Any],
             call_id: str | None = None) -> Task:
    """Add a pending step and its DTQ task to the session. Send the task with send()
    once the session commits."""
    s.add(AgentStep(run_id=run.id, step_no=step_no, turn=turn, kind=kind, status=StepStatus.PENDING,
                    call_id=call_id, input=input,
                    idempotency_key=f"{run.id}:{step_no}" if kind == StepKind.TOOL else None))
    task = Task(id=step_task_id(run.id, step_no), name=AGENT_STEP, queue=run.queue,
                payload={"run_id": run.id, "step_no": step_no}, max_retries=run.step_max_retries)
    s.add(task)
    return task


def send(tasks: list[Task]) -> None:
    """Send committed step tasks to Redis, then mark them enqueued, as POST /tasks does.
    Raises RedisError if Redis is down; DTQ's reconciler sends them later."""
    if not tasks:
        return
    broker.send_many([(t.queue, t.id, None) for t in tasks])
    with SessionLocal() as s:
        s.execute(update(Task).where(Task.id.in_([t.id for t in tasks])).values(enqueued_at=utcnow()))
        s.commit()


@dataclass
class Outcome:
    status: StepStatus  # SUCCEEDED or FAILED
    output: Any
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_micros: int | None = None


def _step_task_died(session, task: Task, error: str | None) -> None:
    """on_dead hook: a step whose DTQ task died (retries used up, or a poison task) can
    never finish, so fail the step and its run in the transaction that marks it dead."""
    run_id, step_no = task.payload["run_id"], task.payload["step_no"]
    reason = (error or "task died").strip().splitlines()[-1]
    session.execute(
        update(AgentStep)
        .where(AgentStep.run_id == run_id, AgentStep.step_no == step_no,
               AgentStep.status.in_([StepStatus.PENDING, StepStatus.STARTED]))
        .values(status=StepStatus.FAILED, output={"error": reason}, finished_at=utcnow())
    )
    transition(session, run_id, ALLOWED_FROM[RunStatus.FAILED], RunStatus.FAILED,
               error=f"step {step_no}: {reason}")


@registry.task(AGENT_STEP, on_dead=_step_task_died, cancellable=False)
def agent_step(run_id: str, step_no: int) -> dict[str, Any]:
    ctx = registry.current_task.get()
    if ctx is None:
        raise RuntimeError("agent_step runs as a DTQ task: it needs the task's attempt number")
    begun = _begin(run_id, step_no, ctx.attempt)
    if isinstance(begun, str):
        log.info("run %s step %d: nothing to do (%s)", run_id, step_no, begun)
        return {"step_no": step_no, "skipped": begun}

    run, step, messages = begun
    outcome = _call_llm(run, step, messages) if step.kind == StepKind.LLM else _call_tool(run, step)

    next_tasks = _record(run, step, ctx.attempt, outcome)
    if next_tasks is None:
        log.warning("run %s step %d attempt %d was taken over by a later attempt; dropping its outcome",
                    run_id, step_no, ctx.attempt)
        return {"step_no": step_no, "skipped": "taken over by a later attempt"}
    try:
        send(next_tasks)
    except redis.RedisError:
        log.exception("run %s: couldn't send the next steps; the reconciler will", run_id)
    return {"step_no": step_no, "status": outcome.status.value}


def _begin(run_id: str, step_no: int, attempt: int) -> tuple[AgentRun, AgentStep, list | None] | str:
    """Phase A. Returns the run, the step and (for an LLM step) its prompt, or a reason
    there is nothing to do."""
    with SessionLocal() as s, s.begin():
        step = s.get(AgentStep, (run_id, step_no))  # kind and turn never change
        if step is None:
            return "no such step"
        # Lock the run row, and check the step's turn is still current and the run may
        # start it. The lock orders this against a concurrent cancel.
        startable = [RunStatus.QUEUED, RunStatus.RUNNING] if step.kind == StepKind.LLM else \
            [RunStatus.RUNNING, RunStatus.AWAITING_APPROVAL]
        current = s.execute(
            update(AgentRun)
            .where(AgentRun.id == run_id, AgentRun.turn == step.turn, AgentRun.status.in_(startable))
            .values(turn=AgentRun.turn)
        )
        if not current.rowcount:
            return "the run has ended or moved past this step's turn"
        if step.kind == StepKind.LLM:
            transition(s, run_id, RunStatus.QUEUED, RunStatus.RUNNING, started_at=utcnow())

        # Claim the step: a pending one, or one a lower (stale) attempt left started. A
        # step that finished, or that a higher attempt holds, is left alone.
        claimed = s.execute(
            update(AgentStep)
            .where(AgentStep.run_id == run_id, AgentStep.step_no == step_no,
                   or_(AgentStep.status == StepStatus.PENDING,
                       and_(AgentStep.status == StepStatus.STARTED, AgentStep.attempt < attempt)))
            .values(status=StepStatus.STARTED, attempt=attempt, started_at=utcnow())
        )
        if not claimed.rowcount:
            return "the step has finished, or a later attempt owns it"

        run = s.get(AgentRun, run_id)
        if step.kind != StepKind.LLM:
            return run, step, None
        steps = s.scalars(select(AgentStep).where(AgentStep.run_id == run_id)).all()
        messages = build_messages(run.goal, steps)
        s.execute(
            update(AgentStep)
            .where(AgentStep.run_id == run_id, AgentStep.step_no == step_no)
            .values(input={**step.input, "messages_sha256": prompt_sha256(run.model["system"], messages)})
        )
        return run, step, messages


def _call_llm(run: AgentRun, step: AgentStep, messages: list) -> Outcome:
    """Phase B for an LLM step. A retryable provider error propagates, so DTQ retries
    the step with backoff; any other error fails the run."""
    provider = llm.get_provider(run.model["provider"])
    if provider is None:
        return Outcome(StepStatus.FAILED, {"error": f"unknown provider {run.model['provider']!r}"})
    request = llm.LLMRequest(
        model=run.model["name"],
        system=run.model["system"],
        messages=messages,
        tools=[llm.ToolSpec(t.name, t.description) for name in run.tools if (t := tools.get(name))],
        max_output_tokens=run.model["max_output_tokens"],
        idempotency_key=f"{run.id}:{step.step_no}",
    )
    try:
        response = provider.complete(request)
    except llm.ProviderError as exc:
        if exc.retryable:
            raise
        return Outcome(StepStatus.FAILED, {"error": str(exc)})
    return Outcome(StepStatus.SUCCEEDED, response.to_output(), response.input_tokens, response.output_tokens,
                   llm.cost_micros(run.model["name"], response.input_tokens, response.output_tokens))


def _call_tool(run: AgentRun, step: AgentStep) -> Outcome:
    """Phase B for a tool step. Errors the agent can act on (an unknown tool, bad
    arguments, a ToolError) go back to the LLM as the call's result. Any other
    exception propagates, so DTQ retries the step: every registered tool is read-only
    or idempotent, so running it again is safe."""
    name, args = step.input["tool"], step.input["args"]
    t = tools.get(name) if name in run.tools else None
    if t is None:
        return Outcome(StepStatus.FAILED, {"error": f"unknown tool {name!r}; this run's tools: {run.tools}"})
    try:
        kwargs = tools.bind_args(t, args, step.idempotency_key)
    except TypeError as exc:
        return Outcome(StepStatus.FAILED, {"error": f"bad arguments for {name!r}: {exc}"})
    try:
        result = t.fn(**kwargs)
    except tools.ToolError as exc:
        return Outcome(StepStatus.FAILED, {"error": str(exc)})
    try:
        json.dumps(result, allow_nan=False)
    except (TypeError, ValueError):
        result = repr(result)
    return Outcome(StepStatus.SUCCEEDED, result)


def _record(run: AgentRun, step: AgentStep, attempt: int, outcome: Outcome) -> list[Task] | None:
    """Phase C. Returns the tasks of the steps it created, or None if a later attempt
    has taken over the step (nothing is recorded then)."""
    with SessionLocal() as s, s.begin():
        recorded = s.execute(
            update(AgentStep)
            .where(AgentStep.run_id == run.id, AgentStep.step_no == step.step_no,
                   AgentStep.status == StepStatus.STARTED, AgentStep.attempt == attempt)
            .values(status=outcome.status, output=outcome.output, input_tokens=outcome.input_tokens,
                    output_tokens=outcome.output_tokens, cost_micros=outcome.cost_micros, finished_at=utcnow())
        )
        if not recorded.rowcount:
            return None
        if step.kind == StepKind.LLM:
            s.execute(
                update(AgentRun).where(AgentRun.id == run.id).values(
                    llm_calls=AgentRun.llm_calls + 1,
                    input_tokens=AgentRun.input_tokens + (outcome.input_tokens or 0),
                    output_tokens=AgentRun.output_tokens + (outcome.output_tokens or 0),
                    cost_micros=AgentRun.cost_micros + (outcome.cost_micros or 0),
                )
            )
            return _after_llm(s, run, step, outcome)
        return _after_tool(s, run, step)


def _after_llm(s, run: AgentRun, step: AgentStep, outcome: Outcome) -> list[Task]:
    """Finish the run, or schedule the turn's tool calls. If the run was cancelled
    while the call ran, the transitions and the guard match nothing, so nothing is
    scheduled."""
    if outcome.status == StepStatus.FAILED:
        transition(s, run.id, RunStatus.RUNNING, RunStatus.FAILED,
                   error=f"step {step.step_no}: {outcome.output['error']}")
        return []
    calls = outcome.output["tool_calls"]
    if not calls:
        transition(s, run.id, RunStatus.RUNNING, RunStatus.SUCCEEDED, result=outcome.output["text"])
        return []
    scheduling = s.execute(
        update(AgentRun)
        .where(AgentRun.id == run.id, AgentRun.turn == step.turn, AgentRun.status == RunStatus.RUNNING)
        .values(calls_remaining=len(calls))
    )
    if not scheduling.rowcount:
        return []
    first = s.scalar(select(AgentRun.next_step_no).where(AgentRun.id == run.id).with_for_update())
    s.execute(update(AgentRun).where(AgentRun.id == run.id).values(next_step_no=first + len(calls)))
    return [new_step(s, run, first + i, StepKind.TOOL, step.turn, {"tool": c["name"], "args": c["args"]}, c["id"])
            for i, c in enumerate(calls)]


def _after_tool(s, run: AgentRun, step: AgentStep) -> list[Task]:
    """Count this call's result against the turn. The call that takes calls_remaining
    from 1 to 0 creates the next LLM step, so the next turn starts exactly once."""
    last = s.execute(
        update(AgentRun)
        .where(AgentRun.id == run.id, AgentRun.turn == step.turn, AgentRun.calls_remaining == 1,
               AgentRun.status == RunStatus.RUNNING)
        .values(calls_remaining=0)
    )
    if not last.rowcount:
        s.execute(
            update(AgentRun)
            .where(AgentRun.id == run.id, AgentRun.turn == step.turn, AgentRun.calls_remaining > 0)
            .values(calls_remaining=AgentRun.calls_remaining - 1)
        )
        return []
    n = s.scalar(select(AgentRun.next_step_no).where(AgentRun.id == run.id).with_for_update())
    s.execute(update(AgentRun).where(AgentRun.id == run.id).values(turn=n, next_step_no=n + 1))
    return [new_step(s, run, n, StepKind.LLM, n, llm_step_input(run.model))]
