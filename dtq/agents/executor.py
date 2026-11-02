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
from sqlalchemy import select, update

from .. import registry
from ..broker import Broker
from ..db import SessionLocal, Task, utcnow
from . import budgets, llm, scripted, tools  # noqa: F401  (scripted registers its provider)
from .history import build_messages, prompt_sha256
from .models import AgentRun, AgentStep, RunStatus, StepKind, StepStatus
from .states import ALLOWED_FROM, transition
from .tools import Effect

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
             call_id: str | None = None, idempotency_key: str | None = None) -> Task:
    """Add a pending step and its DTQ task to the session. Send the task with send()
    once the session commits. A tool step's idempotency key is "<run_id>:<step_no>",
    unless it retries an earlier call, whose key it keeps."""
    if kind == StepKind.TOOL and idempotency_key is None:
        idempotency_key = f"{run.id}:{step_no}"
    s.add(AgentStep(run_id=run.id, step_no=step_no, turn=turn, kind=kind, status=StepStatus.PENDING,
                    call_id=call_id, input=input, idempotency_key=idempotency_key))
    task = Task(id=step_task_id(run.id, step_no), name=AGENT_STEP, queue=run.queue,
                payload={"run_id": run.id, "step_no": step_no}, max_retries=run.step_max_retries)
    s.add(task)
    return task


def allocate_steps(s, run_id: str, count: int) -> int:
    """Reserve count step numbers, returning the first. The caller holds the run row's
    lock, so numbers are handed out in order and never twice."""
    first = s.scalar(select(AgentRun.next_step_no).where(AgentRun.id == run_id).with_for_update())
    s.execute(update(AgentRun).where(AgentRun.id == run_id).values(next_step_no=first + count))
    return first


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

    run, step, messages, took_over = begun
    outcome = _call_llm(run, step, messages) if step.kind == StepKind.LLM else _call_tool(run, step, took_over)

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


def _begin(run_id: str, step_no: int, attempt: int) -> tuple[AgentRun, AgentStep, list | None, bool] | str:
    """Phase A. Returns the run, the step, its prompt (LLM steps only) and whether this
    attempt took the step over from an earlier one; or a reason there is nothing to do.

    Claiming the step commits it as started before anything is called. For a tool with
    side effects, that row is the intent record: an attempt that takes the step over
    never makes the call again (see _call_tool)."""
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

        run = s.get(AgentRun, run_id)
        this_step = (AgentStep.run_id == run_id, AgentStep.step_no == step_no)
        # Claim the step if it is pending; otherwise take it over if a lower (stale)
        # attempt left it started. A step that finished, or that a higher attempt
        # holds, is left alone.
        fresh = s.execute(
            update(AgentStep).where(*this_step, AgentStep.status == StepStatus.PENDING)
            .values(status=StepStatus.STARTED, attempt=attempt, started_at=utcnow())
        )
        took_over = False
        if not fresh.rowcount:
            taken = s.execute(
                update(AgentStep)
                .where(*this_step, AgentStep.status == StepStatus.STARTED, AgentStep.attempt < attempt)
                .values(attempt=attempt, started_at=utcnow())
            )
            if not taken.rowcount:
                return "the step has finished, or a later attempt owns it"
            took_over = True

        if step.kind != StepKind.LLM:
            return run, step, None, took_over
        over = budgets.exceeded(run, utcnow())
        if over:
            s.execute(update(AgentStep).where(*this_step, AgentStep.attempt == attempt).values(
                status=StepStatus.FAILED, output={"error": f"not run: budget exceeded ({over})"}, finished_at=utcnow()))
            transition(s, run_id, RunStatus.RUNNING, RunStatus.BUDGET_EXCEEDED, error=f"budget exceeded: {over}")
            return f"budget exceeded: {over}"
        steps = s.scalars(select(AgentStep).where(AgentStep.run_id == run_id)).all()
        messages = build_messages(run.goal, steps)
        s.execute(
            update(AgentStep)
            .where(AgentStep.run_id == run_id, AgentStep.step_no == step_no)
            .values(input={**step.input, "messages_sha256": prompt_sha256(run.model["system"], messages)})
        )
        return run, step, messages, took_over


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
        tools=[llm.ToolSpec(t.name, t.description, t.parameters) for name in run.tools if (t := tools.get(name))],
        max_output_tokens=budgets.max_output_tokens(run),
        idempotency_key=f"{run.id}:{step.step_no}",
        effort=run.model.get("effort"),
    )
    try:
        response = provider.complete(request)
    except llm.ProviderError as exc:
        if exc.retryable:
            raise
        # A call that failed for good can still have used tokens (a response cut off
        # at its limit), and they count toward the run's usage and budgets.
        usage = exc.usage_by_model
        return Outcome(StepStatus.FAILED, {"error": str(exc)}, sum(u[1] for u in usage), sum(u[2] for u in usage),
                       llm.cost_of(usage))
    usage = response.usage_by_model or [(run.model["name"], response.input_tokens, response.output_tokens)]
    return Outcome(StepStatus.SUCCEEDED, response.to_output(), response.input_tokens, response.output_tokens,
                   llm.cost_of(usage))


def _run_tool(run: AgentRun, name: str) -> tools.Tool | None:
    """The tool called name, or None if it isn't one of the run's tools."""
    return tools.get(name) if name in run.tools else None


def _tool_for(run: AgentRun, step: AgentStep) -> tools.Tool | None:
    return _run_tool(run, step.input["tool"]) if step.kind == StepKind.TOOL else None


def _call_tool(run: AgentRun, step: AgentStep, took_over: bool) -> Outcome:
    """Phase B for a tool step. Errors the agent can act on (an unknown tool, bad
    arguments, a ToolError) go back to the LLM as the call's result.

    Any other exception from a read-only or idempotent tool propagates, so DTQ retries
    the step. A side-effecting call is never made twice: if an earlier attempt may
    have made it, or this one raised partway, the outcome is settled by the tool's
    check, or marked unknown."""
    name, args = step.input["tool"], step.input["args"]
    t = _tool_for(run, step)
    if t is None:
        return Outcome(StepStatus.FAILED, {"error": f"unknown tool {name!r}; this run's tools: {run.tools}"})
    try:
        kwargs = tools.bind_args(t, args, step.idempotency_key)
    except TypeError as exc:
        return Outcome(StepStatus.FAILED, {"error": f"bad arguments for {name!r}: {exc}"})
    if took_over and t.effect is Effect.SIDE_EFFECT:
        return _check(t, step, INTERRUPTED)
    try:
        result = t.fn(**kwargs)
    except tools.ToolError as exc:
        return Outcome(StepStatus.FAILED, {"error": str(exc)})
    except Exception as exc:
        if t.effect is not Effect.SIDE_EFFECT:
            raise
        log.exception("run %s step %d: side-effecting call %s raised", run.id, step.step_no, name)
        return _check(t, step, f"the call raised {type(exc).__name__}: {exc}")
    return Outcome(StepStatus.SUCCEEDED, _json_safe(result))


INTERRUPTED = "the worker running this call stopped mid-call"


def _check(t: tools.Tool, step: AgentStep, why: str) -> Outcome:
    """Settle a side-effecting call that may or may not have happened. The tool's check
    can prove the call happened. It can't prove the call never will (a stalled attempt
    could still make it), so finding nothing leaves the outcome unknown. If check
    raises, DTQ retries the step and the next attempt checks again."""
    if t.check is not None:
        found = t.check(step.idempotency_key)
        if found is not None:
            log.info("run %s step %d: check found the call happened", step.run_id, step.step_no)
            return Outcome(StepStatus.SUCCEEDED, _json_safe(found))
        why += "; check found no sign of it"
    return Outcome(StepStatus.UNKNOWN_OUTCOME, {"error": why})


def _json_safe(result: Any) -> Any:
    """MySQL's JSON type can't hold everything json.dumps accepts (NaN, Infinity), nor
    arbitrary objects: store such a result's repr instead, as DTQ does."""
    try:
        json.dumps(result, allow_nan=False)
    except (TypeError, ValueError):
        return repr(result)
    return result


def _ask_for_approval(s, run_id: str, turn: int, input: dict[str, Any], call_id: str | None = None) -> None:
    """Add a waiting approval step, and pause the run if it is running. The caller holds
    the run row's lock. A person decides through POST /runs/{id}/approve or /reject."""
    n = allocate_steps(s, run_id, 1)
    s.add(AgentStep(run_id=run_id, step_no=n, turn=turn, kind=StepKind.APPROVAL, status=StepStatus.WAITING,
                    call_id=call_id, input=input))
    transition(s, run_id, RunStatus.RUNNING, RunStatus.AWAITING_APPROVAL, paused_at=utcnow())


def _pause_for_unknown_outcome(s, run_id: str, step: AgentStep) -> None:
    """Ask a person about a side-effecting call whose outcome is unknown. The turn's
    other calls keep running; the next turn waits, because this call has no result."""
    _ask_for_approval(s, run_id, step.turn, call_id=step.call_id, input={
        "reason": "unknown_outcome", "step_no": step.step_no,
        "calls": [{"call_id": step.call_id, "tool": step.input["tool"], "args": step.input["args"]}]})


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
        if outcome.status == StepStatus.UNKNOWN_OUTCOME:
            active = s.execute(
                update(AgentRun)
                .where(AgentRun.id == run.id, AgentRun.status.in_([RunStatus.RUNNING, RunStatus.AWAITING_APPROVAL]))
                .values(turn=AgentRun.turn)
            )
            if active.rowcount:
                _pause_for_unknown_outcome(s, run.id, step)
            return []
        return call_finished(s, run, step.turn)


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
    gated = [c for c in calls if (t := _run_tool(run, c["name"])) and t.requires_approval]
    if not gated:
        return schedule_tools(s, run, step.turn, calls)
    # One approval for the turn, listing the calls that need it. None of the turn's
    # calls run until it is approved: approving schedules them all.
    pausing = s.execute(
        update(AgentRun)
        .where(AgentRun.id == run.id, AgentRun.turn == step.turn, AgentRun.status == RunStatus.RUNNING)
        .values(turn=AgentRun.turn)
    )
    if pausing.rowcount:
        _ask_for_approval(s, run.id, step.turn, input={
            "reason": "approval_required",
            "calls": [{"call_id": c["id"], "tool": c["name"], "args": c["args"]} for c in gated]})
    return []


def schedule_tools(s, run: AgentRun, turn: int, calls: list[dict[str, Any]]) -> list[Task]:
    """Create a tool step for each of the turn's calls, numbered in the order the LLM
    listed them, if the run is still running that turn."""
    scheduling = s.execute(
        update(AgentRun)
        .where(AgentRun.id == run.id, AgentRun.turn == turn, AgentRun.status == RunStatus.RUNNING)
        .values(calls_remaining=len(calls))
    )
    if not scheduling.rowcount:
        return []
    first = allocate_steps(s, run.id, len(calls))
    return [new_step(s, run, first + i, StepKind.TOOL, turn, {"tool": c["name"], "args": c["args"]}, c["id"])
            for i, c in enumerate(calls)]


def call_finished(s, run: AgentRun, turn: int) -> list[Task]:
    """Count a call's result against its turn. The call that takes calls_remaining from
    1 to 0 creates the next LLM step, so the next turn starts exactly once."""
    last = s.execute(
        update(AgentRun)
        .where(AgentRun.id == run.id, AgentRun.turn == turn, AgentRun.calls_remaining == 1,
               AgentRun.status == RunStatus.RUNNING)
        .values(calls_remaining=0)
    )
    if not last.rowcount:
        s.execute(
            update(AgentRun)
            .where(AgentRun.id == run.id, AgentRun.turn == turn, AgentRun.calls_remaining > 0)
            .values(calls_remaining=AgentRun.calls_remaining - 1)
        )
        return []
    n = allocate_steps(s, run.id, 1)
    s.execute(update(AgentRun).where(AgentRun.id == run.id).values(turn=n))
    return [new_step(s, run, n, StepKind.LLM, n, llm_step_input(run.model))]
