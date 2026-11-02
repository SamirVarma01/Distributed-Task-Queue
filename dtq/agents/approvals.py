"""A person's decisions on a paused run (docs/agent-runtime-design.md, section 8).

Each decision is one transaction that first locks the run row and checks the run is
awaiting approval. A decision racing a cancel, another decision, or the same click
twice is settled by whichever commits first; the others are told the run or step
has moved on."""

from typing import Any

from sqlalchemy import func, select, update

from ..db import SessionLocal, Task, utcnow
from . import executor
from .models import AgentRun, AgentStep, RunStatus, StepKind, StepStatus
from .states import transition

UNSET: Any = object()  # approve() without a tool_output


class DecisionError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


def approve(run_id: str, step_no: int, *, note: str | None = None, tool_output: Any = UNSET) -> list[Task]:
    """Approve the waiting approval step step_no. Returns the tasks it created, to be
    sent once committed (executor.send).

    For a call whose outcome is unknown: without tool_output, the person says it didn't
    happen, and a new tool step retries it with the same idempotency key. With
    tool_output, they say it happened and this was its result, so nothing is retried."""
    with SessionLocal() as s, s.begin():
        run = _lock(s, run_id)
        approval = _waiting_step(s, run_id, step_no)
        reason = approval.input["reason"]
        if tool_output is not UNSET and reason != "unknown_outcome":
            raise DecisionError(400, "tool_output only applies to a call whose outcome is unknown")
        output = {"decision": "approve", "note": note}
        if tool_output is not UNSET:
            output["tool_output"] = tool_output
        _decide(s, approval, output)
        _resume_if_nothing_waits(s, run)

        if reason == "approval_required":
            llm_step = s.get(AgentStep, (run_id, approval.turn))
            return executor.schedule_tools(s, run, approval.turn, llm_step.output["tool_calls"])
        if tool_output is not UNSET:
            return executor.call_finished(s, run, approval.turn)
        unknown = s.get(AgentStep, (run_id, approval.input["step_no"]))
        retry_no = executor.allocate_steps(s, run_id, 1)
        return [executor.new_step(s, run, retry_no, StepKind.TOOL, unknown.turn, unknown.input, unknown.call_id,
                                  idempotency_key=unknown.idempotency_key)]


def reject(run_id: str, step_no: int, *, note: str | None = None) -> None:
    """Reject the waiting approval step step_no, which fails the run."""
    with SessionLocal() as s, s.begin():
        _lock(s, run_id)
        _decide(s, _waiting_step(s, run_id, step_no), {"decision": "reject", "note": note})
        transition(s, run_id, RunStatus.AWAITING_APPROVAL, RunStatus.FAILED, paused_at=None,
                   error=f"step {step_no} was rejected" + (f": {note}" if note else ""))


def _lock(s, run_id: str) -> AgentRun:
    """Lock the run row, and check the run is awaiting approval."""
    locked = s.execute(
        update(AgentRun)
        .where(AgentRun.id == run_id, AgentRun.status == RunStatus.AWAITING_APPROVAL)
        .values(turn=AgentRun.turn)
    )
    run = s.get(AgentRun, run_id)
    if run is None:
        raise DecisionError(404, "run not found")
    if not locked.rowcount:
        raise DecisionError(409, f"run is {run.status.value}, not awaiting approval")
    return run


def _waiting_step(s, run_id: str, step_no: int) -> AgentStep:
    step = s.get(AgentStep, (run_id, step_no))
    if step is None or step.kind != StepKind.APPROVAL or step.status != StepStatus.WAITING:
        raise DecisionError(409, f"step {step_no} isn't waiting for a decision")
    return step


def _decide(s, approval: AgentStep, output: dict[str, Any]) -> None:
    s.execute(
        update(AgentStep)
        .where(AgentStep.run_id == approval.run_id, AgentStep.step_no == approval.step_no,
               AgentStep.status == StepStatus.WAITING)
        .values(status=StepStatus.SUCCEEDED, output=output, finished_at=utcnow())
    )


def _resume_if_nothing_waits(s, run: AgentRun) -> None:
    """Resume the run unless another approval step still waits, adding this pause to
    paused_seconds, which wall-time budgets don't count."""
    waiting = s.scalar(
        select(func.count()).select_from(AgentStep)
        .where(AgentStep.run_id == run.id, AgentStep.kind == StepKind.APPROVAL, AgentStep.status == StepStatus.WAITING)
    )
    if waiting:
        return
    paused = int((utcnow() - run.paused_at).total_seconds()) if run.paused_at else 0
    transition(s, run.id, RunStatus.AWAITING_APPROVAL, RunStatus.RUNNING,
               paused_at=None, paused_seconds=AgentRun.paused_seconds + paused)
