import uuid
from datetime import datetime
from typing import Any

import redis
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update

from .. import config
from ..db import SessionLocal, Task, TaskStatus, utcnow
from . import executor
from .models import AgentRun, AgentStep, RunStatus, StepKind, StepStatus
from .states import ALLOWED_FROM, transition

router = APIRouter(tags=["agent runs"])


class ModelConfig(BaseModel):
    provider: str
    name: str
    system: str = ""
    max_output_tokens: int = Field(1024, ge=1)


class SubmitRun(BaseModel):
    goal: str = Field(min_length=1)
    model: ModelConfig
    tools: list[str] = Field(default_factory=list)
    queue: str = "default"
    step_max_retries: int = Field(3, ge=0, le=20)
    # Budgets. None means no limit; max_steps counts LLM calls.
    max_steps: int = Field(20, ge=1)
    max_tokens: int | None = Field(None, ge=1)
    max_cost_micros: int | None = Field(None, ge=1)
    max_wall_seconds: int | None = Field(None, ge=1)


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    id: str
    goal: str
    status: RunStatus
    model: dict[str, Any]
    tools: list[str]
    queue: str
    step_max_retries: int
    max_steps: int
    max_tokens: int | None
    max_cost_micros: int | None
    max_wall_seconds: int | None
    llm_calls: int
    input_tokens: int
    output_tokens: int
    cost_micros: int
    turn: int
    calls_remaining: int
    next_step_no: int
    paused_seconds: int
    paused_at: datetime | None
    result: Any = None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class StepOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    step_no: int
    turn: int
    kind: StepKind
    status: StepStatus
    attempt: int | None
    call_id: str | None
    input: dict[str, Any]
    output: Any = None
    input_tokens: int | None
    output_tokens: int | None
    cost_micros: int | None
    idempotency_key: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@router.post("/runs", response_model=RunOut, status_code=202)
def submit_run(body: SubmitRun):
    """Create a run and its first step, an LLM call, and enqueue that step. Same order
    and failure handling as POST /tasks: the rows are committed before the message is
    sent, and the reconciler re-sends the step if the API dies in between."""
    if body.queue not in config.QUEUES:
        raise HTTPException(400, f"unknown queue {body.queue!r}; available: {config.QUEUES}")

    run = AgentRun(id=str(uuid.uuid4()), **body.model_dump(exclude={"model"}), model=body.model.model_dump())
    with SessionLocal() as s:
        s.add(run)
        s.flush()  # the step's foreign key needs the run's row first
        first = executor.new_step(s, run, 0, StepKind.LLM, 0, executor.llm_step_input(run.model))
        s.commit()
        try:
            executor.send([first])
        except redis.RedisError as exc:
            error = f"enqueue failed: {exc}"
            s.execute(update(Task).where(Task.id == first.id)
                      .values(status=TaskStatus.DEAD, error=error, finished_at=utcnow()))
            transition(s, run.id, RunStatus.QUEUED, RunStatus.FAILED, error=error)
            s.commit()
            raise HTTPException(503, "broker unavailable")
    return run


@router.get("/runs/{run_id}", response_model=RunOut)
def get_run(run_id: str):
    with SessionLocal() as s:
        run = s.get(AgentRun, run_id)
    if run is None:
        raise HTTPException(404, "run not found")
    return run


@router.get("/runs/{run_id}/steps", response_model=list[StepOut])
def list_steps(run_id: str):
    with SessionLocal() as s:
        if s.get(AgentRun, run_id) is None:
            raise HTTPException(404, "run not found")
        return s.scalars(select(AgentStep).where(AgentStep.run_id == run_id).order_by(AgentStep.step_no)).all()


@router.post("/runs/{run_id}/cancel", response_model=RunOut)
def cancel_run(run_id: str):
    """Cancel a run that hasn't finished. A step already executing finishes and records
    its outcome, but nothing after it runs."""
    with SessionLocal() as s:
        cancelled = transition(s, run_id, ALLOWED_FROM[RunStatus.CANCELLED], RunStatus.CANCELLED)
        s.commit()
        run = s.get(AgentRun, run_id)
    if run is None:
        raise HTTPException(404, "run not found")
    if not cancelled:
        raise HTTPException(409, f"run is {run.status.value}, cannot cancel")
    return run
