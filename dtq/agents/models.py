import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base, utcnow


class RunStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    AWAITING_APPROVAL = "awaiting_approval"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BUDGET_EXCEEDED = "budget_exceeded"


class StepKind(str, enum.Enum):
    LLM = "llm"
    TOOL = "tool"
    APPROVAL = "approval"


class StepStatus(str, enum.Enum):
    PENDING = "pending"  # scheduled, no attempt has begun
    STARTED = "started"  # an attempt has begun; for a tool, the call may have been made
    WAITING = "waiting"  # an approval step waiting for a person
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN_OUTCOME = "unknown_outcome"  # a side-effecting call that may or may not have happened


def _enum(cls: type[enum.Enum]) -> Enum:
    return Enum(cls, values_callable=lambda e: [m.value for m in e])


class AgentRun(Base):
    """Mirrors sql/schema.sql, which is the source of truth for the table."""

    __tablename__ = "agent_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    goal: Mapped[str] = mapped_column(Text)
    status: Mapped[RunStatus] = mapped_column(_enum(RunStatus), default=RunStatus.QUEUED)
    model: Mapped[dict] = mapped_column(JSON)
    tools: Mapped[list] = mapped_column(JSON)
    queue: Mapped[str] = mapped_column(String(64))
    step_max_retries: Mapped[int] = mapped_column(Integer, default=3)
    # Budgets. None means no limit; max_steps counts LLM calls.
    max_steps: Mapped[int] = mapped_column(Integer)
    max_tokens: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    max_cost_micros: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    max_wall_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Usage totals. Cost is in millionths of a US dollar, so totals don't drift.
    llm_calls: Mapped[int] = mapped_column(Integer, default=0)
    input_tokens: Mapped[int] = mapped_column(BigInteger, default=0)
    output_tokens: Mapped[int] = mapped_column(BigInteger, default=0)
    cost_micros: Mapped[int] = mapped_column(BigInteger, default=0)
    # The current turn's LLM step, its tool calls still without a result, and the next
    # unused step number. Step 0 is created with the run.
    turn: Mapped[int] = mapped_column(Integer, default=0)
    calls_remaining: Mapped[int] = mapped_column(Integer, default=0)
    next_step_no: Mapped[int] = mapped_column(Integer, default=1)
    # Time spent awaiting approval, which doesn't count toward max_wall_seconds.
    paused_seconds: Mapped[int] = mapped_column(Integer, default=0)
    paused_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class AgentStep(Base):
    """Mirrors sql/schema.sql. Append-only: a row is created when its step is
    scheduled, and its outcome is written once."""

    __tablename__ = "agent_steps"

    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("agent_runs.id"), primary_key=True)
    step_no: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    # The LLM step this step belongs to (its own step_no for an LLM step).
    turn: Mapped[int] = mapped_column(Integer)
    kind: Mapped[StepKind] = mapped_column(_enum(StepKind))
    status: Mapped[StepStatus] = mapped_column(_enum(StepStatus))
    # The DTQ attempt that owns the step: its fencing token.
    attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)
    call_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    input: Mapped[dict] = mapped_column(JSON)
    output: Mapped[Any] = mapped_column(JSON, nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_micros: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
