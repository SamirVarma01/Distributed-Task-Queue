"""The run state machine (docs/agent-runtime-design.md, section 3).

Every status change goes through transition(): a conditional UPDATE that moves the run
only from the states the caller expects, the same way DTQ claims tasks. A move the
table doesn't allow is a bug and raises; a run that has already moved on (a lost race,
such as a cancel against an approval) is normal and just returns False."""

from collections.abc import Iterable

from sqlalchemy import update
from sqlalchemy.orm import Session

from ..db import utcnow
from .models import AgentRun, RunStatus

S = RunStatus

# For each status, the statuses a run may move to it from. Runs are created queued.
ALLOWED_FROM: dict[RunStatus, frozenset[RunStatus]] = {
    S.QUEUED: frozenset(),
    S.RUNNING: frozenset({S.QUEUED, S.AWAITING_APPROVAL}),
    S.AWAITING_APPROVAL: frozenset({S.RUNNING}),
    S.SUCCEEDED: frozenset({S.RUNNING}),
    S.FAILED: frozenset({S.RUNNING, S.AWAITING_APPROVAL}),
    S.CANCELLED: frozenset({S.QUEUED, S.RUNNING, S.AWAITING_APPROVAL}),
    S.BUDGET_EXCEEDED: frozenset({S.RUNNING}),
}

TERMINAL = frozenset({S.SUCCEEDED, S.FAILED, S.CANCELLED, S.BUDGET_EXCEEDED})


class InvalidTransition(ValueError):
    pass


def check(src: RunStatus, dst: RunStatus) -> None:
    if src not in ALLOWED_FROM[dst]:
        raise InvalidTransition(f"a run cannot move from {src.value} to {dst.value}")


def transition(session: Session, run_id: str, src: RunStatus | Iterable[RunStatus],
               dst: RunStatus, **values) -> bool:
    """Move the run to dst if its status is src (or one of them), in the caller's
    transaction, also setting values. Moving to a terminal status sets finished_at.
    Returns False if the run isn't in src or doesn't exist."""
    sources = {src} if isinstance(src, RunStatus) else set(src)
    for s in sources:
        check(s, dst)
    if dst in TERMINAL:
        values.setdefault("finished_at", utcnow())
    res = session.execute(
        update(AgentRun)
        .where(AgentRun.id == run_id, AgentRun.status.in_(sources))
        .values(status=dst, **values)
    )
    return res.rowcount == 1
