import enum
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, Enum, Integer, String, Text, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from . import config

log = logging.getLogger(__name__)


class TaskStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    DEAD = "dead"
    CANCELLED = "cancelled"


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class Task(Base):
    """Mirrors sql/schema.sql, which is the source of truth for the table."""

    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(128))
    queue: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, values_callable=lambda e: [m.value for m in e]),
        default=TaskStatus.QUEUED,
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    # When a delayed task should first run; NULL means as soon as possible.
    run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Set once the task's message is in Redis. Queued rows left NULL are re-sent by the
    # reconciler (the API crashed between the INSERT and the XADD).
    enqueued_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


# pool_reset_on_return=None: by default the pool sends a ROLLBACK every time a connection
# is returned, even right after a COMMIT. Every connection here is used through a
# Session or Connection, which already ends its transaction (commit, or rollback on
# close or error) before returning it, so that ROLLBACK is a wasted round trip.
engine = create_engine(config.MYSQL_URL, pool_pre_ping=True, pool_recycle=1800, pool_reset_on_return=None)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


def wait_for_db(timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except OperationalError:
            if time.monotonic() > deadline:
                raise
            log.info("waiting for MySQL...")
            time.sleep(2)
