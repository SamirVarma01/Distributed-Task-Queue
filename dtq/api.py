from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any

import redis
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, update

from . import config, registry, tasks  # noqa: F401  (tasks import registers them)
from .broker import Broker
from .db import SessionLocal, Task, TaskStatus, utcnow, wait_for_db

broker = Broker()


@asynccontextmanager
async def lifespan(_: FastAPI):
    wait_for_db()
    yield


app = FastAPI(title="DTQ", lifespan=lifespan)


class SubmitTask(BaseModel):
    name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = "default"
    max_retries: int = Field(3, ge=0, le=20)
    delay_seconds: float = Field(0, ge=0)


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    queue: str
    payload: dict[str, Any]
    status: TaskStatus
    attempts: int
    max_retries: int
    result: Any = None
    error: str | None = None
    worker_id: str | None = None
    created_at: datetime
    run_at: datetime | None = None
    enqueued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@app.post("/tasks", response_model=TaskOut, status_code=202)
def submit_task(body: SubmitTask):
    fn = registry.get(body.name)
    if fn is None:
        raise HTTPException(400, f"unknown task {body.name!r}; available: {registry.names()}")
    if body.queue not in config.QUEUES:
        raise HTTPException(400, f"unknown queue {body.queue!r}; available: {config.QUEUES}")
    try:
        registry.check_payload(fn, body.payload)
    except TypeError as exc:
        raise HTTPException(400, f"bad payload for {body.name!r}: {exc}")

    # MySQL first: a worker can only pick up a task whose row already exists. If we
    # crash before enqueued_at is set, the reconciler re-sends the task.
    with SessionLocal() as s:
        run_at = utcnow() + timedelta(seconds=body.delay_seconds) if body.delay_seconds else None
        task = Task(name=body.name, queue=body.queue, payload=body.payload,
                    max_retries=body.max_retries, run_at=run_at)
        s.add(task)
        s.commit()

        try:
            broker.send(task.queue, task.id, task.run_at)
        except redis.RedisError as exc:
            task.status, task.error, task.finished_at = TaskStatus.DEAD, f"enqueue failed: {exc}", utcnow()
            s.commit()
            raise HTTPException(503, "broker unavailable")
        task.enqueued_at = utcnow()
        s.commit()
    return task


@app.get("/tasks/{task_id}", response_model=TaskOut)
def get_task(task_id: str):
    with SessionLocal() as s:
        task = s.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    return task


@app.get("/tasks", response_model=list[TaskOut])
def list_tasks(
    status: TaskStatus | None = None,
    queue: str | None = None,
    limit: int = Query(50, ge=1, le=500),
):
    stmt = select(Task).order_by(Task.created_at.desc()).limit(limit)
    if status:
        stmt = stmt.where(Task.status == status)
    if queue:
        stmt = stmt.where(Task.queue == queue)
    with SessionLocal() as s:
        return s.scalars(stmt).all()


@app.post("/tasks/{task_id}/cancel", response_model=TaskOut)
def cancel_task(task_id: str):
    """Cancel a task that has not started. Its queued message is skipped when read."""
    with SessionLocal() as s:
        res = s.execute(
            update(Task)
            .where(Task.id == task_id, Task.status.in_([TaskStatus.QUEUED, TaskStatus.RETRYING]))
            .values(status=TaskStatus.CANCELLED, finished_at=utcnow())
        )
        s.commit()
        task = s.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    if res.rowcount == 0:
        raise HTTPException(409, f"task is {task.status.value}, cannot cancel")
    return task


@app.get("/stats")
def stats():
    with SessionLocal() as s:
        counts = dict(s.execute(select(Task.status, func.count()).group_by(Task.status)).all())
    return {
        "tasks": {st.value: counts.get(st, 0) for st in TaskStatus},
        "broker": broker.stats(config.QUEUES),
    }


@app.get("/health")
def health():
    with SessionLocal() as s:
        s.execute(select(1))
    broker.ping()
    return {"ok": True}
