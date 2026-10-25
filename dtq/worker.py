import json
import logging
import os
import random
import signal
import socket
import threading
import time
import traceback

from sqlalchemy import update

from . import config, registry, tasks  # noqa: F401  (tasks import registers them)
from .broker import Broker
from .db import TERMINAL_STATUSES, SessionLocal, Task, TaskStatus, utcnow, wait_for_db

log = logging.getLogger("dtq.worker")


class Heartbeat(threading.Thread):
    """Keeps an in-flight message's idle time low so other workers don't reclaim it."""

    def __init__(self, broker: Broker, consumer: str, stream: str, msg_id: str):
        super().__init__(daemon=True)
        self.broker, self.consumer, self.stream, self.msg_id = broker, consumer, stream, msg_id
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.wait(config.HEARTBEAT_SECONDS):
            try:
                self.broker.touch(self.consumer, self.stream, self.msg_id)
            except Exception:
                log.exception("heartbeat failed for %s", self.msg_id)

    def stop(self):
        self._stop_event.set()
        self.join()


class Worker:
    def __init__(self):
        self.id = f"{socket.gethostname()}-{os.getpid()}"
        self.broker = Broker()
        self.queues = config.QUEUES
        self.stopping = False
        self._last_reclaim = 0.0

    def run(self):
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        wait_for_db()
        for q in self.queues:
            self.broker.ensure_group(q)
        log.info("worker %s consuming %s", self.id, self.queues)

        while not self.stopping:
            self.broker.promote_due()
            if time.monotonic() - self._last_reclaim > config.RECLAIM_INTERVAL_SECONDS:
                self._reclaim_stale()
            for msg in self.broker.read(self.id, self.queues, block_ms=1000):
                self.handle(*msg)
        log.info("worker %s stopped", self.id)

    def _request_stop(self, *_):
        log.info("shutdown requested; finishing current task")
        self.stopping = True

    def _reclaim_stale(self):
        self._last_reclaim = time.monotonic()
        for q in self.queues:
            for stream, msg_id, fields in self.broker.reclaim(self.id, q, config.VISIBILITY_TIMEOUT_MS):
                log.warning("reclaimed %s from a dead worker", fields.get("task_id"))
                self.handle(stream, msg_id, fields)
                if self.stopping:
                    return

    def handle(self, stream: str, msg_id: str, fields: dict):
        task = self._claim(fields.get("task_id"))
        if task is None:  # already finished, cancelled, or unknown: drop the message
            self.broker.ack(stream, msg_id)
            return

        heartbeat = Heartbeat(self.broker, self.id, stream, msg_id)
        heartbeat.start()
        try:
            self._execute(task)
        finally:
            heartbeat.stop()
            self.broker.ack(stream, msg_id)

    def _claim(self, task_id: str | None) -> Task | None:
        """Mark the task running. The conditional update makes duplicate deliveries harmless."""
        if not task_id:
            return None
        with SessionLocal() as s:
            res = s.execute(
                update(Task)
                .where(Task.id == task_id, Task.status.not_in(TERMINAL_STATUSES))
                .values(status=TaskStatus.RUNNING, attempts=Task.attempts + 1,
                        worker_id=self.id, started_at=utcnow())
            )
            s.commit()
            return s.get(Task, task_id) if res.rowcount else None

    def _execute(self, task: Task):
        fn = registry.get(task.name)
        if fn is None:
            return self._finish(task, TaskStatus.DEAD, error=f"no task registered as {task.name!r}")
        # Attempts also count runs lost to worker crashes, so a task that keeps killing
        # its worker eventually goes dead instead of looping forever.
        if task.attempts > task.max_retries + 1:
            return self._finish(task, TaskStatus.DEAD, error="exceeded max attempts (worker crashes)")

        log.info("running %s %s (attempt %d)", task.name, task.id, task.attempts)
        try:
            result = fn(**task.payload)
        except Exception:
            self._on_failure(task, traceback.format_exc())
        else:
            try:
                json.dumps(result)
            except TypeError:
                result = repr(result)
            self._finish(task, TaskStatus.SUCCEEDED, result=result)

    def _on_failure(self, task: Task, error: str):
        if task.attempts > task.max_retries:
            log.error("task %s is dead after %d attempts", task.id, task.attempts)
            return self._finish(task, TaskStatus.DEAD, error=error)

        delay = min(config.RETRY_BACKOFF_BASE_SECONDS * 2 ** (task.attempts - 1),
                    config.RETRY_BACKOFF_MAX_SECONDS)
        delay *= random.uniform(0.8, 1.2)  # jitter so retries don't stampede
        with SessionLocal() as s:
            s.execute(update(Task).where(Task.id == task.id)
                      .values(status=TaskStatus.RETRYING, error=error))
            s.commit()
        self.broker.schedule(task.queue, task.id, time.time() + delay)
        log.warning("task %s failed; retrying in %.1fs", task.id, delay)

    def _finish(self, task: Task, status: TaskStatus, result=None, error: str | None = None):
        with SessionLocal() as s:
            s.execute(update(Task).where(Task.id == task.id)
                      .values(status=status, result=result, error=error, finished_at=utcnow()))
            s.commit()
        log.info("task %s %s", task.id, status.value)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    Worker().run()
