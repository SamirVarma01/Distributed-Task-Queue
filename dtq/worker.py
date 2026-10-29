import json
import logging
import os
import random
import signal
import socket
import threading
import time
import traceback
from datetime import timedelta

from sqlalchemy import select, update

from . import config, registry, tasks  # noqa: F401  (tasks import registers them)
from .broker import Broker
from .db import SessionLocal, Task, TaskStatus, utcnow, wait_for_db

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
                if not self.broker.touch(self.consumer, self.stream, self.msg_id):
                    log.warning("message %s was reclaimed by another worker; heartbeat stopped", self.msg_id)
                    return
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
        self._last_reconcile = 0.0

    def run(self):
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        wait_for_db()
        for q in self.queues:
            self.broker.ensure_group(q)
        log.info("worker %s consuming %s", self.id, self.queues)

        while not self.stopping:
            try:
                self.step(block_ms=config.READ_BLOCK_MS)
            except Exception:
                # MySQL or Redis is unavailable. Any message we held stays unacked and
                # is reclaimed once the outage is over, so back off and keep going.
                log.exception("worker loop failed; retrying in %.0fs", config.ERROR_BACKOFF_SECONDS)
                time.sleep(config.ERROR_BACKOFF_SECONDS)
        log.info("worker %s stopped", self.id)

    def step(self, block_ms: int = 0) -> bool:
        """One loop iteration: promote due retries, re-send lost tasks, reclaim stale
        messages, and run the next task, waiting up to block_ms for one if the queues
        are empty. Returns False if there was no new task to run."""
        self.broker.promote_due()
        if time.monotonic() - self._last_reconcile > config.RECONCILE_INTERVAL_SECONDS:
            self._reconcile()
        if time.monotonic() - self._last_reclaim > config.RECLAIM_INTERVAL_SECONDS:
            self._reclaim_stale()
        msg = self.broker.read(self.id, self.queues, block_ms)
        if msg is None:
            return False
        self.handle(*msg)
        return True

    def _request_stop(self, *_):
        log.info("shutdown requested; finishing current task")
        self.stopping = True

    def _reclaim_stale(self):
        self._last_reclaim = time.monotonic()
        for q in self.queues:
            while not self.stopping:
                msg = self.broker.reclaim(self.id, q, config.VISIBILITY_TIMEOUT_MS)
                if msg is None:
                    break
                log.warning("reclaimed %s from a dead worker", msg[2].get("task_id"))
                self.handle(*msg, reclaimed=True)

    def _reconcile(self):
        """Re-send queued tasks whose message never reached Redis (the API crashed
        between the INSERT and the XADD). If the message did get there after all, the
        duplicate is harmless: a fresh message cannot claim a task that is already
        running or finished."""
        self._last_reconcile = time.monotonic()
        cutoff = utcnow() - timedelta(seconds=config.RECONCILE_AFTER_SECONDS)
        with SessionLocal() as s:
            lost = s.scalars(
                select(Task)
                .where(Task.status == TaskStatus.QUEUED, Task.enqueued_at.is_(None), Task.created_at < cutoff)
                .limit(100)
            ).all()
            for task in lost:
                log.warning("re-sending task %s, which never reached the broker", task.id)
                self.broker.send(task.queue, task.id, task.run_at)
                task.enqueued_at = utcnow()
                s.commit()

    def handle(self, stream: str, msg_id: str, fields: dict, reclaimed: bool = False):
        task = self._claim(fields.get("task_id"), reclaimed)
        if task is None:  # already running, finished, cancelled, or unknown: drop the message
            self.broker.ack(self.id, stream, msg_id)
            return

        heartbeat = Heartbeat(self.broker, self.id, stream, msg_id)
        heartbeat.start()
        try:
            self._execute(task)
        finally:
            heartbeat.stop()
        # Ack only once the outcome is recorded. If recording it raised (MySQL or Redis
        # down), the message stays pending and is reclaimed after the visibility timeout.
        self.broker.ack(self.id, stream, msg_id)

    def _claim(self, task_id: str | None, reclaimed: bool) -> Task | None:
        """Mark the task running. The conditional update makes duplicate deliveries
        harmless: a fresh message only claims a task that is waiting to run. A reclaimed
        message may also take over a 'running' task, whose worker died mid-run."""
        if not task_id:
            return None
        claimable = [TaskStatus.QUEUED, TaskStatus.RETRYING]
        if reclaimed:
            claimable.append(TaskStatus.RUNNING)
        # Read the claimed row in the claim's own transaction: reading it after the
        # commit would open a second transaction that then has to be rolled back.
        with SessionLocal() as s, s.begin():
            res = s.execute(
                update(Task)
                .where(Task.id == task_id, Task.status.in_(claimable))
                .values(status=TaskStatus.RUNNING, attempts=Task.attempts + 1,
                        worker_id=self.id, started_at=utcnow())
            )
            return s.get(Task, task_id) if res.rowcount else None

    def _execute(self, task: Task):
        fn = registry.get(task.name)
        if fn is None:
            return self._finish(task, TaskStatus.DEAD, error=f"no task registered as {task.name!r}")
        try:
            registry.check_payload(fn, task.payload)
        except TypeError as exc:  # can never succeed, so don't retry
            return self._finish(task, TaskStatus.DEAD, error=f"bad payload: {exc}")
        # Attempts also count runs lost to worker crashes, so a task that keeps killing
        # its worker eventually goes dead instead of looping forever.
        if task.attempts > task.max_retries + 1:
            return self._finish(task, TaskStatus.DEAD, error="exceeded max attempts (worker crashes)")

        log.info("running %s %s (attempt %d)", task.name, task.id, task.attempts)
        try:
            result = self._call(fn, task)
        except Exception:
            self._on_failure(task, traceback.format_exc())
        else:
            # MySQL's JSON type rejects NaN and Infinity, which json.dumps accepts by
            # default; such a result could never be saved, so store its repr instead.
            try:
                json.dumps(result, allow_nan=False)
            except (TypeError, ValueError):
                result = repr(result)
            self._finish(task, TaskStatus.SUCCEEDED, result=result)

    @staticmethod
    def _call(fn, task: Task):
        token = registry.current_task.set(registry.TaskContext(task.id, task.attempts))
        try:
            return fn(**task.payload)
        finally:
            registry.current_task.reset(token)

    def _on_failure(self, task: Task, error: str):
        if task.attempts > task.max_retries:
            log.error("task %s is dead after %d attempts", task.id, task.attempts)
            return self._finish(task, TaskStatus.DEAD, error=error)

        delay = min(config.RETRY_BACKOFF_BASE_SECONDS * 2 ** (task.attempts - 1),
                    config.RETRY_BACKOFF_MAX_SECONDS)
        delay *= random.uniform(0.8, 1.2)  # jitter so retries don't stampede
        if not self._record(task, status=TaskStatus.RETRYING, error=error):
            return
        self.broker.schedule(task.queue, task.id, time.time() + delay)
        log.warning("task %s failed; retrying in %.1fs", task.id, delay)

    def _finish(self, task: Task, status: TaskStatus, result=None, error: str | None = None):
        if self._record(task, status=status, result=result, error=error, finished_at=utcnow()):
            log.info("task %s %s", task.id, status.value)

    def _record(self, task: Task, **values) -> bool:
        """Save this attempt's outcome, fenced on the attempt we claimed. If this worker
        stalled past the visibility timeout, another worker may have reclaimed the task:
        its claim bumped attempts, so this stale write matches no row instead of
        overwriting that worker's progress or result."""
        with SessionLocal() as s:
            res = s.execute(
                update(Task)
                .where(Task.id == task.id, Task.status == TaskStatus.RUNNING, Task.attempts == task.attempts)
                .values(**values)
            )
            hook = registry.on_dead(task.name) if values.get("status") == TaskStatus.DEAD else None
            if res.rowcount and hook:
                hook(s, task, values.get("error"))
            s.commit()
        if not res.rowcount:
            log.warning("task %s attempt %d was taken over by another worker; dropping its outcome",
                        task.id, task.attempts)
        return bool(res.rowcount)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    Worker().run()
