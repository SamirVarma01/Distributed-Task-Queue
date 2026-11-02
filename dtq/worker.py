import logging
import os
import random
import signal
import socket
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from sqlalchemy import select, update

from . import config, registry, tasks  # noqa: F401  (tasks import registers them)
from .agents import demo, executor  # noqa: F401  (register agent_step and the demo tools)
from .broker import Broker
from .db import SessionLocal, Task, TaskStatus, utcnow, wait_for_db
from .runner import TaskFailed, TaskProcess, json_safe

log = logging.getLogger("dtq.worker")


class Heartbeat(threading.Thread):
    """Keeps every in-flight message's idle time low so other workers don't reclaim it.
    One per worker, started when its first message is added; it touches each message in
    the table every HEARTBEAT_SECONDS."""

    def __init__(self, broker: Broker):
        super().__init__(daemon=True, name="heartbeat")
        self.broker = broker
        self._in_flight: dict[str, tuple[str, str]] = {}  # message id -> (stream, consumer)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def add(self, consumer: str, stream: str, msg_id: str) -> None:
        with self._lock:
            self._in_flight[msg_id] = (stream, consumer)
            if not self.is_alive() and not self._stop_event.is_set():
                self.start()

    def remove(self, msg_id: str) -> None:
        """Stop touching a message. Once this returns, no touch of it is in progress or
        will start, so the message can be acked."""
        with self._lock:
            self._in_flight.pop(msg_id, None)

    def run(self):
        while not self._stop_event.wait(config.HEARTBEAT_SECONDS):
            with self._lock:
                messages = list(self._in_flight.items())
            for msg_id, (stream, consumer) in messages:
                with self._lock:  # held per message, so remove() waits for a touch in progress
                    if msg_id not in self._in_flight:
                        continue
                    try:
                        if not self.broker.touch(consumer, stream, msg_id):
                            log.warning("message %s was reclaimed by another worker; no longer touching it", msg_id)
                            del self._in_flight[msg_id]
                    except Exception:
                        log.exception("heartbeat failed for %s", msg_id)

    def stop(self):
        self._stop_event.set()
        if self.is_alive():
            self.join()


class Worker:
    """Runs up to `concurrency` tasks at once, each in a slot thread. One dispatcher
    (run()'s loop) takes a message only while a slot is free and hands it straight to
    that slot, so the worker never holds a message nobody is working on. See
    docs/worker-concurrency-design.md."""

    def __init__(self, concurrency: int | None = None):
        self.id = f"{socket.gethostname()}-{os.getpid()}"
        self.broker = Broker()
        self.queues = config.QUEUES
        self.concurrency = config.WORKER_CONCURRENCY if concurrency is None else concurrency
        if self.concurrency < 1:
            raise ValueError(f"concurrency must be at least 1, not {self.concurrency}")
        self.stopping = False
        self.heartbeat = Heartbeat(self.broker)
        self._slots = threading.BoundedSemaphore(self.concurrency)
        self._pool = ThreadPoolExecutor(self.concurrency, thread_name_prefix="slot")
        self._local = threading.local()
        self._task_processes: list[TaskProcess] = []
        self._lock = threading.Lock()
        self._last_reclaim = 0.0
        self._reclaiming = False
        self._last_reconcile = 0.0

    @property
    def task_process(self) -> TaskProcess:
        """The calling slot's child process for tasks with a timeout. Each slot thread
        has its own, started the first time that slot runs such a task."""
        process = getattr(self._local, "task_process", None)
        if process is None:
            process = self._local.task_process = TaskProcess()
            with self._lock:
                self._task_processes.append(process)
        return process

    def run(self):
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        wait_for_db()
        for q in self.queues:
            self.broker.ensure_group(q)
        log.info("worker %s consuming %s with %d slot(s)", self.id, self.queues, self.concurrency)

        try:
            while not self.stopping:
                if not self._slots.acquire(timeout=config.READ_BLOCK_MS / 1000):
                    continue  # every slot is busy
                try:
                    msg = self._next(block_ms=config.READ_BLOCK_MS)
                except Exception:
                    self._slots.release()
                    # MySQL or Redis is unavailable. Back off and keep going.
                    log.exception("worker loop failed; retrying in %.0fs", config.ERROR_BACKOFF_SECONDS)
                    time.sleep(config.ERROR_BACKOFF_SECONDS)
                    continue
                if msg is None:
                    self._slots.release()
                else:
                    self._pool.submit(self._work, msg)
        finally:
            self._pool.shutdown(wait=True)  # graceful shutdown: let the in-flight tasks finish
            self.close()
        log.info("worker %s stopped", self.id)

    def _work(self, msg: tuple) -> None:
        """A slot runs one message, then frees itself."""
        try:
            self.handle(*msg)
        except Exception:
            # MySQL or Redis failed mid-task. The message stays unacked and is reclaimed
            # once the outage is over; back off before this slot takes another.
            log.exception("slot failed; retrying in %.0fs", config.ERROR_BACKOFF_SECONDS)
            time.sleep(config.ERROR_BACKOFF_SECONDS)
        finally:
            self._slots.release()

    def close(self) -> None:
        """Stop the heartbeat and every slot's child process."""
        self.heartbeat.stop()
        with self._lock:
            processes = list(self._task_processes)
        for process in processes:
            process.stop()

    def step(self, block_ms: int = 0) -> bool:
        """Take the next message, as run()'s loop does, and run it in the calling
        thread, waiting up to block_ms for one if the queues are empty. Returns False if
        there was none. Tests drive workers with this instead of run()."""
        msg = self._next(block_ms)
        if msg is None:
            return False
        self.handle(*msg)
        return True

    def _next(self, block_ms: int) -> tuple | None:
        """Promote due retries, re-send lost tasks, and return the next message to work
        on: a stale one reclaimed from a dead worker, or else a new one, waiting up to
        block_ms. Called only when a slot is free: the message is now this worker's."""
        self.broker.promote_due()
        if time.monotonic() - self._last_reconcile > config.RECONCILE_INTERVAL_SECONDS:
            self._reconcile()
        if not self._reclaiming and time.monotonic() - self._last_reclaim > config.RECLAIM_INTERVAL_SECONDS:
            self._reclaiming, self._last_reclaim = True, time.monotonic()
        if self._reclaiming:  # a pass takes stale messages one per call until none are left
            for q in self.queues:
                msg = self.broker.reclaim(self.id, q, config.VISIBILITY_TIMEOUT_MS)
                if msg is not None:
                    log.warning("reclaimed %s from a dead worker", msg[2].get("task_id"))
                    return (*msg, True)
            self._reclaiming = False
        msg = self.broker.read(self.id, self.queues, block_ms)
        return None if msg is None else (*msg, False)

    def _request_stop(self, *_):
        log.info("shutdown requested; finishing in-flight tasks")
        self.stopping = True

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

        self.heartbeat.add(self.id, stream, msg_id)
        try:
            self._execute(task)
        finally:
            self.heartbeat.remove(msg_id)
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

        timeout = registry.timeout(task.name)
        if timeout is not None:
            # Outside the try below: a child process that can't start is the worker's
            # problem, not the task's. The exception leaves the message pending, and it
            # is reclaimed as after a crash.
            self.task_process.wait_ready()

        log.info("running %s %s (attempt %d)", task.name, task.id, task.attempts)
        try:
            result = self._call(fn, task, timeout)
        except TaskFailed as exc:  # in the child process: raised, timed out, or died
            self._on_failure(task, str(exc))
        except Exception:
            self._on_failure(task, traceback.format_exc())
        else:
            self._finish(task, TaskStatus.SUCCEEDED, result=json_safe(result))

    def _call(self, fn, task: Task, timeout: float | None):
        context = registry.TaskContext(task.id, task.attempts)
        if timeout is not None:
            return self.task_process.call(fn, task.payload, context, timeout)
        token = registry.current_task.set(context)
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
