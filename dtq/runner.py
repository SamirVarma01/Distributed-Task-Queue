"""Runs tasks that have a timeout in a child process, so one that overruns can be killed.

Python can't stop a thread from outside, so a task that must be stoppable runs in a
separate process. Each worker keeps one child and reuses it for every task with a
timeout; tasks without one still run in the worker process itself. A child that overruns
is SIGKILLed and replaced.

Children are forked from a forkserver: a clean process that imports the worker's tasks
once (about 1.5 s, most of it the LLM SDKs), so replacing a killed child is a fork, not
another import. Forking the worker itself would copy its heartbeat thread's locks and its
open MySQL and Redis connections; starting each child from scratch ("spawn") would pay
the import every time.

The worker's heartbeat thread keeps refreshing the task's message while the worker waits
for the child's answer, so a long task isn't reclaimed by another worker."""

import json
import logging
import multiprocessing
import pickle
import signal
import traceback
from typing import Any, Callable

from . import registry

log = logging.getLogger("dtq.runner")

# A child imports nothing at startup (the forkserver already has), so this is generous.
STARTUP_TIMEOUT_SECONDS = 60

_context = multiprocessing.get_context("forkserver")
_context.set_forkserver_preload(["dtq.worker"])


class TaskFailed(Exception):
    """The attempt failed in the child: the task raised, overran its timeout, or its
    process died. str(exc) is the error to record."""


def json_safe(result: Any) -> Any:
    """MySQL's JSON type rejects NaN and Infinity, which json.dumps accepts by default;
    such a result could never be saved, so it is stored as its repr instead."""
    try:
        json.dumps(result, allow_nan=False)
    except (TypeError, ValueError):
        return repr(result)
    return result


class TaskProcess:
    """A worker's child process for tasks with a timeout."""

    def __init__(self):
        self._proc = None
        self._conn = None
        self._ready = False

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc else None

    def start(self) -> None:
        """Start a child unless one is running. Doesn't wait for it to be ready."""
        if self._proc is not None and self._proc.is_alive():
            return
        self._discard()
        self._conn, child_end = _context.Pipe()
        self._proc = _context.Process(target=_serve, args=(child_end,), name="dtq-task", daemon=True)
        self._proc.start()
        child_end.close()
        self._ready = False
        log.info("started task process %d", self._proc.pid)

    def wait_ready(self) -> None:
        """Start a child if needed and wait until it can take a task. Raises RuntimeError
        if it can't: then the worker's environment is broken, which is no fault of the
        task's, so the caller mustn't count it as a failed attempt."""
        self.start()
        if self._ready:
            return
        try:
            self._ready = self._conn.poll(STARTUP_TIMEOUT_SECONDS) and self._conn.recv() == "ready"
        except EOFError:
            pass
        if not self._ready:
            code = self._discard()
            raise RuntimeError(f"the task process didn't start (exit code {code}); see the worker's log")

    def call(self, fn: Callable[..., Any], payload: dict, context: registry.TaskContext, timeout: float) -> Any:
        """Run fn(**payload) in the child (wait_ready first) and return its result, or
        raise TaskFailed. A child that overruns the timeout is killed, and a new one
        started straight away so it is ready sooner for the next task."""
        try:
            request = pickle.dumps((fn, payload, context))
        except Exception:
            raise TaskFailed(traceback.format_exc())
        self._conn.send_bytes(request)
        if not self._conn.poll(timeout):
            self._discard()
            self.start()
            raise TaskFailed(f"TimeoutError: exceeded {timeout:g}s")
        try:
            ok, value = self._conn.recv()
        except EOFError:  # it died: os._exit, a signal, the OOM killer...
            code = self._discard()
            self.start()
            raise TaskFailed(f"the task's process exited with code {code}")
        if not ok:
            raise TaskFailed(value)
        return value

    def stop(self) -> None:
        """Close the pipe, which tells an idle child to exit, and wait for it briefly."""
        if self._proc is None:
            return
        self._conn.close()
        self._proc.join(5)
        self._discard()

    def _discard(self) -> int | None:
        """Kill the child if it is still running and forget it. Returns its exit code."""
        if self._proc is None:
            return None
        if self._proc.is_alive():
            self._proc.kill()
        self._proc.join()
        code = self._proc.exitcode
        self._conn.close()
        self._proc.close()
        self._proc = self._conn = None
        return code


def _serve(conn) -> None:
    """The child: run each task it is sent, and reply (True, result) or (False, error)."""
    # Ctrl-C in a terminal reaches the whole process group; the worker decides when to
    # stop, and closes the pipe once its current task is done.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    conn.send("ready")
    while True:
        try:
            request = conn.recv_bytes()
        except EOFError:  # the worker closed the pipe, or died
            return
        try:
            fn, payload, context = pickle.loads(request)
            token = registry.current_task.set(context)
            try:
                reply = (True, json_safe(fn(**payload)))
            finally:
                registry.current_task.reset(token)
        except Exception:
            reply = (False, traceback.format_exc())
        conn.send(reply)
