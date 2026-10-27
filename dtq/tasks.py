"""Example tasks. Delivery is at-least-once, so real tasks should be idempotent."""

import random
import socket
import time

import redis

from . import config
from .registry import task


@task()
def add(a: float, b: float) -> float:
    return a + b


@task()
def sleep(seconds: float = 1) -> dict:
    time.sleep(seconds)
    return {"slept": seconds}


@task()
def flaky(fail_rate: float = 0.5) -> str:
    """Fails randomly; useful for watching retries and the dead state."""
    if random.random() < fail_rate:
        raise RuntimeError("random failure")
    return "ok"


@task()
def noop() -> None:
    """Does nothing; benchmarks measure the queue's own overhead with it."""


@task()
def sleep_ms(ms: int = 10) -> None:
    """Simulates a short I/O-bound task for benchmarks."""
    time.sleep(ms / 1000)


# Appends "<redis server time> <worker hostname>" to the task's list. Using Redis's clock
# lets scripts/chaos.py compare execution times with its kill times, which also use it.
_RECORD_LUA = """
local t = redis.call('TIME')
return redis.call('RPUSH', KEYS[1], t[1] .. '.' .. string.format('%06d', tonumber(t[2])) .. ' ' .. ARGV[1])
"""
_record = None


@task()
def record_execution(key: str, ms: int = 0) -> None:
    """Chaos-test task (scripts/chaos.py). Records this execution in the Redis list
    dtq:chaos:<key>, then works for ms milliseconds. Each task gets its own key, so a
    list longer than 1 means the task's function ran more than once."""
    global _record
    if _record is None:
        _record = redis.Redis.from_url(config.REDIS_URL, decode_responses=True).register_script(_RECORD_LUA)
    _record(keys=[f"dtq:chaos:{key}"], args=[socket.gethostname()])
    time.sleep(ms / 1000)
