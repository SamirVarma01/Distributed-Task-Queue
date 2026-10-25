"""Example tasks. Delivery is at-least-once, so real tasks should be idempotent."""

import random
import time

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
