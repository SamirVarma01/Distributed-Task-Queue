"""Redis side of the queue.

Each queue is a Redis Stream consumed by one consumer group, so every message goes to
exactly one worker and stays in the group's pending list until acked. Messages from
crashed workers are reclaimed with XAUTOCLAIM. Delayed work (retries, scheduled tasks)
waits in a sorted set scored by run-at time until it is promoted back into its stream.
"""

import time

import redis

from . import config

STREAM_PREFIX = "dtq:stream:"
DELAYED_KEY = "dtq:delayed"

# Atomically move due members of the delayed set into their streams, so any number of
# workers can run the promoter without double-enqueueing. Members are "<queue>|<task_id>".
_PROMOTE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  local sep = string.find(member, '|', 1, true)
  redis.call('XADD', ARGV[3] .. string.sub(member, 1, sep - 1), '*',
             'task_id', string.sub(member, sep + 1))
end
return #due
"""


def stream_key(queue: str) -> str:
    return f"{STREAM_PREFIX}{queue}"


class Broker:
    def __init__(self, url: str = config.REDIS_URL):
        self.r = redis.Redis.from_url(url, decode_responses=True)
        self._promote = self.r.register_script(_PROMOTE_LUA)

    def ping(self) -> bool:
        return self.r.ping()

    def ensure_group(self, queue: str) -> None:
        try:
            self.r.xgroup_create(stream_key(queue), config.CONSUMER_GROUP, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def enqueue(self, queue: str, task_id: str) -> None:
        self.r.xadd(stream_key(queue), {"task_id": task_id})

    def schedule(self, queue: str, task_id: str, run_at: float) -> None:
        self.r.zadd(DELAYED_KEY, {f"{queue}|{task_id}": run_at})

    def promote_due(self, limit: int = 100) -> int:
        return self._promote(keys=[DELAYED_KEY], args=[time.time(), limit, STREAM_PREFIX])

    def read(self, consumer: str, queues: list[str], block_ms: int):
        """Return new (stream, message_id, fields) tuples, at most one per queue, in
        priority order. Every returned message is now owned by this consumer and must
        be handled, or it sits pending until reclaimed."""
        resp = self.r.xreadgroup(
            config.CONSUMER_GROUP, consumer, {stream_key(q): ">" for q in queues},
            count=1, block=block_ms,
        )
        priority = {stream_key(q): i for i, q in enumerate(queues)}
        messages = [(stream, msg_id, fields) for stream, msgs in resp or [] for msg_id, fields in msgs]
        return sorted(messages, key=lambda m: priority[m[0]])

    def reclaim(self, consumer: str, queue: str, min_idle_ms: int, count: int = 10):
        """Take over messages another consumer has held unacked for too long."""
        _, messages, _ = self.r.xautoclaim(
            stream_key(queue), config.CONSUMER_GROUP, consumer, min_idle_ms, "0-0", count=count,
        )
        return [(stream_key(queue), msg_id, fields) for msg_id, fields in messages if fields]

    def touch(self, consumer: str, stream: str, msg_id: str) -> None:
        """Reset a pending message's idle time so it is not reclaimed (heartbeat)."""
        self.r.xclaim(stream, config.CONSUMER_GROUP, consumer, 0, [msg_id], justid=True)

    def ack(self, stream: str, msg_id: str) -> None:
        pipe = self.r.pipeline()
        pipe.xack(stream, config.CONSUMER_GROUP, msg_id)
        pipe.xdel(stream, msg_id)
        pipe.execute()

    def stats(self, queues: list[str]) -> dict:
        out = {"delayed": self.r.zcard(DELAYED_KEY), "queues": {}}
        for q in queues:
            key = stream_key(q)
            try:
                pending = self.r.xpending(key, config.CONSUMER_GROUP)["pending"]
            except redis.ResponseError:  # stream or group not created yet
                pending = 0
            out["queues"][q] = {"waiting": max(self.r.xlen(key) - pending, 0), "in_flight": pending}
        return out
