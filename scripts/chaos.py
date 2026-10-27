"""Chaos test: kill workers while tasks run, then count lost and duplicated work.

    python scripts/chaos.py --tasks 10000 --workers 5 --kill-interval 5

Resets the Compose stack (docker compose down -v, which deletes its data) with W
workers and submits N record_execution tasks. Every K seconds it picks a random worker
container, kills it with SIGKILL (docker kill), records the messages that worker was
holding (still pending under its consumer name once the kill has landed), and starts it
again (docker compose up). Each task appends one entry per execution to its own Redis
list, timestamped with Redis's clock, which the kill times also use. When every task has
reached a terminal state, or --drain-timeout passes, it reports:

  lost        tasks still not terminal at the end. At-least-once delivery means this
              should be 0; anything else is a bug.
  duplicates  tasks whose function ran more than once. Expected under at-least-once
              delivery: a worker killed after the function started but before its result
              was saved leaves the task to be run again.
  recovery    for each message a killed worker was holding, the time from the kill until
              its task's next execution began. A message whose task had already finished
              (killed after saving the result but before acking) is never re-run, and is
              counted separately.

Results go to bench/results/chaos-<label>.json.
"""

import argparse
import json
import random
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx
import redis
from sqlalchemy import bindparam, create_engine, text

import bench

sys.path.insert(0, str(bench.ROOT))
from dtq import config  # noqa: E402
from dtq.broker import stream_key  # noqa: E402

STREAM = stream_key("default")  # chaos tasks are submitted to the default queue
TERMINAL = ("succeeded", "dead", "cancelled")
SELECT_STATUS = text("SELECT id, status, attempts FROM tasks WHERE id IN :ids").bindparams(
    bindparam("ids", expanding=True))


def parse_args():
    p = argparse.ArgumentParser(description="Kill workers while tasks run; count lost and duplicated work.")
    p.add_argument("--tasks", type=int, default=10000)
    p.add_argument("--workers", type=int, default=5)
    p.add_argument("--kill-interval", type=float, default=5, help="seconds between kills (default 5)")
    p.add_argument("--task-ms", type=int, default=50, help="work per task after recording it (default 50)")
    p.add_argument("--drain-timeout", type=float, default=600,
                   help="seconds to wait for every task to finish before counting the rest as lost")
    p.add_argument("--seed", type=int, help="seed for choosing which worker to kill (default: random)")
    p.add_argument("--label", help="default: <tasks>t-w<workers>-k<interval>-<timestamp>")
    p.add_argument("--no-reset", action="store_true", help="use the running stack as is")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--mysql-url", default="mysql+pymysql://dtq:dtq@localhost:3306/dtq")
    p.add_argument("--redis-url", default="redis://localhost:6379/0")
    return p.parse_args()


def sh(*cmd: str) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=bench.ROOT).stdout


def start_workers(workers: int) -> None:
    """Start any stopped worker containers (and add missing ones) without touching the rest."""
    sh("docker", "compose", "up", "-d", "--no-build", "--no-recreate", "--scale", f"worker={workers}")


def running_workers() -> list[str]:
    return sh("docker", "compose", "ps", "-q", "--status", "running", "worker").split()


def redis_time(r: redis.Redis) -> float:
    seconds, micros = r.time()
    return seconds + micros / 1e6


def submit(api: str, keys: list[str], ms: int, batch_size: int = 500, concurrency: int = 8) -> list[str]:
    batches = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]
    with httpx.Client(base_url=api, timeout=60) as client:
        def post(batch: list[str]) -> list[str]:
            tasks = [{"name": "record_execution", "payload": {"key": k, "ms": ms}} for k in batch]
            resp = client.post("/tasks/batch", json={"tasks": tasks})
            resp.raise_for_status()
            return resp.json()["ids"]

        with ThreadPoolExecutor(concurrency) as pool:
            return [task_id for ids in pool.map(post, batches) for task_id in ids]


class Killer(threading.Thread):
    """Every interval: pick a running worker, kill it, record what it held, restart it."""

    def __init__(self, interval: float, workers: int, r: redis.Redis, rng: random.Random):
        super().__init__(daemon=True)
        self.interval, self.workers, self.r, self.rng = interval, workers, r, rng
        self.stop_event = threading.Event()
        self.kills: list[dict] = []
        self._seen: set[tuple[str, int]] = set()

    def held_messages(self, container: str) -> list[str]:
        """Task ids of messages pending under the killed worker's consumer name. A worker's
        id (its consumer name) is "<hostname>-<pid>", and a container's hostname is its
        short id, which a restarted container keeps. So a message left pending by an
        earlier kill of the same container is skipped, unless it was delivered again since."""
        task_ids = []
        for consumer in self.r.xinfo_consumers(STREAM, config.CONSUMER_GROUP):
            if not consumer["name"].startswith(f"{container[:12]}-"):
                continue
            for p in self.r.xpending_range(STREAM, config.CONSUMER_GROUP, "-", "+", 100, consumer["name"]):
                seen = (p["message_id"], p["times_delivered"])
                if seen in self._seen:
                    continue
                self._seen.add(seen)
                entry = self.r.xrange(STREAM, p["message_id"], p["message_id"])
                if entry:
                    task_ids.append(entry[0][1]["task_id"])
        return task_ids

    def run(self):
        while not self.stop_event.wait(self.interval):
            containers = running_workers()
            if not containers:
                continue
            container = self.rng.choice(containers)
            sh("docker", "kill", container)
            killed_at = redis_time(self.r)  # after docker kill returns, so the worker is dead
            in_flight = self.held_messages(container)
            start_workers(self.workers)
            self.kills.append({"time": killed_at, "container": container[:12], "in_flight": in_flight})
            print(f"\n  kill {len(self.kills)}: {container[:12]} (holding {len(in_flight)} message(s))", flush=True)

    def stop(self):
        self.stop_event.set()
        self.join()


def poll_statuses(engine, ids: list[str]) -> dict[str, tuple[str, int]]:
    out = {}
    with engine.connect() as conn:
        for chunk in bench.chunks(ids):
            out.update({row.id: (row.status, row.attempts) for row in conn.execute(SELECT_STATUS, {"ids": chunk})})
    return out


def wait_for_drain(engine, ids: list[str], timeout: float) -> dict[str, tuple[str, int]]:
    deadline = time.monotonic() + timeout
    while True:
        statuses = poll_statuses(engine, ids)
        done = sum(1 for status, _ in statuses.values() if status in TERMINAL)
        print(f"\r  {done}/{len(ids)} finished", end="", flush=True)
        if done == len(ids) or time.monotonic() > deadline:
            print()
            return statuses
        time.sleep(1)


def read_executions(r: redis.Redis, keys: list[str]) -> dict[str, list[tuple[float, str]]]:
    out = {}
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        pipe = r.pipeline(transaction=False)
        for k in chunk:
            pipe.lrange(f"dtq:chaos:{k}", 0, -1)
        for k, entries in zip(chunk, pipe.execute()):
            out[k] = [(float(e.split()[0]), e.split()[1]) for e in entries]
    return out


def seconds_stats(values: list[float]) -> dict:
    if not values:
        return {"count": 0}
    values = sorted(values)
    return {"count": len(values), "p50": round(bench.percentile(values, 50), 2),
            "p95": round(bench.percentile(values, 95), 2), "max": round(values[-1], 2)}


def analyze(task_keys: dict[str, str], statuses: dict[str, tuple[str, int]],
            executions: dict[str, list[tuple[float, str]]], kills: list[dict]) -> dict:
    """task_keys maps task id -> execution key; statuses maps task id -> (status, attempts)."""
    counts: dict[str, int] = {}
    for status, _ in statuses.values():
        counts[status] = counts.get(status, 0) + 1
    lost = sorted(i for i in task_keys if statuses.get(i, ("missing", 0))[0] not in TERMINAL)
    runs = {i: len(executions.get(k, [])) for i, k in task_keys.items()}
    duplicated = [i for i, n in runs.items() if n > 1]

    recovery, already_finished = [], 0
    for kill in kills:
        for task_id in kill["in_flight"]:
            later = [t for t, _ in executions.get(task_keys.get(task_id, ""), []) if t > kill["time"]]
            if later:
                recovery.append(min(later) - kill["time"])
            else:
                already_finished += 1

    return {
        "tasks": {
            "submitted": len(task_keys),
            "succeeded": counts.get("succeeded", 0),
            "dead": counts.get("dead", 0),
            "cancelled": counts.get("cancelled", 0),
            "lost": len(lost),
            "lost_ids": lost[:20],
            "ran_more_than_one_attempt": sum(1 for _, attempts in statuses.values() if attempts > 1),
        },
        "executions": {
            "total": sum(runs.values()),
            "tasks_run_more_than_once": len(duplicated),
            "extra_executions": sum(runs[i] - 1 for i in duplicated),
            "max_per_task": max(runs.values(), default=0),
            "succeeded_without_execution": sum(
                1 for i, n in runs.items() if n == 0 and statuses.get(i, ("", 0))[0] == "succeeded"),
        },
        "kills": len(kills),
        "recovery_seconds": seconds_stats(recovery) | {"in_flight_already_finished": already_finished},
    }


def main():
    args = parse_args()
    started = datetime.now(timezone.utc)
    label = args.label or f"{args.tasks}t-w{args.workers}-k{args.kill_interval:g}-{started:%Y%m%d-%H%M%S}"
    seed = args.seed if args.seed is not None else random.randrange(2**32)

    if not args.no_reset:
        print(f"resetting stack with {args.workers} workers")
        sh("docker", "compose", "down", "-v", "--remove-orphans", "--timeout", "5")
        sh("docker", "compose", "up", "-d", "--build", "--wait", "--scale", f"worker={args.workers}")
    engine = create_engine(args.mysql_url)
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    run_id = uuid.uuid4().hex[:8]
    keys = [f"{run_id}:{i}" for i in range(args.tasks)]
    print(f"submitting {args.tasks} record_execution tasks ({args.task_ms} ms each)")
    t0 = time.monotonic()
    ids = submit(args.api, keys, args.task_ms)
    task_keys = dict(zip(ids, keys))

    killer = Killer(args.kill_interval, args.workers, r, random.Random(seed))
    killer.start()
    statuses = wait_for_drain(engine, ids, args.drain_timeout)
    killer.stop()
    duration = time.monotonic() - t0
    start_workers(args.workers)

    result = {
        "label": f"chaos-{label}",
        "date": started.isoformat(timespec="seconds"),
        "settings": {"tasks": args.tasks, "workers": args.workers, "kill_interval_seconds": args.kill_interval,
                     "task_ms": args.task_ms, "drain_timeout_seconds": args.drain_timeout, "seed": seed},
        "environment": bench.environment(),
        "duration_seconds": round(duration, 1),
        **analyze(task_keys, statuses, read_executions(r, keys), killer.kills),
        "kill_log": killer.kills,
    }
    bench.RESULTS.mkdir(parents=True, exist_ok=True)
    out = bench.RESULTS / f"{result['label']}.json"
    out.write_text(json.dumps(result, indent=2) + "\n")

    t, e, rec = result["tasks"], result["executions"], result["recovery_seconds"]
    print(f"{result['kills']} kills, {t['lost']} lost, {e['extra_executions']} duplicate executions "
          f"({e['tasks_run_more_than_once']} tasks ran more than once), {t['dead']} dead, "
          f"{t['succeeded']}/{t['submitted']} succeeded in {duration:.0f}s")
    if rec["count"]:
        print(f"recovery after a kill: p50 {rec['p50']}s, p95 {rec['p95']}s, max {rec['max']}s "
              f"over {rec['count']} in-flight messages")
    print(f"wrote {out.relative_to(bench.ROOT)}")
    if t["lost"]:
        sys.exit(f"{t['lost']} tasks were lost, e.g. {t['lost_ids'][:5]}")


if __name__ == "__main__":
    main()
