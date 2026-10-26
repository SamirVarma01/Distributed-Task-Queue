"""Benchmark DTQ end to end. With the stack running (docker compose up -d):

    python scripts/bench.py --tasks 10000 --task noop --label my-run

Submits the tasks through POST /tasks/batch, with several batches in flight so the
client is not the bottleneck, and waits until every one reaches a terminal state. Then
it reads their timestamps from MySQL and writes bench/results/<label>.json and a row in
bench/results/summary.md.

This is a burst benchmark: all tasks are submitted as fast as possible, so latency and
queue wait include the time a task spends in the backlog behind the others.

Definitions (all timestamps come from the tasks table):
  wall time     last finished_at - first created_at
  throughput    tasks / wall time
  submit time   last created_at - first created_at
  latency       finished_at - created_at, per task
  queue wait    started_at - created_at, per task (started_at is the last attempt's)
  run time      finished_at - started_at, per task
Percentiles use the nearest-rank method.
"""

import argparse
import json
import math
import platform
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import httpx
from sqlalchemy import bindparam, create_engine, text

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
SUMMARY_HEADER = (
    "| label | task | workers | tasks | wall (s) | throughput (tasks/s) "
    "| latency p50 / p95 / p99 (ms) | queue wait p50 / p95 (ms) | run p50 (ms) | not succeeded | commit |\n"
    "|---|---|---|---|---|---|---|---|---|---|---|\n"
)

COUNT_TERMINAL = text(
    "SELECT COUNT(*) FROM tasks WHERE status IN ('succeeded', 'dead', 'cancelled') AND id IN :ids"
).bindparams(bindparam("ids", expanding=True))
SELECT_TIMES = text(
    "SELECT status, created_at, started_at, finished_at FROM tasks WHERE id IN :ids"
).bindparams(bindparam("ids", expanding=True))


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark DTQ throughput and latency.")
    p.add_argument("--tasks", type=int, default=10000, help="number of measured tasks (default 10000)")
    p.add_argument("--task", choices=["noop", "sleep_ms"], default="noop")
    p.add_argument("--sleep-ms", type=int, default=10, help="duration of each sleep_ms task (default 10)")
    p.add_argument("--label", help="result name (default: <task>-<workers>w-<timestamp>)")
    p.add_argument("--workers", type=int, help="worker count to record (default: counted with docker compose ps)")
    p.add_argument("--warmup", type=int, default=200, help="unmeasured tasks run first (default 200)")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--concurrency", type=int, default=8, help="batches in flight at once")
    p.add_argument("--timeout", type=float, default=1800, help="seconds to wait for tasks to finish")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--mysql-url", default="mysql+pymysql://dtq:dtq@localhost:3306/dtq")
    return p.parse_args()


def run(cmd: list[str]) -> str | None:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=ROOT).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def count_workers() -> int | None:
    out = run(["docker", "compose", "ps", "--status", "running", "--format", "{{.Service}}"])
    return None if out is None else out.split().count("worker")


def environment() -> dict:
    cpu = run(["sysctl", "-n", "machdep.cpu.brand_string"])
    if cpu is None and Path("/proc/cpuinfo").exists():
        cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                    if line.startswith("model name")), None)
    docker = run(["docker", "info", "--format", "{{.NCPU}}|{{.MemTotal}}|{{.ServerVersion}}"])
    ncpu, mem, version = docker.split("|") if docker else (None, None, None)
    return {
        "commit": run(["git", "rev-parse", "--short", "HEAD"]),
        "uncommitted_changes": bool(run(["git", "status", "--porcelain", "--untracked-files=no"])),
        "host": platform.platform(),
        "cpu": cpu,
        "docker_cpus": int(ncpu) if ncpu else None,
        "docker_mem_gib": round(int(mem) / 2**30, 1) if mem else None,
        "docker_version": version,
    }


def submit(api: str, spec: dict, n: int, batch_size: int, concurrency: int) -> list[str]:
    sizes = [min(batch_size, n - i) for i in range(0, n, batch_size)]
    with httpx.Client(base_url=api, timeout=60) as client:
        def post(size: int) -> list[str]:
            resp = client.post("/tasks/batch", json={"tasks": [spec] * size})
            resp.raise_for_status()
            return resp.json()["ids"]

        with ThreadPoolExecutor(concurrency) as pool:
            return [task_id for ids in pool.map(post, sizes) for task_id in ids]


def chunks(ids: list[str], size: int = 1000):
    for i in range(0, len(ids), size):
        yield ids[i:i + size]


def wait_until_terminal(engine, ids: list[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        with engine.connect() as conn:
            done = sum(conn.execute(COUNT_TERMINAL, {"ids": c}).scalar_one() for c in chunks(ids))
        print(f"\r  {done}/{len(ids)} finished", end="", flush=True)
        if done == len(ids):
            print()
            return
        if time.monotonic() > deadline:
            print()
            sys.exit(f"timed out after {timeout:.0f}s with {len(ids) - done} tasks unfinished")
        time.sleep(0.5)


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile of an ascending list."""
    return sorted_values[max(math.ceil(p / 100 * len(sorted_values)) - 1, 0)]


def ms_stats(seconds: list[float], ps=(50, 95, 99)) -> dict:
    values = sorted(s * 1000 for s in seconds)
    out = {f"p{p}": round(percentile(values, p), 1) for p in ps}
    out["max"] = round(values[-1], 1)
    return out


def measure(engine, ids: list[str]) -> dict:
    with engine.connect() as conn:
        rows = [r for c in chunks(ids) for r in conn.execute(SELECT_TIMES, {"ids": c}).all()]
    statuses: dict[str, int] = {}
    for r in rows:
        statuses[r.status] = statuses.get(r.status, 0) + 1
    ran = [r for r in rows if r.started_at and r.finished_at]

    first_created = min(r.created_at for r in rows)
    wall = (max(r.finished_at for r in rows) - first_created).total_seconds()
    return {
        "statuses": statuses,
        "wall_seconds": round(wall, 3),
        "throughput_per_second": round(len(rows) / wall, 1),
        "submit_seconds": round((max(r.created_at for r in rows) - first_created).total_seconds(), 3),
        "latency_ms": ms_stats([(r.finished_at - r.created_at).total_seconds() for r in ran]),
        "queue_wait_ms": ms_stats([(r.started_at - r.created_at).total_seconds() for r in ran]),
        "run_ms": ms_stats([(r.finished_at - r.started_at).total_seconds() for r in ran]),
    }


def summary_row(result: dict) -> str:
    m, lat, wait = result["metrics"], result["metrics"]["latency_ms"], result["metrics"]["queue_wait_ms"]
    not_succeeded = result["tasks"] - m["statuses"].get("succeeded", 0)
    commit = result["environment"]["commit"] or "?"
    if result["environment"]["uncommitted_changes"]:
        commit += "+changes"
    return (
        f"| {result['label']} | {result['task']} | {result['workers']} | {result['tasks']} "
        f"| {m['wall_seconds']:.1f} | {m['throughput_per_second']:.0f} "
        f"| {lat['p50']:.0f} / {lat['p95']:.0f} / {lat['p99']:.0f} | {wait['p50']:.0f} / {wait['p95']:.0f} "
        f"| {m['run_ms']['p50']:.1f} | {not_succeeded} | {commit} |\n"
    )


def main():
    args = parse_args()
    workers = args.workers if args.workers is not None else count_workers()
    started = datetime.now(timezone.utc)
    label = args.label or f"{args.task}-{workers}w-{started:%Y%m%d-%H%M%S}"
    spec = {"name": args.task, "payload": {"ms": args.sleep_ms} if args.task == "sleep_ms" else {}}
    engine = create_engine(args.mysql_url)

    if args.warmup:
        print(f"warming up with {args.warmup} tasks")
        wait_until_terminal(engine, submit(args.api, spec, args.warmup, args.batch_size, args.concurrency),
                            args.timeout)

    print(f"submitting {args.tasks} {args.task} tasks to {workers} workers")
    ids = submit(args.api, spec, args.tasks, args.batch_size, args.concurrency)
    wait_until_terminal(engine, ids, args.timeout)

    result = {
        "label": label,
        "date": started.isoformat(timespec="seconds"),
        "task": args.task,
        "payload": spec["payload"],
        "tasks": args.tasks,
        "workers": workers,
        "settings": {k: getattr(args, k) for k in ("warmup", "batch_size", "concurrency")},
        "environment": environment(),
        "metrics": measure(engine, ids),
    }

    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / f"{label}.json").write_text(json.dumps(result, indent=2) + "\n")
    summary = RESULTS / "summary.md"
    if not summary.exists():
        summary.write_text(SUMMARY_HEADER)
    with summary.open("a") as f:
        f.write(summary_row(result))

    m = result["metrics"]
    print(f"{label}: {m['throughput_per_second']:.0f} tasks/s over {m['wall_seconds']:.1f}s "
          f"(submitting took {m['submit_seconds']:.1f}s), latency p50/p95/p99 "
          f"{m['latency_ms']['p50']:.0f}/{m['latency_ms']['p95']:.0f}/{m['latency_ms']['p99']:.0f} ms, "
          f"statuses {m['statuses']}")
    print(f"wrote {(RESULTS / f'{label}.json').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
