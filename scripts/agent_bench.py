"""Benchmark the agent runtime's own overhead, with the scripted LLM and the tools taking
no time.

    python scripts/agent_bench.py --workers 1 5 10 --repeats 3

For each worker count and repeat, resets the Compose stack (docker compose down -v,
which deletes its data) and measures:

  sequential  --sequential-runs runs of bench-chain (llm, tool, llm, tool, llm), one at a
              time. With nothing else queued and the LLM and tool instant, a step's time
              from being created to its outcome being recorded is the runtime's overhead:
              sending its message, a worker picking it up, DTQ's claim, phases A, B and C
              (three transactions), and DTQ recording the task.
  burst       --runs runs of bench-agent (a turn of two parallel tool calls, then one
              call, then an answer: 6 steps) submitted at once. Throughput is runs and
              steps finished per second, from the first run's creation to the last run's
              end.

Each result goes to bench/results/agent-bench-<label>-w<W>-rep<i>.json, and the medians
across repeats are printed. Refuses to run on battery power unless ALLOW_BATTERY=1.
"""

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx
from sqlalchemy import bindparam, create_engine, text

import bench
import chaos

TERMINAL = ("succeeded", "failed", "cancelled", "budget_exceeded")
RUN_TIMES = text("SELECT id, status, created_at, finished_at FROM agent_runs WHERE id IN :ids").bindparams(
    bindparam("ids", expanding=True))
STEP_TIMES = text("SELECT created_at, finished_at FROM agent_steps WHERE run_id IN :ids").bindparams(
    bindparam("ids", expanding=True))


def parse_args():
    p = argparse.ArgumentParser(description="Measure the agent runtime's per-step overhead and throughput.")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 5, 10])
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--runs", type=int, default=500, help="runs in the burst (default 500)")
    p.add_argument("--sequential-runs", type=int, default=30, help="runs one at a time (default 30)")
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--label", help="default: <timestamp>")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--mysql-url", default="mysql+pymysql://dtq:dtq@localhost:3306/dtq")
    return p.parse_args()


def run_body(script: str, i: int) -> dict:
    return {"goal": f"{script} {i}", "model": {"provider": "scripted", "name": script}, "tools": ["echo"],
            "max_steps": 10}


def wait_for(engine, ids: list[str], timeout: float) -> list:
    deadline = time.monotonic() + timeout
    while True:
        with engine.connect() as conn:
            rows = [row for chunk in bench.chunks(ids) for row in conn.execute(RUN_TIMES, {"ids": chunk})]
        if sum(1 for row in rows if row.status in TERMINAL) == len(ids):
            return rows
        if time.monotonic() > deadline:
            sys.exit(f"timed out with {len(ids) - sum(1 for r in rows if r.status in TERMINAL)} runs unfinished")
        time.sleep(0.05)


def step_seconds(engine, ids: list[str]) -> list[float]:
    """Each step's time from being created to its outcome being recorded."""
    with engine.connect() as conn:
        return [(s.finished_at - s.created_at).total_seconds()
                for chunk in bench.chunks(ids) for s in conn.execute(STEP_TIMES, {"ids": chunk})]


def sequential(client: httpx.Client, engine, n: int, timeout: float) -> dict:
    ids = []
    for i in range(n):
        ids.append(client.post("/runs", json=run_body("bench-chain", i)).raise_for_status().json()["id"])
        wait_for(engine, ids[-1:], timeout)
    runs = wait_for(engine, ids, timeout)
    steps = step_seconds(engine, ids)
    return {"runs": n, "succeeded": sum(1 for r in runs if r.status == "succeeded"), "steps": len(steps),
            "step_overhead_ms": bench.ms_stats(steps),
            "run_ms": bench.ms_stats([(r.finished_at - r.created_at).total_seconds() for r in runs])}


def burst(client: httpx.Client, engine, n: int, timeout: float) -> dict:
    with ThreadPoolExecutor(8) as pool:
        ids = list(pool.map(lambda i: client.post("/runs", json=run_body("bench-agent", i))
                            .raise_for_status().json()["id"], range(n)))
    runs = wait_for(engine, ids, timeout)
    steps = step_seconds(engine, ids)
    span = (max(r.finished_at for r in runs) - min(r.created_at for r in runs)).total_seconds()
    return {"runs": n, "succeeded": sum(1 for r in runs if r.status == "succeeded"), "steps": len(steps),
            "seconds": round(span, 2), "runs_per_second": round(n / span, 1),
            "steps_per_second": round(len(steps) / span, 1), "step_latency_ms": bench.ms_stats(steps)}


def main():
    args = parse_args()
    power = bench.power_source()
    if power and power.startswith("Battery") and not os.getenv("ALLOW_BATTERY"):
        sys.exit("On battery power. Plug in for comparable results, or set ALLOW_BATTERY=1.")
    started = datetime.now(timezone.utc)
    label = args.label or f"{started:%Y%m%d-%H%M%S}"
    engine = create_engine(args.mysql_url, pool_pre_ping=True)
    medians: dict[int, list[dict]] = {}

    for workers in args.workers:
        for rep in range(1, args.repeats + 1):
            print(f"workers={workers} repeat {rep}: resetting stack")
            chaos.sh("docker", "compose", "down", "-v", "--remove-orphans", "--timeout", "5")
            chaos.sh("docker", "compose", "up", "-d", "--build", "--wait", "--scale", f"worker={workers}")
            engine.dispose()  # its pooled connections were to the MySQL container that was just removed
            with httpx.Client(base_url=args.api, timeout=60) as client:
                seq = sequential(client, engine, args.sequential_runs, args.timeout)
                bur = burst(client, engine, args.runs, args.timeout)
            result = {"label": f"agent-bench-{label}-w{workers}-rep{rep}",
                      "date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                      "settings": {"workers": workers, "runs": args.runs, "sequential_runs": args.sequential_runs},
                      "environment": bench.environment(), "sequential": seq, "burst": bur}
            bench.RESULTS.mkdir(parents=True, exist_ok=True)
            (bench.RESULTS / f"{result['label']}.json").write_text(json.dumps(result, indent=2) + "\n")
            print(f"  step overhead p50 {seq['step_overhead_ms']['p50']} ms, p95 {seq['step_overhead_ms']['p95']} ms; "
                  f"burst {bur['runs_per_second']} runs/s, {bur['steps_per_second']} steps/s")
            medians.setdefault(workers, []).append(result)

    print("\nmedians across repeats")
    print("| workers | step overhead p50 / p95 (ms) | runs/s | steps/s | step latency in burst p50 / p95 (ms) |")
    print("|---|---|---|---|---|")
    for workers, results in medians.items():
        med = lambda f: statistics.median(f(r) for r in results)  # noqa: E731
        print(f"| {workers} | {med(lambda r: r['sequential']['step_overhead_ms']['p50']):g} / "
              f"{med(lambda r: r['sequential']['step_overhead_ms']['p95']):g} | "
              f"{med(lambda r: r['burst']['runs_per_second']):g} | {med(lambda r: r['burst']['steps_per_second']):g} | "
              f"{med(lambda r: r['burst']['step_latency_ms']['p50']):g} / "
              f"{med(lambda r: r['burst']['step_latency_ms']['p95']):g} |")


if __name__ == "__main__":
    main()
