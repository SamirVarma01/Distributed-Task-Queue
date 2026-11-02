"""Chaos test for agent runs: kill workers while scripted agents run, then count lost
runs and duplicated side effects.

    python scripts/agent_chaos.py --runs 500 --workers 5 --kill-interval 5

Resets the Compose stack (docker compose down -v, which deletes its data) with W workers
and starts N runs of the chaos-agent script (dtq/agents/demo.py). Each run is four
simulated 0.3 s LLM turns that make a calculator call, an echo call and two
send_notification calls. send_notification is given --notification-ms of simulated
latency, half before and half after its write, so kills can land between a call's
intent and its effect, or between the effect and recording it. Every K seconds a random
worker container is killed with SIGKILL and started again, as in scripts/chaos.py.

A run that pauses on an unknown outcome is resolved as a person would resolve it: look
the notification up in the sink, approve with its result if it was sent, and approve a
retry if it wasn't. (After a SIGKILL there is no stalled attempt that could still send
it.)

When every run has finished, or --drain-timeout passes, it reports:
  lost                runs that never finished. Should be 0.
  duplicate effects   notifications sent more than once for one call: rows in the sink
                      beyond the first for an idempotency key. Should be 0.
  unknown outcomes    calls interrupted between their intent and their effect, and how
                      the operator resolved them.
  settled by check    calls interrupted after their effect, which the tool's check found.
  re-executed steps   steps that more than one attempt began: wasted work.
  resume latency      for each step a killed worker held, the time from the kill until
                      another worker began it again.

Results go to bench/results/agent-chaos-<label>.json.
"""

import argparse
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx
import redis
from sqlalchemy import bindparam, create_engine, text

import bench
import chaos

SCRIPT = "chaos-agent"
TOOLS = ["calculator", "echo", "send_notification"]
LLM_STEPS_PER_RUN = 4  # chaos-agent's turns
NOTIFICATIONS_PER_RUN = 2
TERMINAL = ("succeeded", "failed", "cancelled", "budget_exceeded")


def parse_args():
    p = argparse.ArgumentParser(description="Kill workers while agents run; count lost runs and duplicated effects.")
    p.add_argument("--runs", type=int, default=500)
    p.add_argument("--workers", type=int, default=5)
    p.add_argument("--kill-interval", type=float, default=5, help="seconds between kills (default 5)")
    p.add_argument("--notification-ms", type=int, default=200,
                   help="simulated send_notification latency, split around its write (default 200)")
    p.add_argument("--drain-timeout", type=float, default=900,
                   help="seconds to wait for every run to finish before counting the rest as lost")
    p.add_argument("--seed", type=int, help="seed for choosing which worker to kill (default: random)")
    p.add_argument("--label", help="default: <runs>r-w<workers>-k<interval>-<timestamp>")
    p.add_argument("--no-reset", action="store_true", help="use the running stack as is")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--mysql-url", default="mysql+pymysql://dtq:dtq@localhost:3306/dtq")
    p.add_argument("--redis-url", default="redis://localhost:6379/0")
    return p.parse_args()


def in_(column: str):
    return bindparam(column, expanding=True)


RUNS = text("SELECT id, status, error FROM agent_runs WHERE id IN :ids").bindparams(in_("ids"))
STEPS = text("SELECT run_id, step_no, kind, status, attempt, input, idempotency_key, started_at FROM agent_steps "
             "WHERE run_id IN :ids").bindparams(in_("ids"))
SINK = text("SELECT idempotency_key, MIN(created_at) AS first, COUNT(*) AS n FROM side_effect_sink "
            "GROUP BY idempotency_key")
STEP_TASKS = text("SELECT id, payload FROM tasks WHERE name = 'agent_step' AND id IN :ids").bindparams(in_("ids"))
WAITING = text("SELECT a.run_id, a.step_no, t.idempotency_key FROM agent_steps a JOIN agent_runs r ON r.id = a.run_id "
               "JOIN agent_steps t ON t.run_id = a.run_id "
               "AND t.step_no = CAST(JSON_UNQUOTE(JSON_EXTRACT(a.input, '$.step_no')) AS SIGNED) "
               "WHERE r.status = 'awaiting_approval' AND a.kind = 'approval' AND a.status = 'waiting'")
SENT = text("SELECT id FROM side_effect_sink WHERE idempotency_key = :key ORDER BY id LIMIT 1")


def epoch(dt: datetime | None) -> float | None:
    """A naive UTC datetime from MySQL as Unix time, to compare with Redis's clock."""
    return None if dt is None else dt.replace(tzinfo=timezone.utc).timestamp()


def start_runs(api: str, n: int, concurrency: int = 8) -> list[str]:
    body = {"model": {"provider": "scripted", "name": SCRIPT}, "tools": TOOLS, "max_steps": 10}
    with httpx.Client(base_url=api, timeout=60) as client:
        def start(i: int) -> str:
            resp = client.post("/runs", json={"goal": f"chaos run {i}", **body})
            resp.raise_for_status()
            return resp.json()["id"]

        with ThreadPoolExecutor(concurrency) as pool:
            return list(pool.map(start, range(n)))


class Operator:
    """Resolves paused runs as a person would: if the interrupted notification is in the
    sink, approve with its result; if not, approve a retry."""

    def __init__(self, api: str, engine):
        self.client, self.engine = httpx.Client(base_url=api, timeout=30), engine
        self.decisions: list[dict] = []

    def resolve_waiting(self) -> None:
        with self.engine.connect() as conn:
            waiting = conn.execute(WAITING).all()
            sent = {row.idempotency_key: conn.execute(SENT, {"key": row.idempotency_key}).scalar() for row in waiting}
        for row in waiting:
            entry_id = sent[row.idempotency_key]
            body = {"step_no": row.step_no, "note": "agent_chaos operator"}
            if entry_id is not None:
                body["tool_output"] = {"sent": True, "id": entry_id}
            resp = self.client.post(f"/runs/{row.run_id}/approve", json=body)
            if resp.status_code == 200:
                self.decisions.append({"run_id": row.run_id, "step_no": row.step_no,
                                       "resolution": "sent" if entry_id is not None else "retried"})


def wait_for_runs(engine, ids: list[str], operator: Operator, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        operator.resolve_waiting()
        with engine.connect() as conn:
            statuses = [row.status for chunk in bench.chunks(ids) for row in conn.execute(RUNS, {"ids": chunk})]
        done = sum(1 for s in statuses if s in TERMINAL)
        print(f"\r  {done}/{len(ids)} runs finished", end="", flush=True)
        if done == len(ids) or time.monotonic() > deadline:
            print()
            return
        time.sleep(1)


def fetch(engine, ids: list[str], kills: list[dict]) -> tuple:
    with engine.connect() as conn:
        runs = {r.id: (r.status, r.error) for chunk in bench.chunks(ids) for r in conn.execute(RUNS, {"ids": chunk})}
        steps = [{"run_id": s.run_id, "step_no": s.step_no, "kind": s.kind, "status": s.status, "attempt": s.attempt,
                  "tool": (json.loads(s.input) if isinstance(s.input, str) else s.input).get("tool"),
                  "idempotency_key": s.idempotency_key, "started_at": epoch(s.started_at)}
                 for chunk in bench.chunks(ids) for s in conn.execute(STEPS, {"ids": chunk})]
        sink = {row.idempotency_key: (epoch(row.first), row.n) for row in conn.execute(SINK)}
        held = sorted({task_id for kill in kills for task_id in kill["in_flight"]})
        task_steps = {}
        for chunk in bench.chunks(held):
            for row in conn.execute(STEP_TASKS, {"ids": chunk}):
                payload = json.loads(row.payload) if isinstance(row.payload, str) else row.payload
                task_steps[row.id] = (payload["run_id"], payload["step_no"])
    return runs, steps, sink, task_steps


def analyze(runs: dict[str, tuple[str, str | None]], steps: list[dict], sink: dict[str, tuple[float, int]],
            task_steps: dict[str, tuple[str, int]], kills: list[dict], decisions: list[dict]) -> dict:
    """runs: run id -> (status, error). steps: agent_steps rows, started_at as Unix time.
    sink: idempotency key -> (Unix time of its first row, rows). task_steps: step task id
    -> (run id, step no). kills: chaos.Killer.kills, with Redis-clock times."""
    counts: dict[str, int] = {}
    for status, _ in runs.values():
        counts[status] = counts.get(status, 0) + 1
    lost = sorted(i for i, (status, _) in runs.items() if status not in TERMINAL)
    llm_steps: dict[str, int] = {}
    for s in steps:
        if s["kind"] == "llm":
            llm_steps[s["run_id"]] = llm_steps.get(s["run_id"], 0) + 1

    sends = [s for s in steps if s["tool"] == "send_notification"]
    # A step taken over from an earlier attempt never calls the tool itself, so a sent
    # notification that predates the step's (latest) start was found by the check.
    settled_by_check = sum(1 for s in sends if s["status"] == "succeeded" and s["idempotency_key"] in sink
                           and s["started_at"] is not None and sink[s["idempotency_key"]][0] < s["started_at"])
    succeeded_runs = {i for i, (status, _) in runs.items() if status == "succeeded"}
    keys_by_run: dict[str, set] = {}
    for s in sends:
        if s["idempotency_key"] in sink:
            keys_by_run.setdefault(s["run_id"], set()).add(s["idempotency_key"])

    by_step = {(s["run_id"], s["step_no"]): s for s in steps}
    resume, already_finished, not_found = [], 0, 0
    for kill in kills:
        for task_id in kill["in_flight"]:
            step = by_step.get(task_steps.get(task_id, ("", -1)))
            if step is None:
                not_found += 1
            elif step["started_at"] is not None and step["started_at"] > kill["time"]:
                resume.append(step["started_at"] - kill["time"])
            else:
                already_finished += 1  # its outcome was recorded before the kill

    rerun = [s for s in steps if (s["attempt"] or 0) > 1]
    resolutions = [d["resolution"] for d in decisions]
    return {
        "runs": {
            "started": len(runs),
            **{status: counts.get(status, 0) for status in TERMINAL},
            "lost": len(lost),
            "lost_ids": lost[:20],
            "errors": sorted({error for status, error in runs.values() if status != "succeeded" and error})[:10],
            "with_extra_llm_steps": sum(1 for n in llm_steps.values() if n > LLM_STEPS_PER_RUN),
        },
        "side_effects": {
            "notifications_sent": sum(n for _, n in sink.values()),
            "calls_sent": len(sink),
            "duplicate_effects": sum(n - 1 for _, n in sink.values() if n > 1),
            "succeeded_runs_missing_a_notification": sum(
                1 for i in succeeded_runs if len(keys_by_run.get(i, ())) < NOTIFICATIONS_PER_RUN),
        },
        "unknown_outcomes": {
            "count": sum(1 for s in steps if s["status"] == "unknown_outcome"),
            "resolved_as_sent": resolutions.count("sent"),
            "resolved_by_retry": resolutions.count("retried"),
        },
        "settled_by_check": settled_by_check,
        "re_executed_steps": {
            "llm": sum(1 for s in rerun if s["kind"] == "llm"),
            "tool": sum(1 for s in rerun if s["kind"] == "tool"),
            "extra_attempts": sum(s["attempt"] - 1 for s in rerun),
            "steps_total": len(steps),
        },
        "kills": len(kills),
        "resume_seconds": chaos.seconds_stats(resume) | {"held_steps_already_finished": already_finished,
                                                         "held_tasks_not_found": not_found},
    }


def main():
    args = parse_args()
    started = datetime.now(timezone.utc)
    label = args.label or f"{args.runs}r-w{args.workers}-k{args.kill_interval:g}-{started:%Y%m%d-%H%M%S}"
    seed = args.seed if args.seed is not None else random.randrange(2**32)
    os.environ["DEMO_NOTIFICATION_LATENCY_MS"] = str(args.notification_ms)  # read by docker compose up

    if not args.no_reset:
        print(f"resetting stack with {args.workers} workers")
        chaos.sh("docker", "compose", "down", "-v", "--remove-orphans", "--timeout", "5")
        chaos.sh("docker", "compose", "up", "-d", "--build", "--wait", "--scale", f"worker={args.workers}")
    engine = create_engine(args.mysql_url)
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    print(f"starting {args.runs} {SCRIPT} runs")
    t0 = time.monotonic()
    ids = start_runs(args.api, args.runs)
    killer = chaos.Killer(args.kill_interval, args.workers, r, random.Random(seed))
    operator = Operator(args.api, engine)
    killer.start()
    wait_for_runs(engine, ids, operator, args.drain_timeout)
    killer.stop()
    duration = time.monotonic() - t0
    chaos.start_workers(args.workers)

    result = {
        "label": f"agent-chaos-{label}",
        "date": started.isoformat(timespec="seconds"),
        "settings": {"runs": args.runs, "workers": args.workers, "kill_interval_seconds": args.kill_interval,
                     "notification_ms": args.notification_ms, "drain_timeout_seconds": args.drain_timeout,
                     "seed": seed, "script": SCRIPT},
        "environment": bench.environment(),
        "duration_seconds": round(duration, 1),
        **analyze(*fetch(engine, ids, killer.kills), killer.kills, operator.decisions),
        "kill_log": killer.kills,
        "operator_decisions": operator.decisions,
    }
    bench.RESULTS.mkdir(parents=True, exist_ok=True)
    out = bench.RESULTS / f"{result['label']}.json"
    out.write_text(json.dumps(result, indent=2) + "\n")

    runs, fx, unk, rerun = result["runs"], result["side_effects"], result["unknown_outcomes"], result["re_executed_steps"]
    print(f"{result['kills']} kills: {runs['succeeded']}/{runs['started']} runs succeeded, {runs['lost']} lost, "
          f"{fx['duplicate_effects']} duplicate side effects ({fx['notifications_sent']} notifications sent), "
          f"in {duration:.0f}s")
    print(f"{unk['count']} unknown outcomes ({unk['resolved_as_sent']} found sent, {unk['resolved_by_retry']} retried), "
          f"{result['settled_by_check']} calls settled by check, {rerun['llm'] + rerun['tool']} steps re-executed "
          f"({rerun['llm']} LLM, {rerun['tool']} tool) of {rerun['steps_total']}")
    rs = result["resume_seconds"]
    if rs["count"]:
        print(f"resume after a kill: p50 {rs['p50']}s, p95 {rs['p95']}s, max {rs['max']}s over {rs['count']} steps")
    print(f"wrote {out.relative_to(bench.ROOT)}")
    if runs["lost"] or fx["duplicate_effects"]:
        sys.exit(f"{runs['lost']} runs lost, {fx['duplicate_effects']} duplicate side effects")


if __name__ == "__main__":
    main()
