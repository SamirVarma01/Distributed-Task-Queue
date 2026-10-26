"""Profile a burst benchmark to find what limits throughput:

    python scripts/profile_bench.py --workers 10 --tasks 30000

Resets the Compose stack (docker compose down -v, which deletes its data) at the given
worker count, runs a burst of noop tasks through bench.py's functions, and records
while the tasks run:
  - docker stats: CPU % per container, sampled about once a second
  - MySQL: per-statement counts and latency (performance_schema digests), counter
    deltas from SHOW GLOBAL STATUS, SHOW ENGINE INNODB STATUS every 10s, and a slow
    log of statements over 100 ms
  - Redis: per-command call counts and time (INFO commandstats) and its CPU time
Everything is written to bench/profiles/<label>/ and summarized in summary.json.
"""

import argparse
import csv
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import redis
from sqlalchemy import create_engine, text

import bench

PROFILES = bench.ROOT / "bench" / "profiles"
STATUS_COUNTERS = [
    "Questions", "Com_select", "Com_insert", "Com_update", "Com_commit", "Com_rollback",
    "Innodb_os_log_fsyncs", "Innodb_data_fsyncs", "Innodb_log_waits", "Innodb_row_lock_waits",
    "Innodb_row_lock_time", "Innodb_buffer_pool_wait_free", "Innodb_rows_updated", "Innodb_rows_inserted",
    "Bytes_received", "Bytes_sent", "Connections",
]
VARIABLES = [
    "innodb_flush_log_at_trx_commit", "sync_binlog", "log_bin", "innodb_buffer_pool_size",
    "innodb_redo_log_capacity", "innodb_io_capacity", "max_connections",
]


def parse_args():
    p = argparse.ArgumentParser(description="Profile a DTQ burst benchmark.")
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--tasks", type=int, default=30000)
    p.add_argument("--label", help="default: profile-w<workers>-<timestamp>")
    p.add_argument("--no-reset", action="store_true", help="use the running stack as is")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--mysql-root-url", default="mysql+pymysql://root:root@localhost:3306/dtq")
    p.add_argument("--redis-url", default="redis://localhost:6379/0")
    return p.parse_args()


def sh(*cmd: str) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=bench.ROOT).stdout


def reset_stack(workers: int) -> None:
    sh("docker", "compose", "down", "-v", "--remove-orphans", "--timeout", "5")
    sh("docker", "compose", "up", "-d", "--wait", "--scale", f"worker={workers}")


class Sampler(threading.Thread):
    """Collects docker stats continuously and InnoDB status every 10s until stopped."""

    def __init__(self, engine):
        super().__init__(daemon=True)
        self.engine, self.stop_event = engine, threading.Event()
        self.cpu_rows: list[dict] = []
        self.innodb: list[tuple[float, str]] = []
        self.t0 = time.monotonic()

    def run(self):
        last_innodb = -10.0
        while not self.stop_event.is_set():
            now = time.monotonic() - self.t0
            if now - last_innodb >= 10:
                with self.engine.connect() as conn:
                    self.innodb.append((now, conn.execute(text("SHOW ENGINE INNODB STATUS")).one()[2]))
                last_innodb = now
            out = sh("docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}")
            t = round(time.monotonic() - self.t0, 2)
            for line in out.splitlines():
                name, cpu = line.split("\t")
                self.cpu_rows.append({"t": t, "container": name, "cpu_percent": float(cpu.rstrip("%"))})

    def stop(self):
        self.stop_event.set()
        self.join()


def global_status(conn) -> dict[str, int]:
    rows = conn.execute(text("SHOW GLOBAL STATUS")).all()
    return {k: int(v) for k, v in rows if k in STATUS_COUNTERS}


def main():
    args = parse_args()
    label = args.label or f"profile-w{args.workers}-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    out_dir = PROFILES / label
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.no_reset:
        print(f"resetting stack with {args.workers} workers")
        reset_stack(args.workers)
    engine = create_engine(args.mysql_root_url)
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    spec = {"name": "noop", "payload": {}}

    print("warming up")
    bench.wait_until_terminal(engine, bench.submit(args.api, spec, 500, 500, 8), 600)

    with engine.connect() as conn:
        variables = {k: v for k, v in conn.execute(text("SHOW GLOBAL VARIABLES")).all() if k in VARIABLES}
        conn.execute(text("SET GLOBAL slow_query_log = ON, long_query_time = 0.1, log_output = 'TABLE'"))
        conn.execute(text("TRUNCATE mysql.slow_log"))
        conn.execute(text("TRUNCATE performance_schema.events_statements_summary_by_digest"))
        before = global_status(conn)
    r.config_resetstat()
    redis_cpu_before = r.info("cpu")

    sampler = Sampler(engine)
    sampler.start()
    print(f"submitting {args.tasks} noop tasks")
    t_submit = time.monotonic() - sampler.t0
    ids = bench.submit(args.api, spec, args.tasks, 500, 8)
    bench.wait_until_terminal(engine, ids, 1800)
    t_done = time.monotonic() - sampler.t0
    sampler.stop()

    with engine.connect() as conn:
        after = global_status(conn)
        digests = conn.execute(text(
            "SELECT DIGEST_TEXT, COUNT_STAR, SUM_TIMER_WAIT / 1e9 AS total_ms, AVG_TIMER_WAIT / 1e9 AS avg_ms, "
            "MAX_TIMER_WAIT / 1e9 AS max_ms, SUM_LOCK_TIME / 1e9 AS lock_ms, SUM_ROWS_AFFECTED "
            "FROM performance_schema.events_statements_summary_by_digest "
            "WHERE SCHEMA_NAME = 'dtq' ORDER BY SUM_TIMER_WAIT DESC")).mappings().all()
        slow = conn.execute(text(
            "SELECT start_time, query_time, lock_time, rows_examined, LEFT(sql_text, 200) AS sql_text "
            "FROM mysql.slow_log WHERE db = 'dtq' ORDER BY start_time")).mappings().all()
        conn.execute(text("SET GLOBAL slow_query_log = OFF"))
    redis_cpu_after = r.info("cpu")
    commandstats = r.info("commandstats")
    metrics = bench.measure(engine, ids)
    n = len(ids)

    # docker stats over the measured window only
    window = [row for row in sampler.cpu_rows if t_submit <= row["t"] <= t_done]
    by_container: dict[str, list[float]] = {}
    for row in window:
        by_container.setdefault(row["container"], []).append(row["cpu_percent"])
    cpu_mean = {c: round(sum(v) / len(v), 1) for c, v in sorted(by_container.items())}
    sample_times = sorted({row["t"] for row in window})
    total_per_sample = [sum(row["cpu_percent"] for row in window if row["t"] == t) for t in sample_times]

    delta = {k: after[k] - before[k] for k in STATUS_COUNTERS}
    elapsed = t_done - t_submit
    summary = {
        "label": label,
        "workers": args.workers,
        "tasks": n,
        "environment": bench.environment(),
        "throughput_per_second": metrics["throughput_per_second"],
        "run_ms": metrics["run_ms"],
        "measured_window_seconds": round(elapsed, 1),
        "docker_stats_samples": len(sample_times),
        "cpu_percent_mean_by_container": cpu_mean,
        "cpu_percent_total": {
            "mean": round(sum(total_per_sample) / len(total_per_sample), 1) if total_per_sample else None,
            "max": round(max(total_per_sample), 1) if total_per_sample else None,
            "capacity": (bench.environment()["docker_cpus"] or 0) * 100,
        },
        "mysql_variables": variables,
        "mysql_status_delta": delta,
        "mysql_per_task": {k: round(v / n, 2) for k, v in delta.items() if k.startswith(("Com_", "Questions", "Innodb_os_log_fsyncs", "Innodb_data_fsyncs"))},
        "mysql_per_second": {k: round(v / elapsed, 1) for k, v in delta.items() if k.startswith(("Com_commit", "Innodb_os_log_fsyncs", "Questions"))},
        "mysql_statements_over_100ms": len(slow),
        "redis_cpu_seconds": round(
            (redis_cpu_after["used_cpu_sys"] + redis_cpu_after["used_cpu_user"])
            - (redis_cpu_before["used_cpu_sys"] + redis_cpu_before["used_cpu_user"]), 2),
        "redis_calls_per_task": {k.removeprefix("cmdstat_"): round(v["calls"] / n, 2)
                                 for k, v in sorted(commandstats.items(), key=lambda kv: -kv[1]["calls"])},
    }

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    with (out_dir / "docker_stats.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, ["t", "container", "cpu_percent"])
        w.writeheader()
        w.writerows(sampler.cpu_rows)
    with (out_dir / "mysql_digests.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["digest_text", "count", "per_task", "total_ms", "avg_ms", "max_ms", "lock_ms", "rows_affected"])
        for d in digests:
            w.writerow([d["DIGEST_TEXT"], d["COUNT_STAR"], round(d["COUNT_STAR"] / n, 2), round(d["total_ms"], 1),
                        round(d["avg_ms"], 3), round(d["max_ms"], 1), round(d["lock_ms"], 1), d["SUM_ROWS_AFFECTED"]])
    with (out_dir / "mysql_slow_log.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["start_time", "query_time", "lock_time", "rows_examined", "sql_text"])
        for s in slow:
            w.writerow([s["start_time"], s["query_time"], s["lock_time"], s["rows_examined"],
                        s["sql_text"].decode() if isinstance(s["sql_text"], bytes) else s["sql_text"]])
    (out_dir / "innodb_status.txt").write_text(
        "\n\n".join(f"===== t={t:.0f}s\n{status}" for t, status in sampler.innodb))
    (out_dir / "redis_commandstats.json").write_text(json.dumps(commandstats, indent=2) + "\n")

    print(json.dumps({k: summary[k] for k in (
        "throughput_per_second", "run_ms", "cpu_percent_mean_by_container", "cpu_percent_total",
        "mysql_per_task", "mysql_per_second", "mysql_statements_over_100ms", "redis_cpu_seconds",
        "redis_calls_per_task")}, indent=2))
    print(f"wrote {out_dir.relative_to(bench.ROOT)}")


if __name__ == "__main__":
    main()
