# DTQ — Distributed Task Queue

A small distributed task queue: **Redis Streams** move the work, **MySQL** stores each task's status and result, and **Docker Compose** runs the API and any number of workers.

```
client ──POST /tasks──▶ API ──INSERT──▶ MySQL (tasks table: status, attempts, result)
                         │                ▲
                         └──XADD──▶ Redis stream dtq:stream:<queue>
                                          │ XREADGROUP (consumer group "dtq-workers")
                                          ▼
                                  worker × N ──UPDATE──┘
                                          │ on failure: ZADD dtq:delayed (backoff)
                                          └─ promoter moves due items back into streams
```

## Run it

```sh
docker compose up --build              # mysql, redis, api, 3 workers
docker compose up --scale worker=10    # change the worker count
python scripts/demo.py 100             # submit 100 random tasks and watch them finish
```

API docs: http://localhost:8000/docs. The `api` container's healthcheck calls `/health`, which checks MySQL and Redis, so `docker compose ps` shows whether it is healthy.

`sql/schema.sql` is built into the MySQL image and runs only when the MySQL volume is first created. After changing it, reset the data with `docker compose down -v` and start again with `--build`.

```sh
curl -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
     -d '{"name": "add", "payload": {"a": 2, "b": 3}, "queue": "high"}'
curl localhost:8000/tasks/<id>
curl localhost:8000/stats
```

| Endpoint | Purpose |
|---|---|
| `POST /tasks` | Submit `{name, payload, queue, max_retries, delay_seconds}` |
| `POST /tasks/batch` | Submit `{tasks: [...]}`, up to 1000 at once, in one transaction and one Redis round trip; returns `{ids}` |
| `GET /tasks/{id}` | Status, attempts, result or error |
| `GET /tasks?status=dead&queue=low` | List tasks |
| `POST /tasks/{id}/cancel` | Cancel a queued or retrying task |
| `GET /stats` | Task counts by status, plus waiting, in-flight and delayed counts per queue |

## Adding a task

Write a function in `dtq/tasks.py` (or a module that `tasks.py` imports) and decorate it:

```python
@task()
def resize_image(url: str, width: int) -> dict:
    ...
```

The payload's fields are passed to it as keyword arguments. The return value must be JSON-serializable; anything else is stored as its `repr`.

## How it behaves

- **One worker per message.** Each queue is a stream read through a single consumer group, so each message goes to exactly one worker.
- **Crash recovery.** A message stays in the group's pending list until the worker acks it. While a task runs, its worker sends a heartbeat (`XCLAIM ... JUSTID`) every 10s. A message that sits idle longer than `VISIBILITY_TIMEOUT_MS` means its worker died, and another worker takes it over with `XAUTOCLAIM`.
- **Retries.** A failed task waits in the `dtq:delayed` sorted set, using exponential backoff with jitter. A Lua script moves due tasks back into their stream in one atomic step, so every worker can run it without enqueueing a task twice. After `max_retries` the task is marked `dead`, and its traceback is saved in `error`.
- **Poison tasks.** Attempts lost to worker crashes count toward the limit too, so a task that keeps killing its worker ends up `dead` instead of looping forever.
- **Idempotent claims.** A worker starts a task with a conditional `UPDATE ... WHERE status IN ('queued', 'retrying')`, and also allows `'running'` when it reclaimed the message from a dead worker. If a duplicate message arrives, or the task was cancelled, the worker just acks the message and skips it.
- **Lost submissions.** The API writes the MySQL row first and sets `enqueued_at` once the message is in Redis. If the API crashes in between, a worker's reconciler finds `queued` rows with no `enqueued_at` that are older than `RECONCILE_AFTER_SECONDS` (60s) and re-sends them, keeping any delay (`run_at`).
- **Bad payloads.** The API rejects a payload that doesn't fit the task's signature (missing or unexpected arguments) with a 400. If one reaches a worker anyway, the task goes `dead` at once instead of being retried.
- **One message at a time.** A worker reads (and reclaims) a single message and finishes it before taking another. A message it held but wasn't working on would get no heartbeat, so another worker could reclaim it and run it a second time.
- **Ack after the result is saved.** If MySQL or Redis fails while a worker is recording a task's outcome, the message stays pending and is reclaimed once the outage is over. The worker logs the error, backs off (`ERROR_BACKOFF_SECONDS`) and keeps running.
- **Priority.** `QUEUES=high,default,low` sets the order in which workers check the queues: a task in `low` runs only when `high` and `default` are empty. When every queue is empty, a worker waits on all of them with a blocking `XREADGROUP` (up to `READ_BLOCK_MS`, 100 ms), so a new task is picked up as soon as it arrives. That read returns one message per queue that received one. The worker keeps the highest-priority message and immediately hands the others back to the end of their streams, so it still holds only the message it is working on.
- **Graceful shutdown.** On SIGTERM a worker finishes its current task before it exits (`stop_grace_period: 30s`).

**Delivery is at-least-once.** A worker can crash after a task's side effects happen but before it records the result. When that happens the task runs again, so tasks should be idempotent.

## Durability settings

MySQL runs with `--disable-log-bin`. The binary log is used for replication and point-in-time recovery. This stack does neither: there is one MySQL server, and the data is reset freely. Waiting for disk flushes on commit was the main limit on worker throughput (see `bench/profiles/FINDINGS.md`), and keeping the binary log consistent with InnoDB adds a second flush, of the binary log, to each commit. Turning it off doesn't weaken durability: with `innodb_flush_log_at_trx_commit=1` (the default, unchanged), InnoDB's redo log is flushed to disk before each commit returns, so a committed claim or result survives a crash. If you add a replica or need point-in-time recovery, remove the flag.

**Opt-in, not crash-safe:** `MYSQL_FLUSH_LOG_AT_TRX_COMMIT=2 docker compose up` sets `innodb_flush_log_at_trx_commit=2`. MySQL then writes the redo log on each commit but flushes it to disk only about once a second. That removes the per-commit wait, but if the OS or Docker VM crashes (not just MySQL), up to about a second of commits that clients were told succeeded can be lost. For this queue that breaks at-least-once delivery. A worker can save a task's result, ack and delete its Redis message, and then lose the saved result in the crash. The row goes back to `running` or `queued` with no message left to deliver it, so the task never runs again. Use it only for throwaway benchmarks or where losing tasks is acceptable. The default is `1`.

Redis runs with `--appendfsync always`, so each write is flushed to its append-only file before Redis replies. Redis's default, `everysec`, flushes about once a second. If the OS or Docker VM crashed (killing the Redis process alone is not enough), the last second of writes could be lost, including a task's `XADD` after the API had already returned 202 and set `enqueued_at`. Nothing would re-send that task: the reconciler only re-sends rows without `enqueued_at`, and a task stuck in `retrying` or `running` would lose its message the same way. `REDIS_APPENDFSYNC=everysec docker compose up` is an opt-in with the same tradeoff as the MySQL one above: faster, and not crash-safe.

## Tests

The tests run without Docker: fakeredis stands in for Redis and SQLite for MySQL.

```sh
python -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/pytest
```

## Benchmarks

```sh
scripts/run_bench_matrix.sh                    # 10000 noop tasks at 1, 5 and 10 workers
WORKER_COUNTS=10 RATES="430 687" scripts/run_bench_matrix.sh --duration 60   # open-loop latency at fixed rates
python scripts/bench.py --tasks 10000 --task sleep_ms --sleep-ms 10   # one run against the current stack
```

`bench.py` submits the tasks through `POST /tasks/batch`, waits until all of them finish, and computes throughput and latency from the tasks' `created_at`, `started_at` and `finished_at` timestamps in MySQL. Each run writes `bench/results/<label>.json` (with the commit, CPU and Docker resources it ran on), and `bench/results/summary.md` is regenerated from all result files.

There are two modes. A **burst** run (the default) submits every task at once to measure maximum throughput, so its latency includes time spent in the backlog. A **rate** run (`--rate R --duration S`) is open loop: it submits R tasks/s on a fixed schedule whether or not earlier tasks have finished, which shows latency at a given load. It also records how far behind schedule the client fell (`submit_lag_ms`). The metric definitions are at the top of `scripts/bench.py`.

To judge a change, compare commits with `scripts/ab_compare.sh NAME BEFORE AFTER PAIRS profile|bench ...`, which alternates runs of the two commits so that the machine's speed drifting between runs affects both equally. `scripts/bench_report.py` prints medians over repeated runs.

On a laptop, benchmark on mains power with the lid open. Both scripts refuse to start on battery (set `ALLOW_BATTERY=1` to override) and run under `caffeinate` so the machine doesn't idle-sleep, and every result records its power source.

The matrix and A/B scripts run `docker compose down -v` before each run, which **deletes the stack's data**. Both scripts need the dev requirements (`pip install -r requirements-dev.txt`).

## Possible next steps

- Per-task timeouts (run tasks in a subprocess and kill it when time runs out).
- Several tasks running at once in each worker process (a thread or process pool).
- Prometheus metrics and a dashboard.
- Integration tests against real MySQL and Redis (testcontainers), which would also cover `sql/schema.sql`.
