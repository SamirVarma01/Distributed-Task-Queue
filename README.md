# DTQ — Distributed Task Queue

[![CI](https://github.com/SamirVarma01/Distributed-Task-Queue/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/SamirVarma01/Distributed-Task-Queue/actions/workflows/ci.yml)

A small distributed task queue: **Redis Streams** move the work, **MySQL** stores each task's status and result, and **Docker Compose** runs the API and any number of workers. On top of it, a [durable runtime for LLM agents](#agent-runtime) runs each LLM and tool call as a checkpointed task.

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

To limit how long each attempt may run, give the task a timeout in seconds:

```python
@task(timeout=30)
def fetch_report(url: str) -> dict:
    ...
```

`DEFAULT_TASK_TIMEOUT_SECONDS` gives a timeout to every task that doesn't set its own. It is unset by default, so those tasks have none, and `timeout=None` exempts a task from it. A task with a timeout runs in a child process (see [Timeouts](#how-it-behaves)), so it must be a module-level function.

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
- **Timeouts.** Python can't stop a thread from outside, so a task with a timeout runs in a child process of the worker. If an attempt overruns, the worker SIGKILLs the child and records the attempt as failed, with the error `TimeoutError: exceeded 30s`. It is retried with backoff and counts toward `max_retries`, like any other failure. The worker's heartbeat keeps running while it waits, so a long task isn't reclaimed. Each worker reuses one child for all such tasks, forked from a forkserver that imported the tasks once, so replacing a killed child is a fork rather than another import. A child that crashes (a segfault, `os._exit`) fails the attempt instead of taking the worker down. Tasks without a timeout still run in the worker process itself. Going through the child costs little: with `DEFAULT_TASK_TIMEOUT_SECONDS=60`, 10,000 `noop` tasks on one worker ran at a median of 290 tasks/s, against 291 without, and the median task's run time went from 1.0 to 1.1 ms. These are 3 alternating pairs from `scripts/ab_compare.sh`, in `bench/results/timeout-w1-*.json`, on an Apple M2 on battery power.
- **Graceful shutdown.** On SIGTERM a worker finishes its current task before it exits (`stop_grace_period: 30s`).

**Delivery is at-least-once.** A worker can crash after a task's side effects happen but before it records the result. When that happens the task runs again, so tasks should be idempotent.

## Durability settings

MySQL runs with `--disable-log-bin`. The binary log is used for replication and point-in-time recovery. This stack does neither: there is one MySQL server, and the data is reset freely. Waiting for disk flushes on commit was the main limit on worker throughput (see `bench/profiles/FINDINGS.md`), and keeping the binary log consistent with InnoDB adds a second flush, of the binary log, to each commit. Turning it off doesn't weaken durability: with `innodb_flush_log_at_trx_commit=1` (the default, unchanged), InnoDB's redo log is flushed to disk before each commit returns, so a committed claim or result survives a crash. If you add a replica or need point-in-time recovery, remove the flag.

**Opt-in, not crash-safe:** `MYSQL_FLUSH_LOG_AT_TRX_COMMIT=2 docker compose up` sets `innodb_flush_log_at_trx_commit=2`. MySQL then writes the redo log on each commit but flushes it to disk only about once a second. That removes the per-commit wait, but if the OS or Docker VM crashes (not just MySQL), up to about a second of commits that clients were told succeeded can be lost. For this queue that breaks at-least-once delivery. A worker can save a task's result, ack and delete its Redis message, and then lose the saved result in the crash. The row goes back to `running` or `queued` with no message left to deliver it, so the task never runs again. Use it only for throwaway benchmarks or where losing tasks is acceptable. The default is `1`.

Redis runs with `--appendfsync always`, so each write is flushed to its append-only file before Redis replies. Redis's default, `everysec`, flushes about once a second. If the OS or Docker VM crashed (killing the Redis process alone is not enough), the last second of writes could be lost, including a task's `XADD` after the API had already returned 202 and set `enqueued_at`. Nothing would re-send that task: the reconciler only re-sends rows without `enqueued_at`, and a task stuck in `retrying` or `running` would lose its message the same way. `REDIS_APPENDFSYNC=everysec docker compose up` is an opt-in with the same tradeoff as the MySQL one above: faster, and not crash-safe.

## Tests

The unit tests in `tests/` run without Docker: fakeredis stands in for Redis and SQLite for MySQL. The integration tests in `tests_integration/` run against real MySQL and Redis, which testcontainers starts in Docker. MySQL is built from `sql/Dockerfile`, so they also check the tables `sql/schema.sql` creates.

```sh
python -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/pytest                      # unit tests
.venv/bin/pytest tests_integration    # integration tests: needs Docker running
.venv/bin/pytest -m live              # real LLM providers: needs ANTHROPIC_API_KEY and/or OPENAI_API_KEY, and costs money
```

The integration tests cover:

- a task from submission to `succeeded`, with its message acked and deleted;
- retries with exponential backoff, until the task goes `dead`;
- cancelling a task;
- a duplicate message, and two workers claiming the same task at the same moment;
- the reconciler sending a task after the API crashed between saving it and sending it;
- a worker process killed with SIGKILL mid-task, whose message another worker process reclaims with `XAUTOCLAIM`;
- in real worker processes with `DEFAULT_TASK_TIMEOUT_SECONDS` set, a task that overruns it, and an agent run whose steps all run in the worker's child process;
- the parallel tool calls of 30 agent runs on four workers. SQLite locks the whole database on every write, so only here do they really race.

GitHub Actions (`.github/workflows/ci.yml`) runs the unit tests on every push and the integration tests on pull requests and pushes to main.

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

## Chaos test

```sh
python scripts/chaos.py --tasks 10000 --workers 5 --kill-interval 5
```

`chaos.py` resets the stack and submits `record_execution` tasks. Each one appends an entry to its own Redis list every time it runs, then works for `--task-ms`. While the tasks run, the script SIGKILLs a random worker container every `--kill-interval` seconds (`docker kill`), records the messages that worker was holding, and starts it again. When the queue drains it reports:

- **kills:** how many workers it killed.
- **lost tasks:** tasks that never reached a terminal state. This should be 0.
- **duplicate executions:** tasks whose function ran more than once. Some are expected with at-least-once delivery.
- **recovery time:** from a kill until each held message's task ran again.

Results go to `bench/results/chaos-<label>.json`. The script exits non-zero if any task was lost.

## Agent runtime

`dtq/agents/` runs LLM agents on top of the queue. A **run** is one agent working toward a goal, with a set of tools and budgets. It advances in **turns**: one LLM call, then the tool calls that response asked for, which run in parallel. Each LLM call, tool call and human approval is a **step**. Every step that does work executes as one DTQ task, and its outcome is saved in MySQL in the same transaction that creates the steps after it. The full design, with the reasoning behind each choice, is in [docs/agent-runtime-design.md](docs/agent-runtime-design.md).

```
POST /runs ──▶ agent_runs + step 0 ──▶ DTQ task agent_step(run, 0) ──▶ worker
                                                                         │
      ┌──────────────────────────────────────────────────────────────────┘
      ▼
  A  begin   (1 transaction)  lock the run, check the step's turn is current,
                              claim the step, fenced on DTQ's attempt number
  B  execute (no transaction) call the LLM or the tool
  C  record  (1 transaction)  save the outcome if this attempt still owns the step,
                              add usage, create the next steps and their DTQ tasks
      │
      └──▶ XADD the next steps ──▶ … ──▶ final answer: run succeeded
```

### Run an agent

```sh
export ANTHROPIC_API_KEY=...       # and/or OPENAI_API_KEY; a provider is enabled when its key is set
docker compose up -d --build
python scripts/demo_agent.py "What is 1234 * 5678? Send the answer to ops."
python scripts/demo_agent.py --provider scripted --model demo-approval "anything"   # no API key needed
```

`demo_agent.py` prints each step as it finishes and asks you when the run waits for approval. Or use the API directly:

| Endpoint | Purpose |
|---|---|
| `POST /runs` | Start `{goal, model: {provider, name, system, max_output_tokens, effort}, tools, queue, step_max_retries, max_steps, max_tokens, max_cost_micros, max_wall_seconds}` |
| `GET /runs/{id}` | Status, usage, result or error |
| `GET /runs/{id}/steps` | Every step: its input, output, status, attempt and usage |
| `POST /runs/{id}/approve` | Approve a waiting approval step: `{step_no, note, tool_output}` |
| `POST /runs/{id}/reject` | Reject it, which fails the run: `{step_no, note}` |
| `POST /runs/{id}/cancel` | Stop the run at the next step boundary |

Providers: `scripted` (a deterministic LLM for tests and benchmarks), `anthropic` (Claude, Messages API) and `openai` (Chat Completions). Tools are functions registered with `@tool(effect=...)` in `dtq/agents/`, with their parameters' JSON schema taken from type hints; the demo tools are `echo`, `calculator`, `http_get`, `send_notification` and `publish_announcement` (which requires approval).

### What it guarantees

Each guarantee below is checked by the tests named after it.

- **A run resumes at the interrupted step after a worker crash, not at step 0, and a recorded step never runs again.** (`test_worker_killed_during_an_llm_call_resumes_at_that_step`, `test_duplicate_step_messages_change_nothing`)
- **A run can't fork.** One turn is current at a time; the call that brings the turn's count of unfinished calls to zero is the only one that creates the next LLM step; and every step is fenced on DTQ's attempt number, so a stalled attempt can't record over the one that took it over. (`test_parallel_calls_finishing_in_either_order_start_the_next_turn_once`, `test_the_highest_attempt_owns_a_step_while_both_are_running`)
- **A side-effecting tool call is started at most once.** The step is saved as started before the call; an attempt that takes it over never calls the tool again. It asks the tool's optional `check` whether the call happened, and if nothing can tell, marks the call `unknown_outcome` and pauses the run for a person. A retry they approve keeps the call's idempotency key. (`test_interrupted_call_without_check_is_never_repeated`, `test_demo_notification_is_sent_at_most_once_despite_a_crash_and_redelivery`, `test_approving_an_unknown_outcome_retries_the_call_with_its_key`)
- **Budgets stop a run.** Before each LLM call, a run that has used up `max_steps` (LLM calls), `max_tokens`, `max_cost_micros` or `max_wall_seconds` moves to `budget_exceeded`; time spent awaiting approval doesn't count toward wall time. (`tests/test_agent_budgets.py`)
- **Tools marked `requires_approval` wait for a person**, once per turn, before any of the turn's calls run. Approve, reject and cancel each take one transaction that locks the run row first, so when they race, the first to commit wins and the others get a 409. (`tests/test_agent_approvals.py`)

What it doesn't guarantee:

- **Exactly-once execution.** LLM calls, read-only and idempotent tools, and `check` can run more than once after a crash; only one outcome is recorded. An LLM call lost to a crash is billed by the provider but not counted in the run's usage.
- **That a call marked `unknown_outcome` didn't happen,** or that a tool declared `idempotent` really is. A person decides the first; the runtime trusts the second.
- **Hard budgets.** A run can overshoot by one LLM call, because usage is known only after it.
- **Fast recovery.** Resuming after a worker crash waits for the visibility timeout (30 s).

### Results

**Chaos test** (`scripts/agent_chaos.py`). 500 runs of the `chaos-agent` script on 5 workers: each run is four LLM turns (simulated 0.3 s each) with four tool calls, two of them `send_notification` (200 ms of simulated latency, half before its write and half after). A random worker was SIGKILLed every 5 s and restarted. Runs that paused on an unknown outcome were resolved as a person would: approve with the result if the notification was in the sink, retry if not.

| | |
|---|---|
| kills | 36 (30 hit a worker holding a step) |
| runs succeeded / lost | 500 / 0 |
| runs with an extra LLM step (a fork) | 0 |
| notifications sent | 1000, exactly 2 per run: **0 duplicate side effects** |
| calls killed between intent and send | 2: marked `unknown_outcome`, retried |
| calls killed after sending, before recording | 5: settled by the tool's `check`, not sent again |
| steps re-executed (wasted work) | 28 of 4004 (21 LLM, 7 tool) |
| resume after a kill | p50 32.2 s, p95 32.4 s, set by the 30 s visibility timeout |

From `bench/results/agent-chaos-500r-w5-k5-20261102-045244.json`, at commit `c00a1a4`, on an Apple M2 (Docker: 8 CPUs), on battery power with the machine kept awake.

**Benchmark** (`scripts/agent_bench.py`). The scripted LLM and tools take no time, so these are the runtime's own costs. Medians of 3 repeats per worker count:

| workers | step overhead p50 / p95 | runs/s | steps/s |
|---|---|---|---|
| 1 | 7.0 / 8.6 ms | 19.2 | 115.5 |
| 5 | 6.0 / 9.1 ms | 52.3 | 313.6 |
| 10 | 6.3 / 18.5 ms | 56.4 | 338.2 |

- **Step overhead** is a step's time from being created to its outcome being recorded, over 30 runs (150 steps) run one at a time with nothing else queued. It covers sending the step's message, a worker picking it up, DTQ's claim, the transactions of phases A and C, and DTQ recording the task.
- **Throughput** is for 500 runs of 6 steps each, submitted at once.
- **The 5 to 10 worker plateau.** As with the queue itself, throughput barely rises from 5 to 10 workers. Each step costs five MySQL commits, and the queue's plateau was traced to MySQL's per-commit disk flush (`bench/profiles/FINDINGS.md`).

From `bench/results/agent-bench-20261102-052439-*.json`, at commit `3200dfd`, on an Apple M2 (Docker: 8 CPUs). These were measured on battery power (51–54%), with the machine kept awake. Throughput across the repeats agrees within about 6%. Earlier, the same queue code ran about 10% slower in a session on battery than in one on mains power (`bench/profiles/FINDINGS.md`), so treat these absolute numbers as approximate.

### Design decisions

- **One DTQ task per step, not per run.** A crash loses at most the step in flight, steps of a turn run on different workers, and a run waiting for a person holds no worker. The cost is the per-step overhead measured above.
- **The next steps are created in the transaction that records the outcome.** DTQ's tasks table acts as an outbox: if a worker dies before sending them, DTQ's existing reconciler does.
- **Steps are fenced on DTQ's own attempt number,** so the queue and the step always agree on who owns it.
- **"No sign of it" doesn't allow a retry.** A tool's `check` can prove a side effect happened, but not that a stalled attempt won't still make it, so only a person (or an idempotency key on the external system) can make a retry safe.
- **Anthropic responses are replayed exactly as returned,** including thinking blocks, which the API requires to come back unchanged; the runtime never edits earlier turns.

## Possible next steps

- Several tasks running at once in each worker process (a thread or process pool).
- Prometheus metrics and a dashboard.
- Agent runtime: prompt caching for Anthropic runs, prices for OpenAI models, and recorded results of the live provider tests.
