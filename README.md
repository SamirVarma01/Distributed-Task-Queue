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

API docs: http://localhost:8000/docs

```sh
curl -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
     -d '{"name": "add", "payload": {"a": 2, "b": 3}, "queue": "high"}'
curl localhost:8000/tasks/<id>
curl localhost:8000/stats
```

| Endpoint | Purpose |
|---|---|
| `POST /tasks` | Submit `{name, payload, queue, max_retries, delay_seconds}` |
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
- **Idempotent claims.** A worker starts a task with a conditional `UPDATE ... WHERE status NOT IN (terminal)`. If the same message arrives twice, or the task was cancelled, the worker just acks the message and skips it.
- **Priority.** `QUEUES=high,default,low` sets the order in which workers drain the queues.
- **Graceful shutdown.** On SIGTERM a worker finishes its current task before it exits (`stop_grace_period: 30s`).

**Delivery is at-least-once.** A worker can crash after a task's side effects happen but before it records the result. When that happens the task runs again, so tasks should be idempotent.

## Possible next steps

- A reconciler that re-enqueues `queued` rows that never reached Redis (for example, if the API crashed between the MySQL insert and the `XADD`).
- Per-task timeouts (run tasks in a subprocess and kill it when time runs out).
- Several tasks running at once in each worker process (a thread or process pool).
- Prometheus metrics and a dashboard.
- Tests, using testcontainers for MySQL and Redis.
