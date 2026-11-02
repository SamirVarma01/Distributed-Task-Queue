# Per-worker concurrency: design

DTQ Improvement Plan, Phase 5. Status: **accepted** on 2026-11-02 with every proposal below, and implemented in `dtq/worker.py`.

## Goal

`WORKER_CONCURRENCY=N` lets one worker process run up to N tasks at once. The default is 1, which behaves exactly as today.

## Invariants kept

1. **A worker never holds a message it isn't working on.** It reads or reclaims a message only when a slot is free, and that slot starts on the message at once. A held message that nobody works on gets no heartbeat, so another worker would reclaim it and run it a second time.
2. **Every in-flight message is heartbeated** (`XCLAIM ... JUSTID`) until it is acked.
3. **Ack after the outcome is saved; claims are conditional; outcomes are fenced on `attempts`.** Nothing about one task's path changes; there are just N of them at once.
4. **Graceful shutdown** finishes every in-flight task within the grace period.

## Decision 1: threads or processes for the slots

| | Threads in one worker process | A process per slot |
|---|---|---|
| I/O-bound tasks (HTTP, DB, LLM calls) | Run in parallel: blocking I/O releases the GIL | Run in parallel |
| CPU-bound tasks | Serialized by the GIL | Run in parallel |
| Memory | One process (a worker was 120 MB resident in `/proc` during the Phase 4 benchmark) | N processes |
| A task that crashes its process (segfault, `os._exit`) | Takes down all N slots | Takes down one slot |
| Connections | One MySQL pool and one Redis pool, shared | N of each |
| What it adds over `--scale worker=N` | Concurrency at a fraction of the memory | Little: it is N workers in one container |

**Proposal: threads.** Queue tasks are mostly I/O-bound, and CPU-bound work already scales with `--scale worker=N`. Phase 4 also covers the two weaknesses of threads for any task that has a timeout. Such a task runs in its slot's child process, so it can't crash the worker, and it isn't serialized by the GIL.

## Decision 2: how slots get messages

**A. One dispatcher, N slot threads (proposed).** The main thread holds a semaphore with N permits:

```
while not stopping:
    acquire a permit (wait up to READ_BLOCK_MS, then re-check stopping)
    promote due retries; reconcile; reclaim one stale message, or read one (blocking)
    no message -> release the permit
    message    -> hand it to a slot thread, which releases the permit when it acks
```

The dispatcher reads only while it holds a free permit, so invariant 1 holds by construction. Promoting, reconciling and reclaiming stay in one thread, as today. `_read_blocking`'s priority hand-back is unchanged, because there is still one reader.

**B. N independent loops.** Each slot thread runs today's `step()` loop. The code is simpler, but promote, reconcile and reclaim run N times as often, and N blocking `XREADGROUP`s each tie up a Redis connection.

## Decision 3: heartbeats

Today each in-flight message gets its own `Heartbeat` thread.

- **Proposed:** one heartbeat thread per worker. Every `HEARTBEAT_SECONDS` it touches every message in a lock-protected in-flight table, which slots add to before running a task and remove from after acking. A touch that finds the message reclaimed drops it from the table and logs a warning, as today.
  - The same table tells graceful shutdown what is still running.
  - It's also what Phase 6's in-flight gauge would read.
- **Alternative:** keep one thread per message. This is already correct for N messages, at the cost of N extra threads.

## Decision 4: consumer names

- **Proposed:** all slots share the worker's consumer name (`hostname-pid`). Only one thread reads, and the owner check in the ack and touch scripts still separates workers, which is what matters.
- **Alternative:** per-slot names (`hostname-pid-3`) would show in `XPENDING` which slot holds what. But each restart leaves N dead consumer entries in the group instead of one.

## Interaction with Phase 4 timeouts

- **One child per slot.** Each slot owns its own `TaskProcess`, started the first time that slot runs a task with a timeout. Every child is forked from the worker's one forkserver, so the tasks are imported once per worker, not once per slot.
- **Kills stay local.** A timeout kills only that slot's child, and the next task with a timeout on that slot gets a new one.
- **Sharing one pool of children across slots would need a scheduler,** and it would give a kill a blast radius beyond one slot. One child per slot needs neither.

## Graceful shutdown

On SIGTERM:

1. The dispatcher stops taking permits, so no new message is read.
2. The worker waits for the slot threads to finish. The heartbeat thread keeps running until the in-flight table is empty.
3. The worker stops the children and exits.

Compose's `stop_grace_period` (30 s) still bounds this. Docker SIGKILLs a task that is still running then, and its message is reclaimed after the visibility timeout. A deployment whose tasks can run longer than the grace period should raise the grace period, or set timeouts below it.

## Connection limits

- **SQLAlchemy's pool would block.** Its default pool is 5 connections plus 10 overflow, and a slot past 15 would wait (`pool_timeout`, 30 s) and then fail. The worker's engine would be sized to N + 1.
- **MySQL's default `max_connections` is 151.** Replicas × (N + 1) plus the API's pool must fit under it; for example, 10 workers at concurrency 16 don't. The README would say so, and compose would raise the limit if the benchmark matrix needs it.

## Tests

Unit tests, with in-process workers and fakeredis:

- N slow tasks on one worker with `WORKER_CONCURRENCY=N` finish in about one task's time, not N.
- With N slots busy and more tasks queued, the worker holds exactly N pending messages (invariant 1).
- A stalled slot doesn't block the others: quick tasks finish while one task sleeps.
- With N long tasks and a short visibility timeout, another worker reclaims nothing (invariant 2).
- On shutdown, all in-flight tasks finish before `run()` returns.

Integration tests, with a real worker process:

- SIGKILL a worker with N tasks in flight; a second worker reclaims all N and finishes them.

## Decisions

| # | Question | Decision |
|---|---|---|
| 1 | Threads or processes for slots | Threads; tasks with a timeout run in their slot's child process |
| 2 | How slots get messages | One dispatcher that reads only while it holds a free slot |
| 3 | Heartbeats | One heartbeat thread for every in-flight message |
| 4 | Consumer names | Slots share the worker's name |
| 5 | Phase 7 benchmark matrix | 1 worker at concurrency 1, 5 and 10, next to 1, 5 and 10 workers at concurrency 1 |

## Notes from implementing it

- **The heartbeat table records each message's consumer** along with its stream, so a worker whose name changes after it starts (the tests rename theirs) still owns what it touches.
- **Stale messages are reclaimed one per free slot.** A reclaim pass now spans several turns of the dispatcher's loop, taking one stale message per turn until none are left; before, one pass ran every stale message in turn.
- **`Worker.step()` remains** for tests and drives the same path as the dispatcher, running the message in the calling thread.
