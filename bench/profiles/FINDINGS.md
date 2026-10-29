# Why throughput plateaus between 5 and 10 workers

> **Update after Phase 1.5:** the sections below describe the code at `4bcdeb2`. The Phase 1.5 changes cut MySQL's commit cost, but flushing Redis to disk on every write, which at-least-once delivery needs, then cost about 30% of throughput. Latency at moderate load improved about 40×, and maximum throughput did not improve. See [Phase 1.5](#phase-15-changes-and-their-measured-effect) at the end.

**Bottleneck:** MySQL's durable commit. Each task commits twice in the worker (the claim and the result). With the default `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`, every commit waits for a flush to disk. More workers do not make those flushes faster. Commits just queue up behind each other, so each commit takes longer and total throughput barely rises.

All numbers below come from the `summary.json` and `mysql_digests.csv` files in this directory, produced by `scripts/profile_bench.py --tasks 30000` (Apple M2, Docker limited to 8 CPUs and 3.8 GiB, commit `4bcdeb2`). Derived values are computed from those files: per-task worker time = workers / throughput, and commits in flight = commits/s × average commit latency (Little's law).

## Evidence

| | 5 workers | 10 workers | change |
|---|---|---|---|
| throughput (tasks/s) | 611.0 | 722.2 | +18% |
| MySQL commits/s | 1210.1 | 1416.6 | +17% |
| average `COMMIT` latency (ms) | 1.890 | 3.521 | +86% |
| commits in flight | 2.29 | 4.99 | |
| commits per redo-log fsync | 0.97 | 1.55 | |
| per-task worker time (ms) | 8.18 | 13.85 | |
| of which waiting on the two commits (ms) | 3.78 (46%) | 7.04 (51%) | |
| mean CPU per worker container | 39.0% | 29.6% | |
| total container CPU (of 800%) | 302.8% | 415.8% | |
| MySQL container CPU | 82.6% | 91.8% | |

- **Commits saturate.** Doubling the workers roughly doubles the commits waiting in MySQL (2.29 → 4.99), but the commit rate rises only 17%. The extra commits wait in line, and group commit packs more of them into each fsync (0.97 → 1.55 commits per fsync).
- **Only `COMMIT` is slow.** Over the 10-worker run, `COMMIT` took 211.1 s of MySQL time. Each of the other statements takes 0.18 ms or less on average and adds up to at most 5.8 s (`mysql_digests.csv`).
- **No lock contention.** There was 1 row-lock wait, lasting 6 ms, over 30,000 tasks. `Innodb_log_waits` and `Innodb_buffer_pool_wait_free` were 0, and the `SHOW ENGINE INNODB STATUS` snapshots show `0 queries in queue`.
- **CPU and Redis are not the limit.** Containers used about half of the Docker VM's CPU. Worker CPU fell as workers were added, meaning they spent more time waiting. Redis used 11.0% CPU and about 8 calls per task.

## Confirming experiment (not a fix)

`profile-w10-relaxed-durability-experiment` repeats the 10-worker run with MySQL started as `--innodb-flush-log-at-trx-commit=2 --sync-binlog=0`, using a Compose override that is not in the repo. Its `summary.json` records those two settings.

| 10 workers | default durability | relaxed durability |
|---|---|---|
| throughput (tasks/s) | 722.2 | 1018.6 (+41%) |
| average `COMMIT` latency (ms) | 3.521 | 0.401 |
| redo-log fsyncs/s | 912.7 | 14.4 |
| commit wait per task (ms) | 7.04 of 13.85 | 0.80 of 9.82 |
| total container CPU (of 800%) | 415.8% | 594.2% |

Removing the per-commit fsync cut commit latency by 89% and raised throughput 41%. That confirms the fsync as the main limit at 10 workers. The experiment does not show that relaxing durability is acceptable. With `=2`, a crash of the OS or VM, not just MySQL, can lose about the last second of committed results.

## Secondary costs (measured, not yet the limit)

- **Worker CPU per task:** 3.2–4.1 ms for a `noop` task (CPU share × per-task time). That alone caps a single worker near 250–300 tasks/s, consistent with the 297 tasks/s measured at 1 worker in `bench/results/matrix-20261026-105640-w1.json`. Once commits are cheap, this is the next likely limit, together with MySQL CPU (172% in the relaxed run).
- **10 MySQL round trips per task:** `Questions` / tasks. These are 2 `COMMIT`, 2 `UPDATE`, 1 `SELECT` (re-reading the row after the claim), 1 `INSERT` (the API's batch insert, counted per row) and 4 `ROLLBACK`. I haven't confirmed where the `ROLLBACK`s come from. The likely source is SQLAlchemy rolling back when each session closes and again when the pool resets the connection. Each one is cheap for the server (0.05 ms) but still costs a network round trip.

## Caveats

- Profiling lowers throughput. The `docker stats` sampler runs alongside the benchmark, and these runs had 30,000 tasks instead of 10,000. The unprofiled matrix measured 753 tasks/s at 5 workers and 859 at 10, against 611 and 722 here. Compare profiles with each other, not with `bench/results`.
- `docker stats` was sampled every 1.5–2.5 s (15–24 samples per run), so the CPU figures are averages, not peaks.
- The disk is Docker Desktop's virtual disk on macOS. Fsync cost on Linux hardware will differ, so the plateau point is specific to this machine.

## Phase 1.5: changes and their measured effect

**Method.** Each change was compared with the commit before it using `scripts/ab_compare.sh`. Runs alternate between the two commits (A, B, A, B, …), each on a freshly reset stack, so drift in the machine's speed affects both sides equally. The tables show medians across pairs, produced by `scripts/bench_report.py`. Profiles are 30,000-task bursts at 10 workers, and open-loop runs last 60 s at 10 workers.

Compare numbers within a comparison, not across them. Changes 1–3 were measured on 2026-10-26 on a draining battery, and the rest on 2026-10-27 on AC power. `pmset -g log` shows no sleep inside any window used here. Absolute speed differs between the two sessions: the same worker code measured 932 tasks/s in one and 1026 in the other (`c3-blocking-read-b`, `c4-flush2-optin-a`).

### Each change on its own

| change | A → B | runs | median A → B | throughput ratio B/A per pair |
|---|---|---|---|---|
| 1. Drop redundant round trips | `7ddcff1` → `94e0dc5` | 3 pairs | MySQL round trips per task 13.09 → 8.07 (`ROLLBACK` 4.01 → 0, pings 3 → 2); throughput 446 → 484 tasks/s | 0.71, 1.42, 0.97 |
| 2. Disable the binary log | `94e0dc5` → `066c63a` | 3 pairs | `COMMIT` 5.01 → 3.01 ms; throughput 550 → 644 tasks/s | 1.08, 1.09, 1.91 |
| 3. Blocking read instead of a 0.5 s idle sleep | `9e14866` → `9cd449d` | 3 + 3 pairs | burst throughput 906 → 932 tasks/s (no change expected); at 430/s, latency p50 254 → 6 ms and p95 484 → 15 ms | 1.00, 1.08, 0.73 |
| Fix: Redis `appendfsync always` | `89511a3` → `6cd4486` | 3 pairs | throughput 1084 → 754 tasks/s; Redis CPU about 14–15% → about 10% | 0.63, 0.81, 0.70 |
| Opt-in `innodb_flush_log_at_trx_commit=2` (not the default) | `c8eed96` → same commit with `MYSQL_FLUSH_LOG_AT_TRX_COMMIT=2` | 3 pairs | throughput 1026 → 1325 tasks/s; `COMMIT` 1.58 → 0.37 ms; redo-log fsyncs 1090/s → 15/s | 1.29, 1.19, 1.32 |

- **Change 1** removed exactly what the MySQL general log showed was redundant. Its effect on throughput was smaller than the run-to-run noise (pair ratios on both sides of 1), so this table doesn't claim one.
- **Change 3's latency gain** is consistent across pairs: p95 was 484–485 ms for A and 12–19 ms for B.
- **The other bug fixes** (fenced result writes, owner-checked heartbeat and ack, NaN results) were not measured on their own. They are included in the final comparison below.

### Baseline vs final

Baseline is `7ddcff1` (the Phase 1 code). Final is `6cd4486`: all Phase 1.5 changes, the bug fixes, and Redis `appendfsync always`.

| workload | runs | baseline | final |
|---|---|---|---|
| burst, 1 worker (tasks/s) | 2 pairs | 294 | 292 |
| burst, 5 workers (tasks/s) | 2 pairs | 742 | 730 |
| burst, 10 workers (tasks/s) | 2 pairs | 851 | 757 |
| 430/s open loop, latency p50 / p95 / p99 (ms) | 5 pairs | 233 / 480 / 513 | 6 / 31 / 132 |
| 430/s, p95 range across runs (ms) | | 478–493 | 16–989 |
| 687/s open loop, latency p50 / p95 / p99 (ms) | 5 pairs | 493 / 1936 / 2351 | 79 / 4453 / 4841 |
| 687/s, p95 range across runs (ms) | | 501–9185 | 131–16373 |

A burst matrix on the final code measured 293, 687 and 742 tasks/s at 1, 5 and 10 workers (`matrix-20261027-013238`).

### What this shows

- **Latency at moderate load is the clear win.** At 430/s the median task now finishes in 6 ms instead of 233 ms. That comes from the blocking read: on its own, change 3 took the p50 from 254 ms to 6 ms.
- **Maximum throughput did not improve.** Disabling the binary log helped, but fsyncing Redis on every write cost more. At 10 workers the final code is slower (851 → 757 tasks/s). At 1 and 5 workers the difference is within noise.
- **Near saturation, both versions are erratic, and the final one more so.** 687/s is about 81% of the baseline's burst maximum but about 91% of the final code's. In several runs on both sides, the workers fell behind the rate and built a backlog, which shows up as multi-second p95s.
- **The time now goes to waiting for Redis to flush to disk.** With `appendfsync always`, throughput fell by about 30% while Redis CPU fell from about 14–15% to 10%, MySQL CPU from 93% to 76%, and total container CPU from 517% to 377%. Everything is busy less and waiting more. Each Redis write in a task's path (the `XADD`, the `XREADGROUP` that records the pending entry, the ack) now waits for Redis's append-only file to reach disk.

### Options (not implemented)

1. **Accept the cost.** Crash-safe at-least-once delivery is worth about 30% of peak throughput on this machine.
2. **Fewer Redis writes per task.** For example, ack the finished message and read the next one in a single script, so they share one flush.
3. **Make lost Redis writes recoverable from MySQL instead,** and keep `everysec`. A reconciler would re-send any non-terminal task whose message is gone. The hard part is telling "lost" apart from "waiting in a long backlog", which needs each task's message ID recorded and checked.
4. **`REDIS_APPENDFSYNC=everysec`** remains an opt-in for benchmarks. It is not crash-safe.
