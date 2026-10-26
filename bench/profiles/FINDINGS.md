# Why throughput plateaus between 5 and 10 workers

**Bottleneck:** MySQL's durable commit. Each task commits twice in the worker (the claim and the result). With the default `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`, every commit waits for a flush to disk. More workers do not make those flushes faster. Commits just queue up behind each other, so each commit takes longer and total throughput barely rises.

All numbers below come from the `summary.json` and `mysql_digests.csv` files in this directory, produced by `scripts/profile_bench.py --tasks 30000` (Apple M2, Docker limited to 8 CPUs and 3.8 GiB, commit `c88c862`). Derived values are computed from those files: per-task worker time = workers / throughput, and commits in flight = commits/s × average commit latency (Little's law).

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
