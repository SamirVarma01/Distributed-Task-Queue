#!/usr/bin/env bash
# Run scripts/bench.py for each configuration (and each open-loop rate, if RATES is set),
# each on a fresh stack, and print a comparison table. Extra arguments go to bench.py:
#
#   scripts/run_bench_matrix.sh                         # 10000 noop tasks at 1, 5, 10 workers
#   scripts/run_bench_matrix.sh --task sleep_ms --tasks 5000
#   CONFIGS="1x1 1x5 1x10 5x1 10x1" scripts/run_bench_matrix.sh   # WORKERSxSLOTS per worker
#   WORKER_COUNTS=10 RATES="430 687" scripts/run_bench_matrix.sh --duration 60
#   REPEATS=5 WORKER_COUNTS=10 RATES=430 scripts/run_bench_matrix.sh   # medians via bench_report.py
#
# A configuration WxS runs W worker containers with WORKER_CONCURRENCY=S. WORKER_COUNTS
# is shorthand for one slot each: "1 5 10" means CONFIGS="1x1 5x1 10x1". With REPEATS > 1
# the configurations take turns, one repetition of each at a time, so drift in the
# machine's speed affects them all alike.
#
# WARNING: wipes the Compose stack's MySQL and Redis data (docker compose down -v)
# before each run. The stack is left running in the last configuration.
set -euo pipefail
source "$(dirname "$0")/bench_guard.sh"
cd "$(dirname "$0")/.."

PYTHON=${PYTHON:-$([ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)}
WORKER_COUNTS=${WORKER_COUNTS:-"1 5 10"}
CONFIGS=${CONFIGS:-$(for w in $WORKER_COUNTS; do printf '%sx1 ' "$w"; done)}
RATES=${RATES:-burst}
REPEATS=${REPEATS:-1}
run_id="matrix-$(date +%Y%m%d-%H%M%S)"

docker compose build --quiet
for rep in $(seq "$REPEATS"); do
  for config in $CONFIGS; do
    workers=${config%x*} slots=${config#*x}
    for rate in $RATES; do
      label="$run_id-w$workers-c$slots"
      rate_args=()
      if [ "$rate" != burst ]; then
        label="$label-r$rate"
        rate_args=(--rate "$rate")
      fi
      [ "$REPEATS" -gt 1 ] && label="$label-rep$rep"
      echo "=== $label"
      docker compose down -v --remove-orphans --timeout 5 2>/dev/null
      WORKER_CONCURRENCY=$slots docker compose up -d --wait --scale worker="$workers" 2>/dev/null
      # bench.py reads the slots from a worker container, so the result shows what the stack ran.
      WORKER_CONCURRENCY=$slots "$PYTHON" scripts/bench.py --label "$label" --workers "$workers" \
        ${rate_args[@]+"${rate_args[@]}"} "$@"
    done
  done
done

echo
echo "=== $run_id (all rows are in bench/results/summary.md)"
head -n 2 bench/results/summary.md
grep "^| $run_id-" bench/results/summary.md
if [ "$REPEATS" -gt 1 ]; then
  echo
  echo "=== medians over $REPEATS repetitions"
  "$PYTHON" scripts/bench_report.py results "$run_id"
fi
