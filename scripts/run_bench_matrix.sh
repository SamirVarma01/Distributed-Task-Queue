#!/usr/bin/env bash
# Run scripts/bench.py for each worker count (and each open-loop rate, if RATES is set),
# each on a fresh stack, and print a comparison table. Extra arguments go to bench.py:
#
#   scripts/run_bench_matrix.sh                         # 10000 noop tasks at 1, 5, 10 workers
#   scripts/run_bench_matrix.sh --task sleep_ms --tasks 5000
#   WORKER_COUNTS=10 RATES="430 687" scripts/run_bench_matrix.sh --duration 60
#
# WARNING: wipes the Compose stack's MySQL and Redis data (docker compose down -v)
# before each run. The stack is left running at the last worker count.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=${PYTHON:-$([ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)}
WORKER_COUNTS=${WORKER_COUNTS:-"1 5 10"}
RATES=${RATES:-burst}
run_id="matrix-$(date +%Y%m%d-%H%M%S)"

docker compose build --quiet
for workers in $WORKER_COUNTS; do
  for rate in $RATES; do
    label="$run_id-w$workers"
    rate_args=()
    if [ "$rate" != burst ]; then
      label="$label-r$rate"
      rate_args=(--rate "$rate")
    fi
    echo "=== $label"
    docker compose down -v --remove-orphans --timeout 5 2>/dev/null
    docker compose up -d --wait --scale worker="$workers" 2>/dev/null
    "$PYTHON" scripts/bench.py --label "$label" --workers "$workers" ${rate_args[@]+"${rate_args[@]}"} "$@"
  done
done

echo
echo "=== $run_id (all rows are in bench/results/summary.md)"
head -n 2 bench/results/summary.md
grep "^| $run_id-" bench/results/summary.md
