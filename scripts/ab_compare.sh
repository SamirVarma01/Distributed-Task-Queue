#!/usr/bin/env bash
# Compare two commits with interleaved runs (A, B, A, B, ...), so that drift in the
# machine's speed between runs affects both sides equally. Each side runs from its own
# git worktree on a freshly reset stack.
#
#   scripts/ab_compare.sh NAME A_REF B_REF PAIRS profile [profile_bench.py args]
#   scripts/ab_compare.sh NAME A_REF B_REF PAIRS bench [bench.py args]
#   B_ENV="MYSQL_FLUSH_LOG_AT_TRX_COMMIT=2" scripts/ab_compare.sh NAME REF REF 3 profile
#
# A_ENV / B_ENV are extra environment variables for that side's docker compose up.
# Profiles go to bench/profiles/NAME-{a,b}-<i>/ and results to
# bench/results/NAME-{a,b}-rep<i>.json; medians per side are printed at the end.
#
# WARNING: deletes the Compose stack's data (docker compose down -v) before every run.
set -euo pipefail
source "$(dirname "$0")/bench_guard.sh"
cd "$(dirname "$0")/.."
root=$PWD

name=$1 a_ref=$2 b_ref=$3 pairs=$4 kind=$5
shift 5
PYTHON=${PYTHON:-$([ -x .venv/bin/python ] && echo "$root/.venv/bin/python" || command -v python3)}
WORKERS=${WORKERS:-10}
# Both worktrees drive the same Compose project, so each reset also stops the other side.
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-$(basename "$root" | tr '[:upper:]' '[:lower:]')}

tmp=$(mktemp -d)
cleanup() {
  git worktree remove --force "$tmp/a" 2>/dev/null || true
  git worktree remove --force "$tmp/b" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT
git worktree add --detach "$tmp/a" "$a_ref" >/dev/null 2>&1
git worktree add --detach "$tmp/b" "$b_ref" >/dev/null 2>&1

for i in $(seq "$pairs"); do
  for side in a b; do
    dir=$tmp/$side
    side_env=$([ "$side" = a ] && echo "${A_ENV:-}" || echo "${B_ENV:-}")
    echo "=== $name-$side-$i ($(git -C "$dir" rev-parse --short HEAD)${side_env:+ $side_env})"
    # Keep each run's full output; print only its summary line unless the run fails.
    log=$tmp/$name-$side-$i.log
    if ! (
      cd "$dir"
      docker compose down -v --remove-orphans --timeout 5
      env $side_env docker compose up -d --build --wait --scale worker="$WORKERS"
      if [ "$kind" = profile ]; then
        "$PYTHON" scripts/profile_bench.py --no-reset --workers "$WORKERS" --label "$name-$side-$i" "$@"
      else
        "$PYTHON" scripts/bench.py --workers "$WORKERS" --label "$name-$side-rep$i" "$@"
      fi
    ) >"$log" 2>&1; then
      echo "run failed; last lines of its output:"
      tr '\r' '\n' <"$log" | tail -n 30
      echo "container logs:"
      (cd "$dir" && docker compose logs --tail 20 2>&1) || true
      exit 1
    fi
    tr '\r' '\n' <"$log" | grep -E '"throughput_per_second"|^wrote|tasks/s over' || true
    if [ "$kind" = profile ]; then
      cp -R "$dir/bench/profiles/$name-$side-$i" bench/profiles/
      echo "$side_env" > "bench/profiles/$name-$side-$i/compose_env.txt"
    else
      cp "$dir/bench/results/$name-$side-rep$i.json" bench/results/
    fi
  done
done

echo
if [ "$kind" = profile ]; then
  "$PYTHON" scripts/bench_report.py profiles "$name-"
else
  (cd scripts && "$PYTHON" -c "import bench; bench.write_summary()")
  "$PYTHON" scripts/bench_report.py results "$name-"
fi
