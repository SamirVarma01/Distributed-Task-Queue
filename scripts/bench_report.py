"""Summarize repeated benchmark or profile runs as medians:

    python scripts/bench_report.py results matrix-20261026-111636    # bench/results/<prefix>*.json
    python scripts/bench_report.py profiles w10-0-baseline w10-1-     # bench/profiles/<prefix>*/

Runs are grouped by label with the repetition suffix removed (-rep<N> for results,
-<N> for profiles), and each metric is the median across the group's runs.
"""

import argparse
import csv
import json
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
PROFILES = ROOT / "bench" / "profiles"


def median(values) -> float:
    return statistics.median(values)


def group(items: list[tuple[str, dict]], suffix: str) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for label, data in sorted(items):
        groups.setdefault(re.sub(suffix + "$", "", label), []).append(data)
    return groups


def results_table(prefixes: list[str]) -> str:
    items = [(p.stem, json.loads(p.read_text())) for prefix in prefixes for p in RESULTS.glob(f"{prefix}*.json")]
    rows = ["| runs | mode | workers | rate (tasks/s) | throughput (tasks/s) | latency p50 (ms) | latency p95 (ms) "
            "| latency p99 (ms) | p95 range (ms) | submit lag p99 (ms) |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    for label, runs in group(items, r"-rep\d+").items():
        m = [r["metrics"] for r in runs]
        p95 = [x["latency_ms"]["p95"] for x in m]
        rate = runs[0].get("rate", {}).get("target")
        lag = [r["rate"]["submit_lag_ms"]["p99"] for r in runs if "rate" in r]
        rows.append(
            f"| {len(runs)} × {label} | {runs[0].get('mode', 'burst')} | {runs[0]['workers']} | {rate or '–'} "
            f"| {median(x['throughput_per_second'] for x in m):.0f} "
            f"| {median(x['latency_ms']['p50'] for x in m):.0f} | {median(p95):.0f} "
            f"| {median(x['latency_ms']['p99'] for x in m):.0f} | {min(p95):.0f}–{max(p95):.0f} "
            f"| {f'{median(lag):.1f}' if lag else '–'} |")
    return "\n".join(rows)


def commit_avg_ms(profile_dir: Path) -> float:
    with (profile_dir / "mysql_digests.csv").open() as f:
        return next(float(r["avg_ms"]) for r in csv.DictReader(f) if r["digest_text"] == "COMMIT")


def profiles_table(prefixes: list[str]) -> str:
    items = []
    for prefix in prefixes:
        for d in PROFILES.glob(f"{prefix}*"):
            if (d / "summary.json").exists():
                s = json.loads((d / "summary.json").read_text())
                s["commit_avg_ms"] = commit_avg_ms(d)
                items.append((d.name, s))
    rows = ["| runs | throughput (tasks/s) | COMMIT avg (ms) | MySQL round trips / task | statements / task "
            "| pings / task | ROLLBACK / task | commits/s | log fsyncs/s | worker CPU / task (ms) | MySQL CPU | total CPU |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for label, runs in group(items, r"-\d+").items():
        def med(f):
            return median(f(s) for s in runs)

        def worker_cpu_per_task(s):
            cpu = [v for k, v in s["cpu_percent_mean_by_container"].items() if "worker" in k]
            return sum(cpu) / len(cpu) / 100 * s["workers"] / s["throughput_per_second"] * 1000

        def container_cpu(s, name):
            return next(v for k, v in s["cpu_percent_mean_by_container"].items() if name in k)

        rows.append(
            f"| {len(runs)} × {label} | {med(lambda s: s['throughput_per_second']):.0f} "
            f"| {med(lambda s: s['commit_avg_ms']):.2f} "
            f"| {med(lambda s: s.get('mysql_round_trips_per_task', float('nan'))):.2f} "
            f"| {med(lambda s: s['mysql_per_task']['Questions']):.2f} "
            f"| {med(lambda s: s['mysql_per_task'].get('Com_admin_commands', float('nan'))):.2f} "
            f"| {med(lambda s: s['mysql_per_task']['Com_rollback']):.2f} "
            f"| {med(lambda s: s['mysql_per_second']['Com_commit']):.0f} "
            f"| {med(lambda s: s['mysql_per_second']['Innodb_os_log_fsyncs']):.0f} "
            f"| {med(worker_cpu_per_task):.2f} | {med(lambda s: container_cpu(s, 'mysql')):.0f}% "
            f"| {med(lambda s: s['cpu_percent_total']['mean']):.0f}% |")
    return "\n".join(rows)


def main():
    p = argparse.ArgumentParser(description="Median summary of repeated runs.")
    p.add_argument("kind", choices=["results", "profiles"])
    p.add_argument("prefixes", nargs="+", help="label prefixes to include")
    args = p.parse_args()
    print(results_table(args.prefixes) if args.kind == "results" else profiles_table(args.prefixes))


if __name__ == "__main__":
    main()
