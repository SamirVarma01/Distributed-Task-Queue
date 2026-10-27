"""The chaos test's numbers (lost tasks, duplicates, recovery time) go into the README,
so the analysis that produces them is tested here."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import chaos  # noqa: E402


def test_analyze_counts_lost_duplicated_and_recovered_tasks():
    task_keys = {"t1": "k1", "t2": "k2", "t3": "k3", "t4": "k4"}
    statuses = {
        "t1": ("succeeded", 1),
        "t2": ("succeeded", 2),
        "t3": ("running", 1),     # never finished: lost
        "t4": ("succeeded", 1),   # succeeded but no execution recorded
    }
    executions = {
        "k1": [(10.0, "a")],
        "k2": [(10.0, "a"), (45.0, "b")],  # killed mid-run, re-run 33 s after the kill
        "k3": [(11.0, "c")],
        "k4": [],
    }
    kills = [{"time": 12.0, "container": "a", "in_flight": ["t2", "t1"]}]  # t1 finished, not acked

    out = chaos.analyze(task_keys, statuses, executions, kills)

    assert out["tasks"] | {"lost_ids": None} == {
        "submitted": 4, "succeeded": 3, "dead": 0, "cancelled": 0, "lost": 1, "lost_ids": None,
        "ran_more_than_one_attempt": 1,
    }
    assert out["tasks"]["lost_ids"] == ["t3"]
    assert out["executions"] == {
        "total": 4, "tasks_run_more_than_once": 1, "extra_executions": 1, "max_per_task": 2,
        "succeeded_without_execution": 1,
    }
    assert out["kills"] == 1
    assert out["recovery_seconds"] == {"count": 1, "p50": 33.0, "p95": 33.0, "max": 33.0,
                                       "in_flight_already_finished": 1}
