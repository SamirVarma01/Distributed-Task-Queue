"""The agent chaos test's numbers go into the README, so the analysis that produces them
is tested here."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import agent_chaos  # noqa: E402


def step(run, no, kind, status="succeeded", attempt=1, tool=None, key=None, started=100.0):
    return {"run_id": run, "step_no": no, "kind": kind, "status": status, "attempt": attempt, "tool": tool,
            "idempotency_key": key, "started_at": started}


def run_steps(run, sends=(("k1", 1), ("k2", 1)), started=100.0):
    """The chaos-agent's eight steps, with the two sends' keys and attempts."""
    (k1, a1), (k2, a2) = sends
    return [step(run, 0, "llm"), step(run, 1, "tool", tool="calculator"),
            step(run, 2, "tool", attempt=a1, tool="send_notification", key=k1, started=started),
            step(run, 3, "llm"), step(run, 4, "tool", tool="echo"), step(run, 5, "llm"),
            step(run, 6, "tool", attempt=a2, tool="send_notification", key=k2, started=started), step(run, 7, "llm")]


def test_a_clean_run():
    result = agent_chaos.analyze({"r1": ("succeeded", None)}, run_steps("r1"),
                                 {"k1": (101.0, 1), "k2": (102.0, 1)}, {}, [], [])

    assert result["runs"]["succeeded"] == 1 and result["runs"]["lost"] == 0
    assert result["side_effects"] == {"notifications_sent": 2, "calls_sent": 2, "duplicate_effects": 0,
                                      "succeeded_runs_missing_a_notification": 0}
    assert result["settled_by_check"] == 0
    assert result["re_executed_steps"] == {"llm": 0, "tool": 0, "extra_attempts": 0, "steps_total": 8}
    assert result["resume_seconds"]["count"] == 0


def test_lost_runs_duplicates_and_forks_are_counted():
    runs = {"r1": ("succeeded", None), "r2": ("running", None), "r3": ("failed", "step 0: boom")}
    steps = run_steps("r1") + [step("r1", 8, "llm")]  # a fifth LLM step: the run forked
    sink = {"k1": (101.0, 2), "k2": (102.0, 1)}  # k1 was sent twice

    result = agent_chaos.analyze(runs, steps, sink, {}, [], [])

    assert (result["runs"]["lost"], result["runs"]["lost_ids"]) == (1, ["r2"])
    assert result["runs"]["errors"] == ["step 0: boom"]
    assert result["runs"]["with_extra_llm_steps"] == 1
    assert (result["side_effects"]["duplicate_effects"], result["side_effects"]["notifications_sent"]) == (1, 3)


def test_a_notification_found_by_check_and_one_resolved_by_the_operator():
    # k1: sent at 90, then the worker died; the takeover began at 100 and its check found it.
    # k2: the worker died before sending; unknown outcome, retried by the operator.
    steps = run_steps("r1", sends=(("k1", 2), ("k2", 2)))
    steps[6] = step("r1", 6, "tool", status="unknown_outcome", attempt=2, tool="send_notification", key="k2")
    steps.append(step("r1", 9, "tool", tool="send_notification", key="k2", started=150.0))  # the retry
    sink = {"k1": (90.0, 1), "k2": (151.0, 1)}
    decisions = [{"run_id": "r1", "step_no": 8, "resolution": "retried"}]

    result = agent_chaos.analyze({"r1": ("succeeded", None)}, steps, sink, {}, [], decisions)

    assert result["settled_by_check"] == 1
    assert result["unknown_outcomes"] == {"count": 1, "resolved_as_sent": 0, "resolved_by_retry": 1}
    assert result["side_effects"]["duplicate_effects"] == 0
    assert result["side_effects"]["succeeded_runs_missing_a_notification"] == 0
    assert result["re_executed_steps"]["tool"] == 2 and result["re_executed_steps"]["extra_attempts"] == 2


def test_resume_latency_is_measured_from_the_kill_to_the_step_starting_again():
    steps = run_steps("r1")
    steps[3] = step("r1", 3, "llm", attempt=2, started=133.0)  # taken over 31 s after the kill
    kills = [{"time": 102.0, "container": "c", "in_flight": ["task-3", "task-1", "task-gone"]}]
    task_steps = {"task-3": ("r1", 3), "task-1": ("r1", 1)}  # step 1 had finished before the kill

    result = agent_chaos.analyze({"r1": ("succeeded", None)}, steps, {"k1": (101.0, 1), "k2": (102.0, 1)},
                                 task_steps, kills, [])

    assert result["resume_seconds"] == {"count": 1, "p50": 31.0, "p95": 31.0, "max": 31.0,
                                        "held_steps_already_finished": 1, "held_tasks_not_found": 1}
    assert result["re_executed_steps"]["llm"] == 1
