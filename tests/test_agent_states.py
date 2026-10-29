import itertools
import re
from pathlib import Path

import pytest

from dtq.agents.models import AgentRun, RunStatus
from dtq.agents.states import ALLOWED_FROM, TERMINAL, InvalidTransition, transition
from dtq.db import SessionLocal

ALL_MOVES = list(itertools.product(RunStatus, RunStatus))
ALLOWED = {(src, dst) for dst, sources in ALLOWED_FROM.items() for src in sources}


def make_run(status: RunStatus) -> str:
    run = AgentRun(goal="g", model={}, tools=[], queue="default", max_steps=5, status=status)
    with SessionLocal() as s:
        s.add(run)
        s.commit()
    return run.id


def get_run(run_id: str) -> AgentRun:
    with SessionLocal() as s:
        return s.get(AgentRun, run_id)


@pytest.mark.parametrize("src,dst", [m for m in ALL_MOVES if m in ALLOWED], ids=lambda s: s.value)
def test_allowed_transition_moves_the_run(src, dst):
    run_id = make_run(src)
    with SessionLocal() as s:
        assert transition(s, run_id, src, dst)
        s.commit()

    run = get_run(run_id)
    assert run.status == dst
    assert (run.finished_at is not None) == (dst in TERMINAL)


@pytest.mark.parametrize("src,dst", [m for m in ALL_MOVES if m not in ALLOWED], ids=lambda s: s.value)
def test_illegal_transition_raises_and_leaves_the_run_alone(src, dst):
    run_id = make_run(src)
    with SessionLocal() as s, pytest.raises(InvalidTransition):
        transition(s, run_id, src, dst)

    assert get_run(run_id).status == src


def test_terminal_states_have_no_way_out():
    assert not any(src in TERMINAL for src, _ in ALLOWED)


def test_run_that_already_moved_on_is_not_moved():
    """A cancel that lost a race to the run finishing: the move is legal, but the run
    is no longer in a state it can be cancelled from."""
    run_id = make_run(RunStatus.SUCCEEDED)
    with SessionLocal() as s:
        moved = transition(s, run_id, ALLOWED_FROM[RunStatus.CANCELLED], RunStatus.CANCELLED)
        s.commit()

    assert not moved
    assert get_run(run_id).status == RunStatus.SUCCEEDED


def test_source_states_are_checked_against_the_actual_status():
    """Approving moves a run from awaiting_approval to running; it must not start a
    queued run, even though queued -> running is also a legal move."""
    run_id = make_run(RunStatus.QUEUED)
    with SessionLocal() as s:
        assert not transition(s, run_id, RunStatus.AWAITING_APPROVAL, RunStatus.RUNNING)

    assert get_run(run_id).status == RunStatus.QUEUED


def test_unknown_run_is_not_moved():
    with SessionLocal() as s:
        assert not transition(s, "missing", RunStatus.QUEUED, RunStatus.CANCELLED)


def test_design_doc_diagram_matches_the_table():
    doc = (Path(__file__).parent.parent / "docs" / "agent-runtime-design.md").read_text()
    diagram = re.search(r"```mermaid\n(.*?)```", doc, re.S).group(1)
    edges = {(RunStatus(a), RunStatus(b))
             for a, b in re.findall(r"^\s*(\w+) --> (\w+)", diagram, re.M)}
    assert edges == ALLOWED
