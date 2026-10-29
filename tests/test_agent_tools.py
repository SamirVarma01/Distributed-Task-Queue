import pytest

from dtq.agents import tools
from dtq.agents.tools import Effect, tool


@tool(effect="read_only")
def lookup(key: str, default: str = "") -> str:
    """Look a key up."""
    return default


@tool("upsert_record", effect=Effect.IDEMPOTENT)
def upsert(record: dict, *, idempotency_key: str) -> str:
    return idempotency_key


def test_registered_tools():
    assert {"lookup", "upsert_record"} <= set(tools.names())
    t = tools.get("lookup")
    assert (t.effect, t.description) == (Effect.READ_ONLY, "Look a key up.")
    assert tools.get("missing") is None


def test_side_effecting_tools_wait_for_phase_3():
    with pytest.raises(NotImplementedError, match="Phase 3"):
        @tool(effect="side_effect")
        def send(to: str): ...

    with pytest.raises(NotImplementedError):  # side_effect is the default
        @tool()
        def undeclared(): ...

    assert "send" not in tools.names() and "undeclared" not in tools.names()


def test_bind_args_passes_the_idempotency_key_to_tools_that_take_it():
    assert tools.bind_args(tools.get("lookup"), {"key": "k"}, "run:3") == {"key": "k"}
    assert tools.bind_args(tools.get("upsert_record"), {"record": {}}, "run:3") == {
        "record": {}, "idempotency_key": "run:3"}


@pytest.mark.parametrize("name,args", [
    ("lookup", {}),  # missing argument
    ("lookup", {"key": "k", "nope": 1}),  # unexpected argument
    ("upsert_record", {"record": {}, "idempotency_key": "chosen-by-the-llm"}),
])
def test_bind_args_rejects_arguments_that_dont_fit(name, args):
    with pytest.raises(TypeError):
        tools.bind_args(tools.get(name), args, "run:3")
