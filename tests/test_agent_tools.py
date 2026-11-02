from typing import Literal

import pytest

from dtq.agents import tools
from dtq.agents.tools import Effect, tool


@tool(effect="read_only")
def read_setting(key: str, default: str = "") -> str:
    """Read a setting."""
    return default


@tool("upsert_record", effect=Effect.IDEMPOTENT)
def upsert(record: dict, *, idempotency_key: str) -> str:
    return idempotency_key


def test_registered_tools():
    assert {"read_setting", "upsert_record"} <= set(tools.names())
    t = tools.get("read_setting")
    assert (t.effect, t.description) == (Effect.READ_ONLY, "Read a setting.")
    assert tools.get("missing") is None


def test_side_effect_is_the_default_and_only_side_effecting_tools_take_a_check():
    @tool()
    def undeclared(): ...

    def sent(idempotency_key): ...

    @tool(effect="side_effect", check=sent)
    def send(to: str): ...

    assert tools.get("undeclared").effect is Effect.SIDE_EFFECT
    assert (tools.get("send").effect, tools.get("send").check) == (Effect.SIDE_EFFECT, sent)
    with pytest.raises(ValueError, match="check is for side-effecting tools"):
        tool(effect="read_only", check=sent)


@tool(effect="read_only")
def search(query: str, limit: int = 10, tags: list[str] | None = None, order: Literal["new", "old"] = "new",
           *, idempotency_key: str) -> list:
    """Search."""
    return []


def test_parameter_schema_comes_from_type_hints():
    schema = tools.get("search").parameters

    assert schema["required"] == ["query"]
    assert schema["additionalProperties"] is False
    props = schema["properties"]
    assert set(props) == {"query", "limit", "tags", "order"}  # not idempotency_key
    assert (props["query"]["type"], props["limit"]["type"], props["limit"]["default"]) == ("string", "integer", 10)
    assert props["order"]["enum"] == ["new", "old"]
    assert {"type": "array", "items": {"type": "string"}} in props["tags"]["anyOf"]


def test_bind_args_validates_types():
    t = tools.get("search")
    assert tools.bind_args(t, {"query": "q", "tags": ["a"]}, "run:3") == {
        "query": "q", "limit": 10, "tags": ["a"], "order": "new", "idempotency_key": "run:3"}
    with pytest.raises(TypeError, match="limit"):
        tools.bind_args(t, {"query": "q", "limit": "many"}, "run:3")
    with pytest.raises(TypeError, match="order"):
        tools.bind_args(t, {"query": "q", "order": "random"}, "run:3")
    with pytest.raises(TypeError):
        tools.bind_args(t, ["not", "an", "object"], "run:3")


def test_tools_take_named_parameters_only():
    with pytest.raises(TypeError, match="named parameters only"):
        @tool(effect="read_only")
        def varargs(*items): ...


def test_bind_args_passes_the_idempotency_key_to_tools_that_take_it():
    assert tools.bind_args(tools.get("read_setting"), {"key": "k"}, "run:3") == {"key": "k", "default": ""}
    assert tools.bind_args(tools.get("upsert_record"), {"record": {}}, "run:3") == {
        "record": {}, "idempotency_key": "run:3"}


@pytest.mark.parametrize("name,args", [
    ("read_setting", {}),  # missing argument
    ("read_setting", {"key": "k", "nope": 1}),  # unexpected argument
    ("upsert_record", {"record": {}, "idempotency_key": "chosen-by-the-llm"}),
])
def test_bind_args_rejects_arguments_that_dont_fit(name, args):
    with pytest.raises(TypeError):
        tools.bind_args(tools.get(name), args, "run:3")
