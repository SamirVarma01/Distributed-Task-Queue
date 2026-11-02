"""Tools an agent can call (docs/agent-runtime-design.md, section 7).

Each tool declares its effect, which decides what happens when a worker crashes during
a call. Side-effecting calls need the intent record and unknown-outcome handling of
agent runtime Phase 3; until then only tools that are safe to run again, read-only and
idempotent ones, can be registered."""

import enum
import inspect
import typing
from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, ValidationError, create_model


class Effect(str, enum.Enum):
    READ_ONLY = "read_only"  # no external effects
    IDEMPOTENT = "idempotent"  # repeating a call with the same idempotency key takes effect once
    SIDE_EFFECT = "side_effect"  # not safe to repeat


class ToolError(Exception):
    """Raised by a tool when a call definitely failed with no effect, for example on
    invalid arguments. The message goes back to the LLM as the call's result."""


@dataclass(frozen=True)
class Tool:
    name: str
    fn: Callable[..., Any]
    effect: Effect
    description: str
    # The tool's parameters, from its type hints: validates the LLM's arguments, and
    # gives the JSON schema the LLM is shown.
    args_model: type[BaseModel]
    takes_idempotency_key: bool

    @property
    def parameters(self) -> dict[str, Any]:
        return self.args_model.model_json_schema()


def _args_model(fn: Callable[..., Any]) -> type[BaseModel]:
    hints = typing.get_type_hints(fn)
    fields: dict[str, Any] = {}
    for p in inspect.signature(fn).parameters.values():
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            raise TypeError(f"{fn.__name__}: tools take named parameters only, not *{p.name}")
        if p.name != "idempotency_key":  # supplied by the runtime, never by the LLM
            fields[p.name] = (hints.get(p.name, Any), ... if p.default is p.empty else p.default)
    return create_model(f"{fn.__name__}_arguments", __config__=ConfigDict(extra="forbid"), **fields)


_TOOLS: dict[str, Tool] = {}


def tool(name: str | None = None, *, effect: Effect | str = Effect.SIDE_EFFECT):
    """Register a function as a tool. Its docstring is the description the LLM sees.
    A tool with an idempotency_key parameter is passed its call's key."""
    effect = Effect(effect)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        if effect is Effect.SIDE_EFFECT:
            raise NotImplementedError(
                f"{fn.__name__}: side-effecting tools need agent runtime Phase 3; "
                "declare effect='read_only' or 'idempotent' if the tool is safe to run again")
        key = name or fn.__name__
        _TOOLS[key] = Tool(key, fn, effect, inspect.getdoc(fn) or "", _args_model(fn),
                           "idempotency_key" in inspect.signature(fn).parameters)
        return fn

    return decorator


def get(name: str) -> Tool | None:
    return _TOOLS.get(name)


def names() -> list[str]:
    return sorted(_TOOLS)


def bind_args(t: Tool, args: Any, idempotency_key: str) -> dict[str, Any]:
    """The keyword arguments to call t with: the LLM's arguments, validated against the
    tool's type hints, plus the idempotency key for tools that take one. Raises
    TypeError if the arguments don't fit. The LLM can't choose the key."""
    if isinstance(args, dict) and "idempotency_key" in args:
        raise TypeError("idempotency_key is set by the runtime, not passed as an argument")
    try:
        kwargs = dict(t.args_model.model_validate(args))
    except ValidationError as exc:
        raise TypeError("; ".join(f"{'.'.join(map(str, e['loc'])) or 'arguments'}: {e['msg']}"
                                  for e in exc.errors())) from None
    if t.takes_idempotency_key:
        kwargs["idempotency_key"] = idempotency_key
    return kwargs
