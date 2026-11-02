"""Tools an agent can call (docs/agent-runtime-design.md, section 7).

Each tool declares its effect, which decides what happens when a worker crashes during
a call: read-only and idempotent calls run again; a side-effecting call is never made
twice. Its outcome is settled by the tool's check, if it has one, or else marked
unknown for a person to decide."""

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
    # For a side-effecting tool: check(idempotency_key) returns the call's result if
    # the call happened, or None if there is no sign that it did.
    check: Callable[[str], Any] | None
    # A person must approve each call before it runs (agent runtime Phase 4).
    requires_approval: bool
    # The tool's parameters, from its type hints: validates the LLM's arguments, and
    # gives the JSON schema the LLM is shown.
    args_model: type[BaseModel]
    takes_idempotency_key: bool

    @property
    def parameters(self) -> dict[str, Any]:
        return self.args_model.model_json_schema()


def _args_model(fn: Callable[..., Any]) -> type[BaseModel]:
    hints = typing.get_type_hints(fn, include_extras=True)  # keep Annotated[..., Field(...)]
    fields: dict[str, Any] = {}
    for p in inspect.signature(fn).parameters.values():
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            raise TypeError(f"{fn.__name__}: tools take named parameters only, not *{p.name}")
        if p.name != "idempotency_key":  # supplied by the runtime, never by the LLM
            fields[p.name] = (hints.get(p.name, Any), ... if p.default is p.empty else p.default)
    return create_model(f"{fn.__name__}_arguments", __config__=ConfigDict(extra="forbid"), **fields)


_TOOLS: dict[str, Tool] = {}


def tool(name: str | None = None, *, effect: Effect | str = Effect.SIDE_EFFECT,
         check: Callable[[str], Any] | None = None, requires_approval: bool = False):
    """Register a function as a tool. Its docstring is the description the LLM sees.
    A tool with an idempotency_key parameter is passed its call's key.

    effect defaults to side_effect, so a tool that doesn't declare one is never run
    twice. check is only for side-effecting tools (see Tool.check)."""
    effect = Effect(effect)
    if check is not None and effect is not Effect.SIDE_EFFECT:
        raise ValueError("check is for side-effecting tools; other tools are simply run again")

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        key = name or fn.__name__
        _TOOLS[key] = Tool(key, fn, effect, inspect.getdoc(fn) or "", check, requires_approval, _args_model(fn),
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
