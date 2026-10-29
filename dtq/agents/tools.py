"""Tools an agent can call (docs/agent-runtime-design.md, section 7).

Each tool declares its effect, which decides what happens when a worker crashes during
a call. Side-effecting calls need the intent record and unknown-outcome handling of
agent runtime Phase 3; until then only tools that are safe to run again, read-only and
idempotent ones, can be registered."""

import enum
import inspect
from dataclasses import dataclass
from typing import Any, Callable


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
        _TOOLS[key] = Tool(key, fn, effect, inspect.getdoc(fn) or "")
        return fn

    return decorator


def get(name: str) -> Tool | None:
    return _TOOLS.get(name)


def names() -> list[str]:
    return sorted(_TOOLS)


def bind_args(t: Tool, args: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
    """The keyword arguments to call t with. Raises TypeError if args (from the LLM)
    don't fit its parameters. The idempotency key comes from the runtime, never from
    the LLM."""
    if "idempotency_key" in args:
        raise TypeError("idempotency_key is set by the runtime, not passed as an argument")
    kwargs = dict(args)
    if "idempotency_key" in inspect.signature(t.fn).parameters:
        kwargs["idempotency_key"] = idempotency_key
    inspect.signature(t.fn).bind(**kwargs)
    return kwargs
