"""Example tools and ScriptedLLM scripts, registered in the API and workers like the
example tasks in dtq/tasks.py."""

import ast
import operator
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Annotated

from pydantic import Field
from sqlalchemy import JSON, BigInteger, DateTime, Integer, String, select
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base, SessionLocal, utcnow
from . import scripted
from .tools import ToolError, tool


@tool(effect="read_only")
def echo(text: str) -> str:
    """Return the text unchanged."""
    return text


_BINARY = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv,
           ast.FloorDiv: operator.floordiv, ast.Mod: operator.mod, ast.Pow: operator.pow}
_UNARY = {ast.UAdd: operator.pos, ast.USub: operator.neg}


@tool(effect="read_only")
def calculator(expression: str) -> float:
    """Evaluate an arithmetic expression: numbers, + - * / // % ** and parentheses."""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError:
        raise ToolError(f"not an arithmetic expression: {expression!r}") from None
    return _evaluate(tree.body)


def _evaluate(node: ast.AST) -> float:
    # Only arithmetic is evaluated, never names, calls or attributes, so an expression
    # from the LLM can't run code.
    if isinstance(node, ast.Constant) and type(node.value) in (int, float):
        return node.value
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY:
        return _UNARY[type(node.op)](_evaluate(node.operand))
    if isinstance(node, ast.BinOp) and type(node.op) in _BINARY:
        left, right = _evaluate(node.left), _evaluate(node.right)
        if isinstance(node.op, ast.Pow) and abs(right) > 100:
            raise ToolError("exponents above 100 aren't allowed")
        try:
            return _BINARY[type(node.op)](left, right)
        except (ZeroDivisionError, OverflowError) as exc:
            raise ToolError(str(exc)) from None
    raise ToolError("only numbers, + - * / // % ** and parentheses are allowed")


HTTP_TIMEOUT_SECONDS = 10


@tool(effect="read_only")
def http_get(url: str, max_chars: Annotated[int, Field(ge=1, le=20_000)] = 2000) -> dict:
    """Fetch an http or https URL. Returns the status code and the start of the body."""
    # A demo: it fetches any URL the agent asks for, so don't give it to agents working
    # on goals from people you don't trust.
    if urllib.parse.urlsplit(url).scheme not in ("http", "https"):
        raise ToolError("only http and https URLs can be fetched")
    try:
        with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            status, body = resp.status, resp.read(max_chars * 4)
    except urllib.error.HTTPError as exc:  # an error status is still an answer
        status, body = exc.code, exc.read(max_chars * 4) if exc.fp else b""
    except (urllib.error.URLError, OSError) as exc:  # unreachable or timed out
        raise ToolError(f"request failed: {exc}") from None
    return {"status": status, "body": body.decode("utf-8", "replace")[:max_chars]}


class SinkEntry(Base):
    """One notification sent by send_notification: the demo's "outside world". Mirrors
    sql/schema.sql. The idempotency key deliberately isn't unique, so every execution
    is recorded and the chaos test can count duplicate side effects."""

    __tablename__ = "side_effect_sink"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(80))
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


def _deliver(idempotency_key: str, to: str, text: str) -> int:
    """The external call. Its own transaction, separate from the step's, as a real
    external system's would be."""
    with SessionLocal() as s:
        entry = SinkEntry(idempotency_key=idempotency_key, payload={"to": to, "text": text})
        s.add(entry)
        s.commit()
        return entry.id


def notification_sent(idempotency_key: str) -> dict | None:
    with SessionLocal() as s:
        entry_id = s.scalar(select(SinkEntry.id).where(SinkEntry.idempotency_key == idempotency_key)
                            .order_by(SinkEntry.id).limit(1))
    return None if entry_id is None else {"sent": True, "id": entry_id}


# Simulated latency of the notification service, in milliseconds. Half is spent before
# the write and half after, like a real call's request and response, so the chaos test
# can kill a worker between a call's intent and its effect, or between the effect and
# its result being recorded. 0 by default.
NOTIFICATION_LATENCY_MS = int(os.getenv("DEMO_NOTIFICATION_LATENCY_MS", "0"))


@tool(effect="side_effect", check=notification_sent)
def send_notification(to: str, text: str, *, idempotency_key: str) -> dict:
    """Send a notification. Each call sends one, so it is not safe to repeat."""
    time.sleep(NOTIFICATION_LATENCY_MS / 2000)
    entry_id = _deliver(idempotency_key, to, text)
    time.sleep(NOTIFICATION_LATENCY_MS / 2000)
    return {"sent": True, "id": entry_id}


@tool(effect="side_effect", check=notification_sent, requires_approval=True)
def publish_announcement(text: str, *, idempotency_key: str) -> dict:
    """Publish an announcement to everyone. A person must approve each one."""
    return {"published": True, "id": _deliver(idempotency_key, "everyone", text)}


# Made-up token counts.
scripted.script("demo-echo", [  # two parallel tool calls, then a final answer
    scripted.call_tools(("echo", {"text": "hello"}), ("echo", {"text": "world"}), input_tokens=50, output_tokens=20),
    scripted.answer("The tools said hello and world.", input_tokens=90, output_tokens=12),
])
scripted.script("demo-notify", [  # a read-only call, then a side-effecting one
    scripted.call_tools(("calculator", {"expression": "6 * 7"}), input_tokens=60, output_tokens=15),
    scripted.call_tools(("send_notification", {"to": "ops", "text": "The answer is 42."}),
                        input_tokens=110, output_tokens=25),
    scripted.answer("Sent ops the answer, 42.", input_tokens=150, output_tokens=10),
])
scripted.script("demo-approval", [  # a call that waits for a person's approval
    scripted.call_tools(("publish_announcement", {"text": "The queue is up."}), input_tokens=40, output_tokens=15),
    scripted.answer("Published the announcement.", input_tokens=80, output_tokens=8),
])

# For scripts/agent_chaos.py: four turns, each a simulated 0.3 s LLM call, with two
# notifications (side effects), a read-only call alongside the first, and one after.
CHAOS_TURN_SECONDS = 0.3
scripted.script("chaos-agent", [
    scripted.call_tools(("calculator", {"expression": "17 * 23"}), ("send_notification", {"to": "ops", "text": "started"}),
                        input_tokens=100, output_tokens=30, delay_seconds=CHAOS_TURN_SECONDS),
    scripted.call_tools(("echo", {"text": "checkpoint"}), input_tokens=150, output_tokens=20,
                        delay_seconds=CHAOS_TURN_SECONDS),
    scripted.call_tools(("send_notification", {"to": "ops", "text": "finished"}), input_tokens=200, output_tokens=25,
                        delay_seconds=CHAOS_TURN_SECONDS),
    scripted.answer("Notified ops twice.", input_tokens=250, output_tokens=10, delay_seconds=CHAOS_TURN_SECONDS),
])

# For scripts/agent_bench.py, with no latency, so a step's time is the runtime's own.
# bench-chain's steps run strictly one after another; bench-agent has a parallel turn.
scripted.script("bench-chain", [
    scripted.call_tools(("echo", {"text": "a"})),
    scripted.call_tools(("echo", {"text": "b"})),
    scripted.answer("done"),
])
scripted.script("bench-agent", [
    scripted.call_tools(("echo", {"text": "a"}), ("echo", {"text": "b"})),
    scripted.call_tools(("echo", {"text": "c"})),
    scripted.answer("done"),
])
