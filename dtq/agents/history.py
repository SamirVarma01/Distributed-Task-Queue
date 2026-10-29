"""Rebuild an LLM step's prompt from a run's recorded steps (docs/agent-runtime-design.md,
section 6). Recorded outputs are the only copy ever used, so a re-executed LLM step
sends exactly the prompt the first attempt sent."""

import hashlib
import json
from collections.abc import Iterable
from typing import Any

from .models import AgentStep, StepKind, StepStatus

FINISHED = (StepStatus.SUCCEEDED, StepStatus.FAILED)


def call_results(steps: Iterable[AgentStep], turn: int) -> dict[str, tuple[Any, bool]]:
    """{call_id: (output, is_error)} for the turn's tool calls that have a result. A
    later step for the same call (a retry) supersedes an earlier one."""
    results = {}
    for step in sorted(steps, key=lambda s: s.step_no):
        if step.kind == StepKind.TOOL and step.turn == turn and step.status in FINISHED:
            results[step.call_id] = (step.output, step.status == StepStatus.FAILED)
    return results


def build_messages(goal: str, steps: Iterable[AgentStep]) -> list[dict[str, Any]]:
    """The goal, then for each finished turn the LLM's response and one result per
    tool call. Results follow the order the LLM listed the calls in, not the order
    they finished, so the prompt is the same whichever call finished first."""
    steps = list(steps)
    messages: list[dict[str, Any]] = [{"role": "user", "content": goal}]
    for llm in sorted((s for s in steps if s.kind == StepKind.LLM and s.status == StepStatus.SUCCEEDED),
                      key=lambda s: s.step_no):
        messages.append({"role": "assistant", "content": llm.output["text"], "tool_calls": llm.output["tool_calls"]})
        results = call_results(steps, llm.step_no)
        for call in llm.output["tool_calls"]:
            if call["id"] in results:
                output, is_error = results[call["id"]]
                messages.append({"role": "tool", "tool_call_id": call["id"], "content": output, "is_error": is_error})
    return messages


def prompt_sha256(system: str, messages: list[dict[str, Any]]) -> str:
    canonical = json.dumps({"system": system, "messages": messages}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()
