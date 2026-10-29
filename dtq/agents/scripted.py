"""A deterministic LLM for tests and benchmarks, registered as provider "scripted".

A script is the list of responses for one run, one per turn; a run selects it with
{"provider": "scripted", "name": "<script>"}. The response for a request is picked by
how many turns its history already contains, not by a counter, so a step re-executed
in another worker process gets the same response."""

import time
from dataclasses import dataclass

from .llm import LLMRequest, LLMResponse, ProviderError, ToolCall, register_provider


@dataclass(frozen=True)
class Turn:
    response: LLMResponse | ProviderError  # an error is raised instead of returned
    delay_seconds: float = 0  # simulated latency


_SCRIPTS: dict[str, list[Turn]] = {}


def answer(text: str, *, input_tokens: int = 0, output_tokens: int = 0, delay_seconds: float = 0) -> Turn:
    """A final answer: the run ends."""
    return Turn(LLMResponse(text=text, input_tokens=input_tokens, output_tokens=output_tokens), delay_seconds)


def call_tools(*calls: tuple[str, dict], input_tokens: int = 0, output_tokens: int = 0,
               delay_seconds: float = 0) -> Turn:
    """Tool calls, as (tool name, args) pairs. script() assigns their ids."""
    tool_calls = [ToolCall("", name, args) for name, args in calls]
    return Turn(LLMResponse(tool_calls=tool_calls, stop_reason="tool_use",
                            input_tokens=input_tokens, output_tokens=output_tokens), delay_seconds)


def fail(message: str, *, retryable: bool, delay_seconds: float = 0) -> Turn:
    return Turn(ProviderError(message, retryable=retryable), delay_seconds)


def script(name: str, turns: list[Turn]) -> None:
    """Register a script. Tool call ids are "call_<turn>_<index>"."""
    numbered = []
    for t, turn in enumerate(turns):
        if isinstance(turn.response, LLMResponse) and turn.response.tool_calls:
            calls = [ToolCall(f"call_{t}_{i}", c.name, c.args) for i, c in enumerate(turn.response.tool_calls)]
            turn = Turn(LLMResponse(turn.response.text, calls, turn.response.stop_reason,
                                    turn.response.input_tokens, turn.response.output_tokens), turn.delay_seconds)
        numbered.append(turn)
    _SCRIPTS[name] = numbered


class ScriptedLLM:
    def complete(self, request: LLMRequest) -> LLMResponse:
        turns = _SCRIPTS.get(request.model)
        if turns is None:
            raise ProviderError(f"no script named {request.model!r}", retryable=False)
        n = sum(1 for m in request.messages if m["role"] == "assistant")
        if n >= len(turns):
            raise ProviderError(f"script {request.model!r} has no response for turn {n}", retryable=False)
        turn = turns[n]
        if turn.delay_seconds:
            time.sleep(turn.delay_seconds)
        if isinstance(turn.response, ProviderError):
            raise turn.response
        return turn.response


register_provider("scripted", ScriptedLLM())
