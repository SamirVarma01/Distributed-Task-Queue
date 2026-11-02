"""OpenAI models through the Chat Completions API (provider "openai").

No OpenAI model has a price in llm.PRICES, so these runs record a cost of 0 and can't
take a cost budget: add a model's price there to enable both."""

import json
import os
from typing import Any

import openai

from ..llm import LLMRequest, LLMResponse, ProviderError, ToolCall


class OpenAIProvider:
    def __init__(self, client: Any = None):
        self._client = client

    @property
    def client(self):
        if self._client is None:  # the SDK reads OPENAI_API_KEY
            timeout = os.getenv("LLM_TIMEOUT_SECONDS")
            self._client = openai.OpenAI(**({"timeout": float(timeout)} if timeout else {}))
        return self._client

    def complete(self, request: LLMRequest) -> LLMResponse:
        try:
            completion = self.client.chat.completions.create(**to_openai(request))
        except (openai.RateLimitError, openai.InternalServerError, openai.APIConnectionError) as exc:
            raise ProviderError(f"{type(exc).__name__}: {exc}", retryable=True) from exc
        except openai.APIStatusError as exc:  # other 4xx: retrying the same request won't help
            raise ProviderError(f"{type(exc).__name__}: {exc}", retryable=exc.status_code in (408, 409)) from exc
        return from_openai(completion.to_dict(exclude_none=True))


def to_openai(request: LLMRequest) -> dict[str, Any]:
    """The Chat Completions request for a provider-neutral one."""
    messages: list[dict[str, Any]] = [{"role": "system", "content": request.system}] if request.system else []
    for m in request.messages:
        if m["role"] == "user":
            messages.append({"role": "user", "content": m["content"]})
        elif m["role"] == "assistant":
            assistant: dict[str, Any] = {"role": "assistant", "content": m["content"]}
            if m["tool_calls"]:
                assistant["tool_calls"] = [{"id": c["id"], "type": "function", "function": {
                    "name": c["name"], "arguments": json.dumps(c["args"], sort_keys=True)}} for c in m["tool_calls"]]
            messages.append(assistant)
        else:  # Chat Completions has no error flag on a tool result, so say it in the text
            text = m["content"] if isinstance(m["content"], str) else json.dumps(m["content"], sort_keys=True)
            messages.append({"role": "tool", "tool_call_id": m["tool_call_id"],
                             "content": f"Error: {text}" if m["is_error"] else text})
    params: dict[str, Any] = {"model": request.model, "messages": messages,
                              "max_completion_tokens": request.max_output_tokens}
    if request.tools:
        params["tools"] = [{"type": "function", "function": {"name": t.name, "description": t.description,
                                                             "parameters": t.parameters}} for t in request.tools]
    if request.effort:
        params["reasoning_effort"] = request.effort
    return params


def from_openai(completion: dict[str, Any]) -> LLMResponse:
    """A provider-neutral response from a Chat Completions response (as a dict). A
    refusal, a content-filter stop, or a response cut off at its limit raises a
    non-retryable ProviderError that carries the usage it was still billed for."""
    usage = completion.get("usage") or {}
    by_model = [(completion["model"], usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0))]
    choice = completion["choices"][0]
    message, finish = choice["message"], choice.get("finish_reason")
    if message.get("refusal"):
        raise ProviderError(f"{completion['model']} declined the request: {message['refusal']}",
                            retryable=False, usage_by_model=by_model)
    if finish == "content_filter":
        raise ProviderError("the response was stopped by the content filter", retryable=False, usage_by_model=by_model)
    if finish == "length":
        raise ProviderError("the response was cut off at max_output_tokens; raise it in the run's model config",
                            retryable=False, usage_by_model=by_model)
    calls = [ToolCall(c["id"], c["function"]["name"], _arguments(c["function"].get("arguments", "")))
             for c in message.get("tool_calls") or [] if c.get("type") == "function"]
    return LLMResponse(text=message.get("content") or None, tool_calls=calls, stop_reason=finish or "stop",
                       input_tokens=by_model[0][1], output_tokens=by_model[0][2], usage_by_model=by_model)


def _arguments(raw: str) -> dict[str, Any]:
    """The call's arguments. Text that isn't a JSON object is passed on under a key no
    tool takes, so argument validation rejects it and the error goes back to the model."""
    try:
        args = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return {"unparseable_arguments": raw}
    return args if isinstance(args, dict) else {"unparseable_arguments": raw}
