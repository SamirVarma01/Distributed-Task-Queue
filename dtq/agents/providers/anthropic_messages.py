"""Claude through Anthropic's Messages API (provider "anthropic").

Each assistant turn is recorded exactly as the API returned it (provider_content) and
sent back unchanged in later prompts. That keeps thinking blocks valid: the runtime
rebuilds each prompt from recorded steps without editing earlier turns, which is what
the API's checks on thinking blocks require.

Models that support it run with server-side refusal fallback (fallbacks="default"): if
the model's safety classifiers decline, the API re-runs the request on the model
Anthropic recommends for that refusal category, in the same call."""

import json
import os
from typing import Any

import anthropic

from ..llm import LLMRequest, LLMResponse, ProviderError, ToolCall

FALLBACK_MODELS = {"claude-fable-5-1", "claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5"}
FALLBACK_BETA = "server-side-fallback-2026-07-01"

# Anthropic's first-party API rates in US dollars per million tokens (input, output), as
# published on 2026-10-06. Claude Haiku 5.5 is left out because its rate changes for
# prompts over 100K tokens, which a flat price can't express.
PRICES = {
    "claude-fable-5-1": (10.0, 50.0),
    "claude-opus-5-5": (4.0, 20.0),
    "claude-opus-5": (5.0, 25.0),
    "claude-opus-4-8": (5.0, 25.0),
    "claude-sonnet-5-5": (2.0, 10.0),
    "claude-sonnet-5": (2.0, 10.0),
}


class AnthropicProvider:
    def __init__(self, client: Any = None):
        self._client = client

    @property
    def client(self):
        if self._client is None:  # the SDK reads ANTHROPIC_API_KEY (or another configured credential)
            timeout = os.getenv("LLM_TIMEOUT_SECONDS")
            self._client = anthropic.Anthropic(**({"timeout": float(timeout)} if timeout else {}))
        return self._client

    def complete(self, request: LLMRequest) -> LLMResponse:
        params = to_anthropic(request)
        if request.model in FALLBACK_MODELS:
            params.update(betas=[FALLBACK_BETA], fallbacks="default")
        try:
            message = self.client.beta.messages.create(**params)
        except (anthropic.RateLimitError, anthropic.InternalServerError, anthropic.OverloadedError,
                anthropic.APIConnectionError) as exc:  # 429, 5xx, 529, network and timeouts
            raise ProviderError(f"{type(exc).__name__}: {exc}", retryable=True) from exc
        except anthropic.APIStatusError as exc:  # other 4xx: retrying the same request won't help
            raise ProviderError(f"{type(exc).__name__}: {exc}", retryable=exc.status_code in (408, 409)) from exc
        return from_anthropic(message.to_dict(exclude_none=True))


def to_anthropic(request: LLMRequest) -> dict[str, Any]:
    """The Messages API request for a provider-neutral one."""
    messages: list[dict[str, Any]] = []
    for m in request.messages:
        if m["role"] == "user":
            messages.append({"role": "user", "content": m["content"]})
        elif m["role"] == "assistant":
            messages.append({"role": "assistant", "content": _assistant_content(m)})
        else:
            result = {"type": "tool_result", "tool_use_id": m["tool_call_id"], "content": _as_text(m["content"]),
                      "is_error": m["is_error"]}
            # All of a turn's results go back in one user message.
            if messages[-1]["role"] == "user" and isinstance(messages[-1]["content"], list):
                messages[-1]["content"].append(result)
            else:
                messages.append({"role": "user", "content": [result]})
    params: dict[str, Any] = {"model": request.model, "max_tokens": request.max_output_tokens, "messages": messages}
    if request.system:
        params["system"] = request.system
    if request.tools:
        params["tools"] = [{"name": t.name, "input_schema": t.parameters, **({"description": t.description}
                                                                            if t.description else {})}
                           for t in request.tools]
    if request.effort:
        params["output_config"] = {"effort": request.effort}
    return params


def _assistant_content(message: dict[str, Any]) -> list[dict[str, Any]]:
    recorded = message.get("provider_content")
    if recorded and recorded.get("provider") == "anthropic":
        return recorded["content"]  # exactly as returned, thinking blocks included
    content = [{"type": "text", "text": message["content"]}] if message["content"] else []
    return content + [{"type": "tool_use", "id": c["id"], "name": c["name"], "input": c["args"]}
                      for c in message["tool_calls"]]


def _as_text(content: Any) -> str:
    # Sorted keys, so the same result is always sent as the same text.
    return content if isinstance(content, str) else json.dumps(content, sort_keys=True)


def from_anthropic(message: dict[str, Any]) -> LLMResponse:
    """A provider-neutral response from a Messages API response (as a dict). A refusal,
    or a response cut off at max_tokens, raises a non-retryable ProviderError that
    carries the usage it was still billed for."""
    usage = _usage_by_model(message)
    stop = message.get("stop_reason")
    if stop == "refusal":
        category = (message.get("stop_details") or {}).get("category")
        raise ProviderError(f"{message['model']} declined the request (refusal"
                            + (f", category {category})" if category else ")"), retryable=False, usage_by_model=usage)
    if stop == "max_tokens":
        raise ProviderError("the response was cut off at max_output_tokens; raise it in the run's model config",
                            retryable=False, usage_by_model=usage)
    blocks = message["content"]
    text = "".join(b["text"] for b in blocks if b["type"] == "text") or None
    calls = [ToolCall(b["id"], b["name"], b["input"]) for b in blocks if b["type"] == "tool_use"]
    return LLMResponse(text=text, tool_calls=calls, stop_reason=stop or "end_turn",
                       input_tokens=sum(u[1] for u in usage), output_tokens=sum(u[2] for u in usage),
                       usage_by_model=usage, provider_content={"provider": "anthropic", "content": blocks})


def _usage_by_model(message: dict[str, Any]) -> list[tuple[str, int, int]]:
    """Tokens by the model that used them. With a fallback, usage.iterations has one
    entry per attempt, and is the source of truth: top-level usage covers only the
    attempt that produced the returned message."""
    usage = message["usage"]
    attempts = [i for i in usage.get("iterations") or [] if i.get("type") in ("message", "fallback_message")]
    if not attempts:
        return [(message["model"], usage["input_tokens"], usage["output_tokens"])]
    return [(i.get("model") or message["model"], i["input_tokens"], i["output_tokens"]) for i in attempts]
