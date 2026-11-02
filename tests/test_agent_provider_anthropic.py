"""The Anthropic adapter, without network access: request and response mapping, and how
SDK errors are classified for DTQ's retries."""

import anthropic
import httpx2
import pytest

from dtq.agents.llm import LLMRequest, ProviderError, ToolCall, ToolSpec
from dtq.agents.providers.anthropic_messages import AnthropicProvider, from_anthropic, to_anthropic

SCHEMA = {"type": "object", "properties": {"expression": {"type": "string"}}, "required": ["expression"],
          "additionalProperties": False}
RECORDED = {"provider": "anthropic", "content": [
    {"type": "thinking", "thinking": "", "signature": "sig-1"},
    {"type": "tool_use", "id": "toolu_2", "name": "calculator", "input": {"expression": "2+2"}},
]}


def request(messages, model="claude-opus-5-5", **kw) -> LLMRequest:
    return LLMRequest(model=model, system=kw.pop("system", "Be brief."), messages=messages,
                      tools=kw.pop("tools", [ToolSpec("calculator", "Do arithmetic.", SCHEMA)]),
                      max_output_tokens=16000, idempotency_key="run:0", **kw)


def test_request_mapping():
    params = to_anthropic(request([
        {"role": "user", "content": "goal"},
        {"role": "assistant", "content": "Let me check.", "tool_calls": [
            {"id": "toolu_1", "name": "calculator", "args": {"expression": "6*7"}},
            {"id": "toolu_x", "name": "nope", "args": {}}]},
        {"role": "tool", "tool_call_id": "toolu_1", "content": 42, "is_error": False},
        {"role": "tool", "tool_call_id": "toolu_x", "content": {"error": "unknown tool", "b": 1}, "is_error": True},
        {"role": "assistant", "content": None, "tool_calls": [], "provider_content": RECORDED},
        {"role": "tool", "tool_call_id": "toolu_2", "content": "4", "is_error": False},
    ], effort="high"))

    assert params["model"] == "claude-opus-5-5" and params["max_tokens"] == 16000
    assert params["system"] == "Be brief."
    assert params["output_config"] == {"effort": "high"}
    assert params["tools"] == [{"name": "calculator", "description": "Do arithmetic.", "input_schema": SCHEMA}]
    assert params["messages"] == [
        {"role": "user", "content": "goal"},
        {"role": "assistant", "content": [
            {"type": "text", "text": "Let me check."},
            {"type": "tool_use", "id": "toolu_1", "name": "calculator", "input": {"expression": "6*7"}},
            {"type": "tool_use", "id": "toolu_x", "name": "nope", "input": {}}]},
        # both results in one user message; non-text results as JSON with sorted keys
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "42", "is_error": False},
            {"type": "tool_result", "tool_use_id": "toolu_x", "content": '{"b": 1, "error": "unknown tool"}',
             "is_error": True}]},
        {"role": "assistant", "content": RECORDED["content"]},  # replayed exactly, thinking block included
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_2", "content": "4",
                                      "is_error": False}]},
    ]


def test_another_providers_record_is_not_replayed_to_anthropic():
    params = to_anthropic(request([
        {"role": "user", "content": "goal"},
        {"role": "assistant", "content": "done", "tool_calls": [], "provider_content": {"provider": "openai", "content": "x"}},
    ], system="", tools=[]))

    assert params["messages"][1] == {"role": "assistant", "content": [{"type": "text", "text": "done"}]}
    assert "system" not in params and "tools" not in params and "output_config" not in params


def message(content, stop_reason="end_turn", **usage):
    return {"id": "msg_1", "type": "message", "role": "assistant", "model": "claude-opus-5-5", "content": content,
            "stop_reason": stop_reason, "usage": {"input_tokens": 100, "output_tokens": 20, **usage}}


def test_response_mapping():
    content = [{"type": "thinking", "thinking": "", "signature": "sig"},
               {"type": "text", "text": "Checking "}, {"type": "text", "text": "now."},
               {"type": "tool_use", "id": "toolu_1", "name": "calculator", "input": {"expression": "6*7"}}]
    response = from_anthropic(message(content, "tool_use"))

    assert response.text == "Checking now."
    assert response.tool_calls == [ToolCall("toolu_1", "calculator", {"expression": "6*7"})]
    assert (response.stop_reason, response.input_tokens, response.output_tokens) == ("tool_use", 100, 20)
    assert response.usage_by_model == [("claude-opus-5-5", 100, 20)]
    assert response.provider_content == {"provider": "anthropic", "content": content}


def test_a_fallback_attempt_is_counted_against_its_own_model():
    fallback = {"type": "fallback", "from": {"model": "claude-opus-5-5"}, "to": {"model": "claude-opus-4-8"},
                "trigger": {"type": "refusal", "category": "cyber"}}
    response = from_anthropic({**message([fallback, {"type": "text", "text": "ok"}]), "model": "claude-opus-4-8",
                               "usage": {"input_tokens": 120, "output_tokens": 30, "iterations": [
                                   {"type": "message", "model": "claude-opus-5-5", "input_tokens": 120,
                                    "output_tokens": 0},
                                   {"type": "fallback_message", "model": "claude-opus-4-8", "input_tokens": 120,
                                    "output_tokens": 30}]}})

    assert response.usage_by_model == [("claude-opus-5-5", 120, 0), ("claude-opus-4-8", 120, 30)]
    assert (response.input_tokens, response.output_tokens) == (240, 30)
    assert response.provider_content["content"][0] == fallback  # kept in the turn's record


def test_refusal_and_truncation_fail_for_good_but_keep_their_usage():
    refusal = {**message([], "refusal"), "stop_details": {"type": "refusal", "category": "cyber"}}
    with pytest.raises(ProviderError, match=r"claude-opus-5-5 declined the request \(refusal, category cyber\)") as exc:
        from_anthropic(refusal)
    assert exc.value.retryable is False and exc.value.usage_by_model == [("claude-opus-5-5", 100, 20)]

    with pytest.raises(ProviderError, match="cut off at max_output_tokens") as exc:
        from_anthropic(message([{"type": "text", "text": "partial"}], "max_tokens"))
    assert exc.value.retryable is False and exc.value.usage_by_model == [("claude-opus-5-5", 100, 20)]


class FakeMessages:
    def __init__(self, outcome):
        self.outcome, self.calls = outcome, []

    def create(self, **params):
        self.calls.append(params)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


class FakeClient:
    def __init__(self, outcome):
        self.beta = type("Beta", (), {})()
        self.beta.messages = FakeMessages(outcome)


class Reply:
    def __init__(self, data):
        self.data = data

    def to_dict(self, exclude_none=False):
        return self.data


@pytest.mark.parametrize("model,fallback", [("claude-opus-5-5", True), ("claude-sonnet-5-5", True),
                                            ("claude-haiku-5-5", False)])
def test_server_side_fallback_is_requested_for_models_that_support_it(model, fallback):
    client = FakeClient(Reply(message([{"type": "text", "text": "hi"}])))
    AnthropicProvider(client).complete(request([{"role": "user", "content": "hi"}], model=model))

    [params] = client.beta.messages.calls
    assert ("fallbacks" in params, params.get("fallbacks"), params.get("betas")) == (
        (True, "default", ["server-side-fallback-2026-07-01"]) if fallback else (False, None, None))


REQ = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")


def status_error(cls, code):
    return cls(f"status {code}", response=httpx2.Response(code, request=REQ), body=None)


@pytest.mark.parametrize("error,retryable", [
    (status_error(anthropic.RateLimitError, 429), True),
    (status_error(anthropic.InternalServerError, 500), True),
    (status_error(anthropic.OverloadedError, 529), True),
    (anthropic.APIConnectionError(request=REQ), True),
    (anthropic.APITimeoutError(request=REQ), True),
    (status_error(anthropic.APIStatusError, 408), True),
    (status_error(anthropic.BadRequestError, 400), False),
    (status_error(anthropic.AuthenticationError, 401), False),
    (status_error(anthropic.NotFoundError, 404), False),
], ids=lambda v: type(v).__name__ if isinstance(v, Exception) else str(v))
def test_errors_are_classified_for_retries(error, retryable):
    provider = AnthropicProvider(FakeClient(error))

    with pytest.raises(ProviderError) as exc:
        provider.complete(request([{"role": "user", "content": "hi"}]))
    assert exc.value.retryable is retryable
    assert str(exc.value).startswith(type(error).__name__)
