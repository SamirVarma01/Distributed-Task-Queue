"""The OpenAI adapter, without network access: request and response mapping, and how
SDK errors are classified for DTQ's retries."""

import httpx2
import openai
import pytest

from dtq.agents import tools
from dtq.agents.llm import LLMRequest, ProviderError, ToolCall, ToolSpec
from dtq.agents.providers.openai_chat import OpenAIProvider, from_openai, to_openai

SCHEMA = {"type": "object", "properties": {"expression": {"type": "string"}}, "required": ["expression"]}


def request(messages, **kw) -> LLMRequest:
    return LLMRequest(model="gpt-test", system=kw.pop("system", "Be brief."), messages=messages,
                      tools=kw.pop("tools", [ToolSpec("calculator", "Do arithmetic.", SCHEMA)]),
                      max_output_tokens=16000, idempotency_key="run:0", **kw)


def test_request_mapping():
    params = to_openai(request([
        {"role": "user", "content": "goal"},
        {"role": "assistant", "content": None, "tool_calls": [
            {"id": "call_1", "name": "calculator", "args": {"expression": "6*7"}}]},
        {"role": "tool", "tool_call_id": "call_1", "content": 42, "is_error": False},
        {"role": "assistant", "content": "Retrying.", "tool_calls": [{"id": "call_2", "name": "x", "args": {}}]},
        {"role": "tool", "tool_call_id": "call_2", "content": {"error": "unknown tool"}, "is_error": True},
    ], effort="low"))

    assert (params["model"], params["max_completion_tokens"], params["reasoning_effort"]) == ("gpt-test", 16000, "low")
    assert params["tools"] == [{"type": "function", "function": {"name": "calculator", "description": "Do arithmetic.",
                                                                 "parameters": SCHEMA}}]
    assert params["messages"] == [
        {"role": "system", "content": "Be brief."},
        {"role": "user", "content": "goal"},
        {"role": "assistant", "content": None, "tool_calls": [
            {"id": "call_1", "type": "function", "function": {"name": "calculator",
                                                              "arguments": '{"expression": "6*7"}'}}]},
        {"role": "tool", "tool_call_id": "call_1", "content": "42"},
        {"role": "assistant", "content": "Retrying.", "tool_calls": [
            {"id": "call_2", "type": "function", "function": {"name": "x", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "call_2", "content": 'Error: {"error": "unknown tool"}'},
    ]


def test_a_request_without_system_prompt_tools_or_effort():
    params = to_openai(request([{"role": "user", "content": "goal"}], system="", tools=[]))

    assert params["messages"] == [{"role": "user", "content": "goal"}]
    assert "tools" not in params and "reasoning_effort" not in params


def completion(message, finish="stop"):
    return {"id": "c1", "model": "gpt-test", "choices": [{"index": 0, "finish_reason": finish, "message": message}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 7, "total_tokens": 57}}


def test_response_mapping():
    response = from_openai(completion({"role": "assistant", "content": None, "tool_calls": [
        {"id": "call_1", "type": "function", "function": {"name": "calculator", "arguments": '{"expression": "6*7"}'}},
        {"id": "call_2", "type": "function", "function": {"name": "calculator", "arguments": "{not json"}},
    ]}, "tool_calls"))

    assert response.text is None and response.stop_reason == "tool_calls"
    assert response.tool_calls == [ToolCall("call_1", "calculator", {"expression": "6*7"}),
                                   ToolCall("call_2", "calculator", {"unparseable_arguments": "{not json"})]
    assert (response.input_tokens, response.output_tokens, response.usage_by_model) == (50, 7, [("gpt-test", 50, 7)])
    assert response.provider_content is None


def test_unparseable_arguments_are_rejected_by_argument_validation():
    @tools.tool(effect="read_only")
    def evaluate(expression: str) -> str:
        """Evaluate."""
        return expression

    with pytest.raises(TypeError, match="unparseable_arguments"):
        tools.bind_args(tools.get("evaluate"), {"unparseable_arguments": "{not json"}, "run:1")


@pytest.mark.parametrize("message,finish,error", [
    ({"role": "assistant", "content": None, "refusal": "I can't help with that."}, "stop",
     "gpt-test declined the request: I can't help with that."),
    ({"role": "assistant", "content": "partial"}, "length", "cut off at max_output_tokens"),
    ({"role": "assistant", "content": ""}, "content_filter", "stopped by the content filter"),
])
def test_refusal_filter_and_truncation_fail_for_good_but_keep_their_usage(message, finish, error):
    with pytest.raises(ProviderError, match=error) as exc:
        from_openai(completion(message, finish))
    assert exc.value.retryable is False and exc.value.usage_by_model == [("gpt-test", 50, 7)]


class FakeClient:
    def __init__(self, outcome):
        self.outcome = outcome
        self.chat = type("Chat", (), {})()
        self.chat.completions = self

    def create(self, **params):
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


class Reply:
    def __init__(self, data):
        self.data = data

    def to_dict(self, exclude_none=False):
        return self.data


def test_complete_returns_the_mapped_response():
    reply = Reply(completion({"role": "assistant", "content": "done"}))
    assert OpenAIProvider(FakeClient(reply)).complete(request([{"role": "user", "content": "hi"}])).text == "done"


REQ = httpx2.Request("POST", "https://api.openai.com/v1/chat/completions")


def status_error(cls, code):
    return cls(f"status {code}", response=httpx2.Response(code, request=REQ), body=None)


@pytest.mark.parametrize("error,retryable", [
    (status_error(openai.RateLimitError, 429), True),
    (status_error(openai.InternalServerError, 503), True),
    (openai.APIConnectionError(request=REQ), True),
    (openai.APITimeoutError(request=REQ), True),
    (status_error(openai.APIStatusError, 409), True),
    (status_error(openai.BadRequestError, 400), False),
    (status_error(openai.AuthenticationError, 401), False),
    (status_error(openai.NotFoundError, 404), False),
], ids=lambda v: type(v).__name__ if isinstance(v, Exception) else str(v))
def test_errors_are_classified_for_retries(error, retryable):
    with pytest.raises(ProviderError) as exc:
        OpenAIProvider(FakeClient(error)).complete(request([{"role": "user", "content": "hi"}]))
    assert exc.value.retryable is retryable
