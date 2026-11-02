import pytest

from dtq.agents import llm, scripted
from dtq.agents.llm import LLMRequest, ProviderError, ToolCall

scripted.script("llm-test", [
    scripted.call_tools(("echo", {"text": "a"}), ("echo", {"text": "b"}), input_tokens=10, output_tokens=5),
    scripted.answer("done", input_tokens=20, output_tokens=3, delay_seconds=0.25),
])
scripted.script("llm-test-errors", [scripted.fail("rate limited", retryable=True)])


def request(model: str, turns_so_far: int) -> LLMRequest:
    messages = [{"role": "user", "content": "goal"}]
    for _ in range(turns_so_far):
        messages += [{"role": "assistant", "content": None, "tool_calls": []}]
    return LLMRequest(model, "", messages, [], 100, "run:0")


def test_response_is_picked_by_turns_in_history_not_by_a_counter():
    provider = llm.get_provider("scripted")

    first = provider.complete(request("llm-test", 0))
    again = provider.complete(request("llm-test", 0))  # a re-executed step

    assert first == again
    assert first.tool_calls == [ToolCall("call_0_0", "echo", {"text": "a"}),
                                ToolCall("call_0_1", "echo", {"text": "b"})]
    assert (first.input_tokens, first.output_tokens, first.stop_reason) == (10, 5, "tool_use")


def test_scripted_delay(monkeypatch):
    slept = []
    monkeypatch.setattr(scripted.time, "sleep", slept.append)

    response = llm.get_provider("scripted").complete(request("llm-test", 1))

    assert response.text == "done"
    assert slept == [0.25]


@pytest.mark.parametrize("model,turns,message,retryable", [
    ("llm-test-errors", 0, "rate limited", True),
    ("llm-test", 2, "has no response for turn 2", False),
    ("no-such-script", 0, "no script named", False),
])
def test_errors(model, turns, message, retryable):
    with pytest.raises(ProviderError, match=message) as exc:
        llm.get_provider("scripted").complete(request(model, turns))
    assert exc.value.retryable is retryable


def test_response_as_step_output():
    response = llm.get_provider("scripted").complete(request("llm-test", 0))

    assert response.to_output() == {
        "text": None,
        "tool_calls": [{"id": "call_0_0", "name": "echo", "args": {"text": "a"}},
                       {"id": "call_0_1", "name": "echo", "args": {"text": "b"}}],
        "stop_reason": "tool_use",
    }


def test_cost_in_micro_dollars(monkeypatch):
    monkeypatch.setitem(llm.PRICES, "priced", (3.0, 15.0))
    monkeypatch.setitem(llm.PRICES, "cheap", (0.25, 0.25))

    assert llm.cost_micros("priced", 1000, 100) == 3000 + 1500
    assert llm.cost_micros("cheap", 3, 0) == 1  # 0.75 micro-dollars rounds up
    assert llm.cost_micros("unpriced", 1000, 1000) == 0
