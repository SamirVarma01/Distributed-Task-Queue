import random

from dtq.agents.history import build_messages, prompt_sha256
from dtq.agents.models import AgentStep, StepKind, StepStatus

CALLS = [{"id": "call_0_0", "name": "lookup", "args": {"key": "a"}},
         {"id": "call_0_1", "name": "lookup", "args": {"key": "b"}}]


def llm(step_no, output=None, status=StepStatus.SUCCEEDED):
    return AgentStep(step_no=step_no, turn=step_no, kind=StepKind.LLM, status=status, output=output)


def tool(step_no, turn, call_id, output, status=StepStatus.SUCCEEDED):
    return AgentStep(step_no=step_no, turn=turn, kind=StepKind.TOOL, status=status, call_id=call_id, output=output)


def finished_run():
    return [
        llm(0, {"text": None, "tool_calls": CALLS}),
        tool(1, 0, "call_0_0", "A"),
        tool(2, 0, "call_0_1", {"error": "not found"}, StepStatus.FAILED),
        llm(3, {"text": "done", "tool_calls": []}),
    ]


def test_messages_from_a_finished_run():
    assert build_messages("find a and b", finished_run()) == [
        {"role": "user", "content": "find a and b"},
        {"role": "assistant", "content": None, "tool_calls": CALLS},
        {"role": "tool", "tool_call_id": "call_0_0", "content": "A", "is_error": False},
        {"role": "tool", "tool_call_id": "call_0_1", "content": {"error": "not found"}, "is_error": True},
        {"role": "assistant", "content": "done", "tool_calls": []},
    ]


def test_prompt_does_not_depend_on_the_order_steps_are_read_or_finished():
    expected = build_messages("g", finished_run())
    for seed in range(5):
        steps = finished_run()
        random.Random(seed).shuffle(steps)
        assert build_messages("g", steps) == expected


def test_unfinished_steps_are_left_out():
    steps = [
        llm(0, {"text": None, "tool_calls": CALLS}),
        tool(1, 0, "call_0_0", None, StepStatus.STARTED),
        tool(2, 0, "call_0_1", "B"),
        llm(3, status=StepStatus.STARTED),  # the step being built
    ]
    assert build_messages("g", steps)[2:] == [
        {"role": "tool", "tool_call_id": "call_0_1", "content": "B", "is_error": False}]


def test_a_retried_call_uses_its_latest_result():
    steps = [
        llm(0, {"text": None, "tool_calls": CALLS[:1]}),
        tool(1, 0, "call_0_0", {"error": "timeout"}, StepStatus.FAILED),
        tool(2, 0, "call_0_0", "A"),
    ]
    assert build_messages("g", steps)[2:] == [
        {"role": "tool", "tool_call_id": "call_0_0", "content": "A", "is_error": False}]


def test_prompt_hash_covers_system_prompt_and_messages():
    messages = build_messages("g", finished_run())

    assert prompt_sha256("sys", messages) == prompt_sha256("sys", build_messages("g", finished_run()))
    assert prompt_sha256("other", messages) != prompt_sha256("sys", messages)
    assert prompt_sha256("sys", messages[:-1]) != prompt_sha256("sys", messages)
