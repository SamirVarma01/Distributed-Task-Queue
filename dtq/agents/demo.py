"""Example tools and ScriptedLLM scripts, registered in the API and workers like the
example tasks in dtq/tasks.py."""

from . import scripted
from .tools import tool


@tool(effect="read_only")
def echo(text: str) -> str:
    """Return the text unchanged."""
    return text


# Two parallel tool calls, then a final answer. Token counts are made up.
scripted.script("demo-echo", [
    scripted.call_tools(("echo", {"text": "hello"}), ("echo", {"text": "world"}), input_tokens=50, output_tokens=20),
    scripted.answer("The tools said hello and world.", input_tokens=90, output_tokens=12),
])
