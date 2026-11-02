"""Real agents on real providers: run with `pytest -m live`. Each test is skipped unless
its provider's credentials are set, because it spends (a little) money. Redis and
MySQL are still the test doubles; only the LLM is real."""

import os

import pytest

from dtq.agents.models import StepKind

from .agent_support import drain, get_run, get_steps

pytestmark = pytest.mark.live

PROVIDERS = [
    ("anthropic", os.getenv("ANTHROPIC_TEST_MODEL", "claude-opus-5-5"),
     os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_AUTH_TOKEN")),
    ("openai", os.getenv("OPENAI_TEST_MODEL"), os.getenv("OPENAI_API_KEY")),
]


@pytest.mark.parametrize("provider,model,credential", PROVIDERS, ids=[p[0] for p in PROVIDERS])
def test_a_real_agent_uses_a_tool_to_answer(client, make_worker, provider, model, credential):
    if not credential:
        pytest.skip(f"no {provider} credentials in the environment")
    if not model:
        pytest.skip(f"set {provider.upper()}_TEST_MODEL to the model to test")

    resp = client.post("/runs", json={
        "goal": "Use the calculator tool to compute 1234 * 5678. Reply with only the resulting number.",
        "model": {"provider": provider, "name": model}, "tools": ["calculator"], "max_steps": 5})
    assert resp.status_code == 202, resp.text
    drain(make_worker())

    run = get_run(resp.json()["id"])
    assert run.status.value == "succeeded", run.error
    assert "7006652" in run.result.replace(",", "")
    assert 7006652 in [s.output for s in get_steps(run.id) if s.kind == StepKind.TOOL]
    assert run.llm_calls >= 2 and run.input_tokens > 0 and run.output_tokens > 0
