"""Run budgets (docs/agent-runtime-design.md, section 9). They are checked before each
LLM call against the usage recorded so far, so a run can overshoot by one call."""

from datetime import datetime

from .models import AgentRun


def wall_seconds(run: AgentRun, now: datetime) -> float:
    """Time since the run started, not counting time spent awaiting approval."""
    if run.started_at is None:
        return 0.0
    paused = run.paused_seconds + ((now - run.paused_at).total_seconds() if run.paused_at else 0)
    return (now - run.started_at).total_seconds() - paused


def exceeded(run: AgentRun, now: datetime) -> str | None:
    """The first budget the run has used up, described, or None."""
    if run.llm_calls >= run.max_steps:
        return f"max_steps: {run.llm_calls} of {run.max_steps} LLM calls made"
    used = run.input_tokens + run.output_tokens
    if run.max_tokens is not None and used >= run.max_tokens:
        return f"max_tokens: {used} of {run.max_tokens} tokens used"
    if run.max_cost_micros is not None and run.cost_micros >= run.max_cost_micros:
        return f"max_cost_micros: {run.cost_micros} of {run.max_cost_micros} micro-dollars spent"
    if run.max_wall_seconds is not None and (elapsed := wall_seconds(run, now)) >= run.max_wall_seconds:
        return f"max_wall_seconds: {elapsed:.0f} of {run.max_wall_seconds} seconds used"
    return None


def max_output_tokens(run: AgentRun) -> int:
    """The next call's output limit: the model's, capped at the tokens left in the
    budget. This bounds the call's output, but not its input."""
    limit = run.model["max_output_tokens"]
    if run.max_tokens is not None:
        limit = min(limit, run.max_tokens - run.input_tokens - run.output_tokens)
    return limit
