"""The provider-neutral LLM interface (docs/agent-runtime-design.md, section 10).

Messages are plain JSON-serializable dicts, so they can be hashed and stored:
    {"role": "user", "content": str}
    {"role": "assistant", "content": str | None, "tool_calls": [{"id", "name", "args"}],
     "provider_content": {"provider": str, "content": ...}}  # provider_content is optional
    {"role": "tool", "tool_call_id": str, "content": <JSON>, "is_error": bool}
The system prompt goes in LLMRequest.system, not in the messages. provider_content is a
provider's own record of an assistant turn, for providers that must be sent it back
unchanged (Anthropic's thinking blocks)."""

import math
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    args: dict[str, Any]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]  # JSON schema of the tool's arguments


@dataclass(frozen=True)
class LLMRequest:
    model: str
    system: str
    messages: list[dict[str, Any]]
    tools: list[ToolSpec]
    max_output_tokens: int
    idempotency_key: str  # "<run_id>:<step_no>"
    effort: str | None = None  # passed to providers that support it; None means their default


@dataclass(frozen=True)
class LLMResponse:
    text: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    stop_reason: str = "end_turn"
    input_tokens: int = 0
    output_tokens: int = 0
    # Tokens by the model that used them, when a call used more than one model (a
    # server-side fallback) or the provider names the model that served it. None means
    # all of it was the requested model's.
    usage_by_model: list[tuple[str, int, int]] | None = None
    provider_content: dict[str, Any] | None = None  # see the module docstring

    def to_output(self) -> dict[str, Any]:
        """The response as an LLM step's recorded output (usage is stored separately)."""
        output = {"text": self.text, "tool_calls": [asdict(c) for c in self.tool_calls],
                  "stop_reason": self.stop_reason}
        if self.provider_content is not None:
            output["provider_content"] = self.provider_content
        return output


class ProviderError(Exception):
    """A failed LLM call. Retryable errors (rate limits, 5xx, timeouts) are retried
    with backoff by DTQ; any other fails the run. usage_by_model is what the failed
    call still used (and is billed for), such as a response cut off at its limit."""

    def __init__(self, message: str, *, retryable: bool, usage_by_model: list[tuple[str, int, int]] | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.usage_by_model = usage_by_model or []


class LLMProvider(Protocol):
    def complete(self, request: LLMRequest) -> LLMResponse: ...


_PROVIDERS: dict[str, LLMProvider] = {}


def register_provider(name: str, provider: LLMProvider) -> None:
    _PROVIDERS[name] = provider


def get_provider(name: str) -> LLMProvider | None:
    return _PROVIDERS.get(name)


def provider_names() -> list[str]:
    return sorted(_PROVIDERS)


# Prices in US dollars per million tokens, (input, output), by model name. A dollar per
# million tokens is a micro-dollar per token, so cost in micro-dollars is tokens times
# price. A model with no price costs 0.
PRICES: dict[str, tuple[float, float]] = {}


def cost_micros(model: str, input_tokens: int, output_tokens: int) -> int:
    input_price, output_price = PRICES.get(model, (0, 0))
    return math.ceil(input_tokens * input_price + output_tokens * output_price)


def cost_of(usage_by_model: list[tuple[str, int, int]]) -> int:
    """The cost of usage spread over models, each at its own price."""
    return sum(cost_micros(model, i, o) for model, i, o in usage_by_model)
