"""The provider-neutral LLM interface (docs/agent-runtime-design.md, section 10).

Messages are plain JSON-serializable dicts, so they can be hashed and stored:
    {"role": "user", "content": str}
    {"role": "assistant", "content": str | None, "tool_calls": [{"id", "name", "args"}]}
    {"role": "tool", "tool_call_id": str, "content": <JSON>, "is_error": bool}
The system prompt goes in LLMRequest.system, not in the messages."""

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


@dataclass(frozen=True)
class LLMRequest:
    model: str
    system: str
    messages: list[dict[str, Any]]
    tools: list[ToolSpec]
    max_output_tokens: int
    idempotency_key: str  # "<run_id>:<step_no>"


@dataclass(frozen=True)
class LLMResponse:
    text: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    stop_reason: str = "end_turn"
    input_tokens: int = 0
    output_tokens: int = 0

    def to_output(self) -> dict[str, Any]:
        """The response as an LLM step's recorded output (usage is stored separately)."""
        return {"text": self.text, "tool_calls": [asdict(c) for c in self.tool_calls],
                "stop_reason": self.stop_reason}


class ProviderError(Exception):
    """A failed LLM call. Retryable errors (rate limits, 5xx, timeouts) are retried
    with backoff by DTQ; any other fails the run."""

    def __init__(self, message: str, *, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


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
