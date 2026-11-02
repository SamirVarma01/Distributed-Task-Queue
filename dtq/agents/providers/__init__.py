"""Adapters for real LLM providers."""

import os
from collections.abc import Mapping

from .. import llm
from .anthropic_messages import PRICES as ANTHROPIC_PRICES
from .anthropic_messages import AnthropicProvider
from .openai_chat import OpenAIProvider


def register_configured(environ: Mapping[str, str] = os.environ) -> list[str]:
    """Register the providers whose credentials are set in the environment, and return
    their names: "anthropic" with ANTHROPIC_API_KEY (or ANTHROPIC_AUTH_TOKEN), "openai"
    with OPENAI_API_KEY. Anthropic's model prices are added either way."""
    llm.PRICES.update(ANTHROPIC_PRICES)
    enabled = []
    if environ.get("ANTHROPIC_API_KEY") or environ.get("ANTHROPIC_AUTH_TOKEN"):
        llm.register_provider("anthropic", AnthropicProvider())
        enabled.append("anthropic")
    if environ.get("OPENAI_API_KEY"):
        llm.register_provider("openai", OpenAIProvider())
        enabled.append("openai")
    return enabled
