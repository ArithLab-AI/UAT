"""Lightweight LLM token-usage logging.

Attach ``TokenUsageLogger`` as a callback on any LangChain ``.invoke`` call to log
per-call input/output/total tokens plus a running process total. This makes it
possible to see exactly which functionality (and which prompt) consumes tokens,
instead of guessing.
"""
from __future__ import annotations

import logging
import threading
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler

logger = logging.getLogger("token_usage")

_lock = threading.Lock()
_totals: dict[str, int] = {
    "input_tokens": 0,
    "output_tokens": 0,
    "total_tokens": 0,
    "calls": 0,
}


def _extract_usage(response: Any) -> dict[str, int]:
    """Pull token counts out of a LangChain ``LLMResult`` defensively, trying the
    modern ``usage_metadata`` first and falling back to OpenAI ``token_usage``."""
    input_tokens = 0
    output_tokens = 0
    total_tokens = 0

    try:
        for generation_list in (getattr(response, "generations", None) or []):
            for generation in generation_list:
                message = getattr(generation, "message", None)
                usage = getattr(message, "usage_metadata", None) if message is not None else None
                if usage:
                    input_tokens += int(usage.get("input_tokens", 0) or 0)
                    output_tokens += int(usage.get("output_tokens", 0) or 0)
                    total_tokens += int(usage.get("total_tokens", 0) or 0)
    except Exception:  # pragma: no cover - logging must never break a call
        pass

    if total_tokens == 0:
        try:
            llm_output = getattr(response, "llm_output", None) or {}
            token_usage = llm_output.get("token_usage") or llm_output.get("usage") or {}
            input_tokens = int(token_usage.get("prompt_tokens", 0) or 0)
            output_tokens = int(token_usage.get("completion_tokens", 0) or 0)
            total_tokens = int(token_usage.get("total_tokens", 0) or 0)
        except Exception:  # pragma: no cover
            pass

    if total_tokens == 0:
        total_tokens = input_tokens + output_tokens

    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }


class TokenUsageLogger(BaseCallbackHandler):
    """Logs token usage for every LLM call it is attached to."""

    def __init__(self, *, label: str = "llm") -> None:
        self.label = label

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        usage = _extract_usage(response)
        with _lock:
            _totals["input_tokens"] += usage["input_tokens"]
            _totals["output_tokens"] += usage["output_tokens"]
            _totals["total_tokens"] += usage["total_tokens"]
            _totals["calls"] += 1
            running_total = _totals["total_tokens"]
            call_count = _totals["calls"]

        logger.info(
            "[token-usage] %s | input=%d output=%d total=%d | session_total=%d over %d calls",
            self.label,
            usage["input_tokens"],
            usage["output_tokens"],
            usage["total_tokens"],
            running_total,
            call_count,
        )


def get_token_usage_totals() -> dict[str, int]:
    """Return a snapshot of cumulative token usage since process start."""
    with _lock:
        return dict(_totals)


def reset_token_usage_totals() -> dict[str, int]:
    """Zero the counters and return the totals recorded just before the reset.

    Useful to measure a clean window (reset, run some prompts, read totals)."""
    with _lock:
        previous = dict(_totals)
        for key in _totals:
            _totals[key] = 0
    return previous
