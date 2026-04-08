"""Shared LLM helpers: OpenAI-compatible chat completions with retries, JSON fence stripping."""

from __future__ import annotations

import re as _re
import time
from typing import Any

from openai import NotFoundError, RateLimitError

from .config import LLM_MODEL, LLM_PROVIDER, logger


def _strip_json_code_fence(raw: str) -> str:
    """Remove optional ```json ... ``` wrapper so json.loads succeeds."""
    s = (raw or "").strip()
    if not s.startswith("```"):
        return s
    lines = s.split("\n")
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _llm_create_with_retry(client: Any, max_retries: int = 3, **kwargs: Any):
    """Call client.chat.completions.create with exponential backoff on 429."""
    delay = 30
    for attempt in range(max_retries):
        try:
            return client.chat.completions.create(**kwargs)
        except NotFoundError as e:
            logger.error(
                "LLM model not found (%s / %s). "
                "Update LLM_MODEL in src/config.py or check the provider's model list. Error: %s",
                LLM_PROVIDER,
                LLM_MODEL,
                str(e)[:200],
            )
            raise
        except RateLimitError as e:
            err_str = str(e)
            hard_quota = "limit: 0" in err_str or "insufficient_quota" in err_str

            if hard_quota:
                if LLM_PROVIDER == "gemini":
                    logger.error(
                        "LLM quota exhausted (Gemini free tier). "
                        "Fix: go to console.cloud.google.com, enable billing on the project "
                        "that owns your GEMINI_API_KEY, then re-run. "
                        "Or set LLM_PROVIDER=openai in .env to use OpenAI instead."
                    )
                else:
                    logger.error(
                        "LLM quota exhausted (OpenAI). "
                        "Fix: add credits at platform.openai.com/settings/organization/billing, "
                        "or set LLM_PROVIDER=gemini in .env to use Gemini instead."
                    )
                raise

            if attempt == max_retries - 1:
                logger.error(
                    "LLM rate limit hit %d times, giving up. Error: %s",
                    max_retries,
                    err_str[:300],
                )
                raise

            m = _re.search(r"retry in (\d+(?:\.\d+)?)s", err_str)
            wait = int(float(m.group(1))) + 2 if m else delay
            logger.warning(
                "LLM rate limit — retrying in %ds (attempt %d/%d)...",
                wait,
                attempt + 1,
                max_retries,
            )
            time.sleep(wait)
            delay *= 2
    return None  # unreachable
