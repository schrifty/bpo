"""Shared Pendo aggregation retry and read-timeout helpers.

Used by :meth:`pendo_client.PendoClient.aggregate` and export CLIs so every path
retries transient 429/5xx, timeouts, and connection errors consistently.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any, TypeVar

import requests

from .config import logger

T = TypeVar("T")

# Pendo responses worth retrying: rate limiting (429) and transient server errors.
RETRYABLE_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})
_RETRY_JITTER_S = 2.0
_RETRY_MAX_WAIT_S = 60.0


def resolve_pendo_read_timeout(total_days: int | None) -> float:
    """Scale Pendo read timeout for timeSeries aggregates (more days → longer)."""
    if total_days is None or total_days <= 0:
        return 90.0
    return min(300.0, max(90.0, 90.0 + (total_days - 14) * 3.0))


def resolve_pendo_connect_read_timeout(
    *,
    timeout: tuple[int, float] | None,
    read_timeout_days: int | None,
    default_read_timeout: float,
) -> tuple[int, float]:
    if timeout is not None:
        return timeout
    read_t = resolve_pendo_read_timeout(read_timeout_days) if read_timeout_days is not None else default_read_timeout
    return 10, read_t


def retry_after_seconds(exc: requests.exceptions.RequestException) -> float | None:
    """Return the ``Retry-After`` header (seconds form) from a response, if present."""
    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None)
    raw = headers.get("Retry-After") if headers else None
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def call_with_pendo_retry(
    fn: Callable[[], T],
    *,
    label: str,
    max_attempts: int = 3,
    read_timeout: float | None = None,
) -> T:
    """Call ``fn`` with exponential backoff on retryable Pendo failures."""
    last_exc: BaseException | None = None
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        retry_after: float | None = None
        try:
            return fn()
        except requests.exceptions.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status not in RETRYABLE_HTTP_STATUS:
                raise
            last_exc = exc
            reason = f"HTTP {status}"
            retry_after = retry_after_seconds(exc)
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            reason = "timed out"
        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            reason = "connection error"

        if attempt >= attempts:
            raise last_exc  # type: ignore[misc]
        if retry_after is not None:
            wait = min(_RETRY_MAX_WAIT_S, retry_after)
        else:
            wait = min(_RETRY_MAX_WAIT_S, 5.0 * attempt + random.uniform(0.0, _RETRY_JITTER_S))
        timeout_note = f", read_timeout={read_timeout:.0f}s" if read_timeout is not None else ""
        logger.warning(
            "Pendo %s failed (%s, attempt %d/%d%s); retry in %.1fs",
            label,
            reason,
            attempt,
            attempts,
            timeout_note,
            wait,
        )
        time.sleep(wait)
    raise RuntimeError(f"Pendo {label} failed after {attempts} attempts") from last_exc


def call_with_pendo_retry_optional(
    fn: Callable[[], T],
    *,
    label: str,
    max_attempts: int = 3,
    read_timeout: float | None = None,
    retry: bool = True,
) -> T:
    if retry and max_attempts > 1:
        return call_with_pendo_retry(
            fn,
            label=label,
            max_attempts=max_attempts,
            read_timeout=read_timeout,
        )
    return fn()
