"""Aggregate Drive-backed JSON cache effectiveness (Pendo preload + integration payloads).

Counters advance on each :func:`try_load_pendo_preload_payload` / :func:`try_load_integration_payload`
call. A **hit** means the function returned a non-None payload (fresh or stale-weekday reuse).
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .config import logger
from .hydrate_reproducibility import cache_hit_rate_line

_lock = threading.Lock()
_pendo_attempts = 0
_pendo_hits = 0
_integration_attempts = 0
_integration_hits = 0


def reset_drive_cache_load_stats() -> None:
    """Clear counters (call once at the start of a top-level run)."""
    global _pendo_attempts, _pendo_hits, _integration_attempts, _integration_hits
    with _lock:
        _pendo_attempts = _pendo_hits = _integration_attempts = _integration_hits = 0


def record_pendo_preload_load_attempt(*, hit: bool) -> None:
    global _pendo_attempts, _pendo_hits
    with _lock:
        _pendo_attempts += 1
        if hit:
            _pendo_hits += 1


def record_integration_load_attempt(*, hit: bool) -> None:
    global _integration_attempts, _integration_hits
    with _lock:
        _integration_attempts += 1
        if hit:
            _integration_hits += 1


def drive_cache_load_stats_snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "pendo_preload": {"attempts": _pendo_attempts, "hits": _pendo_hits},
            "integration": {"attempts": _integration_attempts, "hits": _integration_hits},
        }


def format_drive_cache_load_summary() -> str:
    """Single-line summary for logs."""
    snap = drive_cache_load_stats_snapshot()
    pp = snap["pendo_preload"]
    ig = snap["integration"]
    parts: list[str] = []
    if pp["attempts"] > 0:
        parts.append(cache_hit_rate_line("pendo_preload", pp["hits"], pp["attempts"]))
    if ig["attempts"] > 0:
        parts.append(cache_hit_rate_line("integration", ig["hits"], ig["attempts"]))
    if not parts:
        return "Drive JSON cache: no load attempts"
    return "Drive JSON cache — " + " | ".join(parts)


def log_drive_cache_load_summary(*, label: str = "run") -> None:
    """Emit INFO log next to timing summaries."""
    logger.info("%s: %s", label, format_drive_cache_load_summary())


@contextmanager
def drive_cache_stats_scope(*, reset: bool, log_label: str | None) -> Iterator[None]:
    """Reset counters on entry (optional); log summary on exit when ``log_label`` is set."""
    if reset:
        reset_drive_cache_load_stats()
    try:
        yield
    finally:
        if log_label:
            log_drive_cache_load_summary(label=log_label)
