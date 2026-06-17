"""Aggregate Drive-backed JSON cache effectiveness (integration payloads)."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .config import logger
from .hydrate_reproducibility import cache_hit_rate_line

_lock = threading.Lock()
_integration_attempts = 0
_integration_hits = 0


def reset_drive_cache_load_stats() -> None:
    """Clear counters (call once at the start of a top-level run)."""
    global _integration_attempts, _integration_hits
    with _lock:
        _integration_attempts = _integration_hits = 0


def record_integration_load_attempt(*, hit: bool) -> None:
    global _integration_attempts, _integration_hits
    with _lock:
        _integration_attempts += 1
        if hit:
            _integration_hits += 1


def drive_cache_load_stats_snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "integration": {"attempts": _integration_attempts, "hits": _integration_hits},
        }


def _cache_hit_miss_row(label: str, hits: int, attempts: int) -> str | None:
    if attempts <= 0:
        return None
    misses = max(0, attempts - hits)
    pct = (100.0 * hits / attempts) if attempts else 0.0
    return f"    {pct:5.1f}%  {hits:4d} hit  {misses:4d} miss  ({attempts:4d} loads)  {label}"


def drive_cache_breakdown_lines(
    *,
    sf_comprehensive_summary: dict[str, Any] | None = None,
) -> list[str]:
    """Multi-line cache hit/miss stats for export run summary (stderr)."""
    rows: list[str] = []
    snap = drive_cache_load_stats_snapshot()
    bucket = snap["integration"]
    row = _cache_hit_miss_row("integration (Drive JSON)", int(bucket["hits"]), int(bucket["attempts"]))
    if row:
        rows.append(row)

    if isinstance(sf_comprehensive_summary, dict):
        fetched = int(sf_comprehensive_summary.get("customers_fetched") or 0)
        if fetched > 0:
            hits = int(sf_comprehensive_summary.get("customers_drive_cache_hit") or 0)
            misses = int(sf_comprehensive_summary.get("customers_salesforce_fetch") or 0)
            row = _cache_hit_miss_row("salesforce_comprehensive (per customer)", hits, hits + misses)
            if row:
                rows.append(row)

    if not rows:
        return []
    return ["  --- cache hit/miss ---", *rows]


def format_drive_cache_load_summary() -> str:
    """Single-line summary for logs."""
    snap = drive_cache_load_stats_snapshot()
    ig = snap["integration"]
    if ig["attempts"] > 0:
        return "Drive JSON cache — " + cache_hit_rate_line("integration", ig["hits"], ig["attempts"])
    return "Drive JSON cache: no load attempts"


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
