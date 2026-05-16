"""Collect warnings and phase timings during ``decks --export`` / :func:`export_main`."""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

_export_diag: ContextVar["ExportRunDiagnostics | None"] = ContextVar("export_run_diagnostics", default=None)


class ExportRunDiagnostics:
    """Per-export warning list and wall-clock phase timings."""

    def __init__(self) -> None:
        self._warnings: list[str] = []
        self._warning_seen: set[str] = set()
        self._timings: list[tuple[str, float]] = []
        self._t0 = time.monotonic()

    def add_warning(self, message: str) -> None:
        msg = (message or "").strip()
        if not msg or msg in self._warning_seen:
            return
        self._warning_seen.add(msg)
        self._warnings.append(msg)

    def record_phase(self, label: str, elapsed_s: float) -> None:
        self._timings.append((label, max(0.0, float(elapsed_s))))

    @property
    def warnings(self) -> list[str]:
        return list(self._warnings)

    @property
    def timings(self) -> list[tuple[str, float]]:
        return list(self._timings)

    def total_elapsed_s(self) -> float:
        return time.monotonic() - self._t0

    def emit_stderr_summary(self) -> None:
        """Print timing stats, Drive cache summary, then all collected warnings."""
        from .drive_cache_stats import format_drive_cache_load_summary

        total = self.total_elapsed_s()
        print("", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("Export timing:", file=sys.stderr)
        for label, secs in self._timings:
            print(f"  {label}: {secs:.1f}s", file=sys.stderr)
        print(f"  total: {total:.1f}s", file=sys.stderr)
        print(format_drive_cache_load_summary(), file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        if self._warnings:
            print(f"Warnings ({len(self._warnings)}):", file=sys.stderr)
            for i, w in enumerate(self._warnings, 1):
                print(f"  {i}. {w}", file=sys.stderr)
        else:
            print("Warnings: none", file=sys.stderr)
        print("=" * 60, file=sys.stderr)


class _ExportWarningLogHandler(logging.Handler):
    """Capture ``bpo`` logger WARNING+ into the active export diagnostics."""

    def __init__(self, diag: ExportRunDiagnostics) -> None:
        super().__init__(level=logging.WARNING)
        self._diag = diag

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._diag.add_warning(record.getMessage())
        except Exception:
            self.handleError(record)


def active_export_diagnostics() -> ExportRunDiagnostics | None:
    return _export_diag.get()


def collect_export_warning(message: str, *, llm_export: bool = False) -> None:
    """Record a warning for the export recap, or print immediately when not in export scope."""
    msg = (message or "").strip()
    if not msg:
        return
    if llm_export and not msg.lower().startswith("llm export"):
        msg = f"LLM export — {msg}"
    diag = _export_diag.get()
    if diag is not None:
        diag.add_warning(msg)
    else:
        print(f"warning: {msg}", file=sys.stderr)


@contextmanager
def export_diagnostics_scope() -> Iterator[ExportRunDiagnostics]:
    """Install warning capture on the ``bpo`` logger for the duration of an export run."""
    from .config import logger as bpo_logger
    from .drive_cache_stats import reset_drive_cache_load_stats

    reset_drive_cache_load_stats()
    diag = ExportRunDiagnostics()
    token = _export_diag.set(diag)
    handler = _ExportWarningLogHandler(diag)
    bpo_logger.addHandler(handler)
    try:
        yield diag
    finally:
        bpo_logger.removeHandler(handler)
        _export_diag.reset(token)


@contextmanager
def export_phase(diag: ExportRunDiagnostics, label: str) -> Iterator[None]:
    """Record wall time for one export phase."""
    t0 = time.monotonic()
    try:
        yield
    finally:
        diag.record_phase(label, time.monotonic() - t0)
