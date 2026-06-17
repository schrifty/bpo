"""Warnings, timings, and machine-readable summaries for batch deck runs."""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Iterator

from .run_context import current_run_id, run_context_fields

_run_diag: ContextVar["RunDiagnostics | None"] = ContextVar("run_diagnostics", default=None)


def format_elapsed_hms(seconds: float) -> str:
    """Format elapsed seconds as HH:mm:ss (hours may exceed 23 for long runs)."""
    total = int(max(0.0, float(seconds)) + 0.5)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class RunDiagnostics:
    """Per-run warnings, failures, and wall-clock phase timings."""

    def __init__(self, *, scope: str = "run") -> None:
        self.scope = scope
        self._warnings: list[str] = []
        self._warning_seen: set[str] = set()
        self._failures: list[str] = []
        self._timings: list[tuple[str, float]] = []
        self._integration_meta: dict[str, Any] = {}
        self._t0 = time.monotonic()

    def add_warning(self, message: str) -> None:
        msg = (message or "").strip()
        if not msg or msg in self._warning_seen:
            return
        self._warning_seen.add(msg)
        self._warnings.append(msg)

    def add_failure(self, message: str) -> None:
        msg = (message or "").strip()
        if msg:
            self._failures.append(msg)

    def set_integration_meta(self, meta: dict[str, Any]) -> None:
        if isinstance(meta, dict):
            self._integration_meta.update(meta)

    def record_phase(self, label: str, elapsed_s: float) -> None:
        self._timings.append((label, max(0.0, float(elapsed_s))))

    @property
    def warnings(self) -> list[str]:
        return list(self._warnings)

    @property
    def failures(self) -> list[str]:
        return list(self._failures)

    @property
    def timings(self) -> list[tuple[str, float]]:
        return list(self._timings)

    @property
    def integration_meta(self) -> dict[str, Any]:
        return dict(self._integration_meta)

    def total_elapsed_s(self) -> float:
        return time.monotonic() - self._t0

    def timing_breakdown_lines(self) -> list[str]:
        if not self._timings:
            return []
        total = self.total_elapsed_s()
        measured = sum(secs for _, secs in self._timings)
        lines = ["  --- wall-clock timing ---"]
        for label, secs in self._timings:
            pct = (100.0 * secs / total) if total else 0.0
            lines.append(f"    {pct:5.1f}%  {format_elapsed_hms(secs):>8}  {label}")
        overhead = max(0.0, total - measured)
        if overhead >= 0.5:
            pct = (100.0 * overhead / total) if total else 0.0
            lines.append(f"    {pct:5.1f}%  {format_elapsed_hms(overhead):>8}  (unphased overhead)")
        lines.append(f"  total wall time: {format_elapsed_hms(total)}")
        return lines

    def success(self, *, fail_on_warnings: bool = False) -> bool:
        if self._failures:
            return False
        if fail_on_warnings and self._warnings:
            return False
        return True

    def build_summary(self, *, job_name: str | None = None, fail_on_warnings: bool = False) -> dict[str, Any]:
        ctx = run_context_fields()
        summary: dict[str, Any] = {
            "event": "run_complete",
            "success": self.success(fail_on_warnings=fail_on_warnings),
            "run_id": ctx.get("run_id") or current_run_id(),
            "scope": self.scope,
            "duration_s": round(self.total_elapsed_s(), 1),
            "failures": list(self._failures),
            "warnings": list(self._warnings),
            "integration_warnings": len(self._warnings),
            "deck_failures": len(self._failures),
        }
        if job_name:
            summary["job"] = job_name
        elif ctx.get("job_name"):
            summary["job"] = ctx["job_name"]
        summary.update(self._integration_meta)
        return summary

    def emit_run_summary(
        self,
        *,
        job_name: str | None = None,
        fail_on_warnings: bool = False,
        json_summary: bool = True,
    ) -> dict[str, Any]:
        summary = self.build_summary(job_name=job_name, fail_on_warnings=fail_on_warnings)
        line = "BPO_RUN_SUMMARY=" + json.dumps(summary, separators=(",", ":"), default=str)
        print(line, flush=True)
        if json_summary:
            emf = {
                "_aws": {
                    "Timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "BPO",
                            "Dimensions": [["Job"]],
                            "Metrics": [
                                {"Name": "RunSuccess", "Unit": "Count"},
                                {"Name": "RunDurationSeconds", "Unit": "Seconds"},
                                {"Name": "DeckFailures", "Unit": "Count"},
                                {"Name": "IntegrationWarnings", "Unit": "Count"},
                            ],
                        }
                    ],
                },
                "Job": summary.get("job") or "unknown",
                "RunSuccess": 1 if summary["success"] else 0,
                "RunDurationSeconds": summary["duration_s"],
                "DeckFailures": summary["deck_failures"],
                "IntegrationWarnings": summary["integration_warnings"],
            }
            print(json.dumps(emf, separators=(",", ":")), flush=True)
        from .config import logger as bpo_logger

        bpo_logger.info(
            "run_complete success=%s duration_s=%s failures=%d warnings=%d",
            summary["success"],
            summary["duration_s"],
            summary["deck_failures"],
            summary["integration_warnings"],
            extra={"event": "run_complete", "success": summary["success"]},
        )
        return summary

    def emit_stderr_summary(self) -> None:
        print("", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        if self._failures:
            print(f"Failures ({len(self._failures)}):", file=sys.stderr)
            for i, msg in enumerate(self._failures, 1):
                print(f"  {i}. {msg}", file=sys.stderr)
        if self._warnings:
            print(f"Warnings ({len(self._warnings)}):", file=sys.stderr)
            for i, w in enumerate(self._warnings, 1):
                print(f"  {i}. {w}", file=sys.stderr)
        if not self._failures and not self._warnings:
            print("Failures/Warnings: none", file=sys.stderr)
        print("=" * 60, file=sys.stderr)


class _RunWarningLogHandler(logging.Handler):
    def __init__(self, diag: RunDiagnostics) -> None:
        super().__init__(level=logging.WARNING)
        self._diag = diag

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._diag.add_warning(record.getMessage())
        except Exception:
            self.handleError(record)


def active_run_diagnostics() -> RunDiagnostics | None:
    return _run_diag.get()


def collect_run_warning(message: str, *, llm_export: bool = False) -> None:
    msg = (message or "").strip()
    if not msg:
        return
    if llm_export and not msg.lower().startswith("llm export"):
        msg = f"LLM export — {msg}"
    diag = _run_diag.get()
    if diag is not None:
        diag.add_warning(msg)
    else:
        print(f"warning: {msg}", file=sys.stderr)


@contextmanager
def run_diagnostics_scope(*, scope: str = "run", reset_drive_cache: bool = True) -> Iterator[RunDiagnostics]:
    from .config import logger as bpo_logger

    if reset_drive_cache:
        from .drive_cache_stats import reset_drive_cache_load_stats

        reset_drive_cache_load_stats()
    try:
        from .data_governance_warnings import clear_data_governance_warnings

        clear_data_governance_warnings()
    except Exception:
        pass
    diag = RunDiagnostics(scope=scope)
    token = _run_diag.set(diag)
    handler = _RunWarningLogHandler(diag)
    bpo_logger.addHandler(handler)
    try:
        yield diag
    finally:
        bpo_logger.removeHandler(handler)
        _run_diag.reset(token)


@contextmanager
def run_phase(diag: RunDiagnostics, label: str) -> Iterator[None]:
    t0 = time.monotonic()
    try:
        yield
    finally:
        diag.record_phase(label, time.monotonic() - t0)
