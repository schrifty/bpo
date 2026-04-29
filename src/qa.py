"""Quality-assurance registry for cross-source data validation.

Any module can call qa.flag() during report generation to record a discrepancy.
At the end of a deck run the registry is read by the Data Quality slide builder
and then cleared for the next customer.

Usage:
    from src.qa import qa
    qa.flag("status sum != total", expected=73, actual=72,
            sources=("JIRA search count", "status breakdown"),
            severity="error")
"""

from __future__ import annotations

import datetime
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class QAFlag:
    message: str
    severity: Severity = Severity.WARNING
    expected: Any = None
    actual: Any = None
    sources: tuple[str, ...] = ()
    auto_corrected: bool = False
    internal: bool = False
    timestamp: str = field(default_factory=lambda: datetime.datetime.now().isoformat(timespec="seconds"))

    @property
    def severity_label(self) -> str:
        return self.severity.value.upper()


class QARegistry:
    """Thread-safe per-customer discrepancy collector."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._flags: list[QAFlag] = []
        self._checks: int = 0
        self._customer: str = ""

    def begin(self, customer: str) -> None:
        """Start a new validation run for a customer. Clears prior flags."""
        with self._lock:
            self._flags.clear()
            self._checks = 0
            self._customer = customer

    def check(self, description: str | None = None) -> None:
        """Record that a check passed (increments the clean-check counter)."""
        with self._lock:
            self._checks += 1

    def flag(
        self,
        message: str,
        *,
        expected: Any = None,
        actual: Any = None,
        sources: tuple[str, ...] = (),
        severity: str | Severity = "warning",
        auto_corrected: bool = False,
        internal: bool = False,
    ) -> None:
        """Record a discrepancy.

        Set internal=True for infrastructure issues (config sync, Drive fallback)
        that should be logged but not shown on the customer-facing Data Quality slide.
        """
        if isinstance(severity, str):
            severity = Severity(severity.lower())
        with self._lock:
            self._checks += 1
            self._flags.append(QAFlag(
                message=message,
                severity=severity,
                expected=expected,
                actual=actual,
                sources=sources,
                auto_corrected=auto_corrected,
                internal=internal,
            ))

    @property
    def customer(self) -> str:
        return self._customer

    @property
    def total_checks(self) -> int:
        with self._lock:
            return self._checks

    @property
    def flags(self) -> list[QAFlag]:
        with self._lock:
            return list(self._flags)

    @property
    def customer_flags(self) -> list[QAFlag]:
        """Flags visible to customers (excludes internal/infrastructure flags)."""
        return [f for f in self.flags if not f.internal]

    @property
    def errors(self) -> list[QAFlag]:
        return [f for f in self.customer_flags if f.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[QAFlag]:
        return [f for f in self.customer_flags if f.severity == Severity.WARNING]

    @property
    def infos(self) -> list[QAFlag]:
        return [f for f in self.customer_flags if f.severity == Severity.INFO]

    @property
    def clean(self) -> bool:
        return len(self.customer_flags) == 0

    def summary(
        self,
        report: dict | None = None,
        data_source_order: list[str] | None = None,
    ) -> dict[str, Any]:
        """Snapshot suitable for the Data Quality slide builder.

        Only includes customer-facing flags. Internal/infrastructure flags
        are logged but excluded from the slide.
        report: optional health report to infer data source availability (e.g. Salesforce).
        data_source_order: if set, only these keys (in this order) appear in ``data_sources``;
        if omitted, uses the legacy set (Pendo, CS Report, JIRA, Salesforce, GitHub, LeanDNA).
        """
        visible = self.customer_flags
        all_flags = self.flags
        full_sources = self._source_status(all_flags, report)
        if data_source_order is not None:
            data_sources = {k: full_sources[k] for k in data_source_order if k in full_sources}
        else:
            legacy = ("Pendo", "CS Report", "JIRA", "Salesforce", "GitHub", "LeanDNA")
            data_sources = {k: full_sources[k] for k in legacy if k in full_sources}
        return {
            "customer": self._customer,
            "total_checks": self.total_checks,
            "total_flags": len(visible),
            "errors": len([f for f in visible if f.severity == Severity.ERROR]),
            "warnings": len([f for f in visible if f.severity == Severity.WARNING]),
            "infos": len([f for f in visible if f.severity == Severity.INFO]),
            "internal_count": len([f for f in all_flags if f.internal]),
            "data_sources": data_sources,
            "flags": [
                {
                    "message": f.message,
                    "severity": f.severity_label,
                    "expected": f.expected,
                    "actual": f.actual,
                    "sources": f.sources,
                    "auto_corrected": f.auto_corrected,
                }
                for f in visible
            ],
        }

    @staticmethod
    def _leandna_source_status(report: dict | None) -> str:
        if not report or not isinstance(report, dict):
            return "unavailable"
        for key in ("leandna_shortage_trends", "leandna_item_master", "leandna_lean_projects"):
            b = report.get(key)
            if not isinstance(b, dict) or not b:
                continue
            if b.get("enabled"):
                err = (b.get("error") or "").strip()
                if err:
                    return "unavailable"
                return "ok"
        for key in ("leandna_shortage_trends", "leandna_item_master", "leandna_lean_projects"):
            b = report.get(key)
            if isinstance(b, dict) and b and b.get("enabled") is False:
                return "unavailable"
        return "unavailable"

    @staticmethod
    def _github_source_status(report: dict | None) -> str:
        if not report or not isinstance(report, dict):
            return "unavailable"
        g = report.get("github")
        if not isinstance(g, dict) or not g:
            return "unavailable"
        if (g.get("error") or "").strip():
            return "unavailable"
        return "ok"

    @staticmethod
    def _source_status(flags: list[QAFlag], report: dict | None = None) -> dict[str, str]:
        """Determine availability of each data source from the flags and optional report."""
        sources = {
            "Pendo": "ok",
            "CS Report": "ok",
            "JIRA": "ok",
            "Salesforce": "unavailable",
            "GitHub": QARegistry._github_source_status(report),
            "LeanDNA": QARegistry._leandna_source_status(report),
        }
        for f in flags:
            msg = f.message.lower()
            if "jira data unavailable" in msg:
                sources["JIRA"] = "unavailable"
            elif "cs report data unavailable" in msg:
                sources["CS Report"] = "unavailable"
            elif "salesforce data unavailable" in msg:
                sources["Salesforce"] = "unavailable"
        if report:
            sf = report.get("salesforce") or {}
            if isinstance(sf, dict) and sf and "error" not in sf:
                # Only mark ok if we actually got data back (non-empty, no error)
                sources["Salesforce"] = "ok"
        sources["GitHub"] = QARegistry._github_source_status(report)
        sources["LeanDNA"] = QARegistry._leandna_source_status(report)
        return sources


# Module-level singleton — import and use from anywhere
qa = QARegistry()
