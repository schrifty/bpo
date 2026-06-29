"""Tests for run diagnostics and CORTEX_RUN_SUMMARY emission."""

from __future__ import annotations

import json

from src.run_diagnostics import RunDiagnostics, collect_run_warning, run_diagnostics_scope


def test_run_summary_emits_json_line(capsys) -> None:
    diag = RunDiagnostics(scope="test")
    diag.add_failure("step-a: exit 1")
    summary = diag.emit_run_summary(job_name="demo", json_summary=False)
    out = capsys.readouterr().out
    assert out.startswith("CORTEX_RUN_SUMMARY=")
    parsed = json.loads(out.split("=", 1)[1].strip())
    assert parsed["success"] is False
    assert parsed["job"] == "demo"
    assert parsed["failures"] == ["step-a: exit 1"]
    assert summary["event"] == "run_complete"


def test_collect_run_warning_in_scope() -> None:
    with run_diagnostics_scope(scope="unit") as diag:
        collect_run_warning("integration stale")
        assert "integration stale" in diag.warnings


def test_fail_on_warnings() -> None:
    diag = RunDiagnostics()
    diag.add_warning("soft issue")
    assert diag.success(fail_on_warnings=False) is True
    assert diag.success(fail_on_warnings=True) is False
