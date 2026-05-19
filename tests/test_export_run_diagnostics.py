"""Tests for export warning collection and stderr summary."""

from __future__ import annotations

import pytest

from src.export_run_diagnostics import (
    ExportRunDiagnostics,
    collect_export_warning,
    export_diagnostics_scope,
    export_phase,
    format_elapsed_hms,
)


def test_add_warning_dedupes() -> None:
    d = ExportRunDiagnostics()
    d.add_warning("same")
    d.add_warning("same")
    d.add_warning("other")
    assert d.warnings == ["same", "other"]


def test_collect_export_warning_outside_scope_prints(capsys) -> None:
    collect_export_warning("orphan", llm_export=True)
    err = capsys.readouterr().err
    assert "warning: LLM export — orphan" in err


def test_scope_captures_bpo_logger_warning() -> None:
    with export_diagnostics_scope() as diag:
        from src.config import logger

        logger.warning("Portfolio snapshot: test warning")
        assert any("Portfolio snapshot" in w for w in diag.warnings)


def test_emit_stderr_summary_order(capsys) -> None:
    diag = ExportRunDiagnostics()
    diag.record_phase("portfolio", 12.5)
    diag.add_warning("first")
    diag.emit_stderr_summary()
    err = capsys.readouterr().err
    assert "Export timing:" not in err
    warn_pos = err.find("Warnings (1):")
    assert warn_pos >= 0
    assert "Drive JSON cache" not in err
    assert "  1. first" in err


def test_format_elapsed_hms() -> None:
    assert format_elapsed_hms(0) == "00:00:00"
    assert format_elapsed_hms(12.5) == "00:00:13"
    assert format_elapsed_hms(3661) == "01:01:01"
    assert format_elapsed_hms(2253.4) == "00:37:33"


def test_timing_breakdown_lines(capsys) -> None:
    diag = ExportRunDiagnostics()
    diag.record_phase("portfolio snapshot", 10.0)
    diag.record_phase("markdown build", 2.0)
    lines = diag.timing_breakdown_lines()
    assert any("portfolio snapshot" in ln for ln in lines)
    assert any("00:00:10" in ln for ln in lines)
    assert any("total wall time: " in ln and "00:00:" in ln for ln in lines)


def test_export_phase_records_elapsed() -> None:
    diag = ExportRunDiagnostics()
    with export_phase(diag, "step"):
        pass
    assert diag.timings == [("step", pytest.approx(0.0, abs=0.5))]
