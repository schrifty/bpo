"""Tests for export warning collection and stderr summary."""

from __future__ import annotations

import pytest

from src.export_run_diagnostics import (
    ExportRunDiagnostics,
    collect_export_warning,
    export_diagnostics_scope,
    export_phase,
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
    timing_pos = err.find("Export timing:")
    cache_pos = err.find("Drive JSON cache")
    warn_pos = err.find("Warnings (1):")
    assert timing_pos >= 0 and cache_pos > timing_pos and warn_pos > cache_pos
    assert "portfolio: 12.5s" in err
    assert "  1. first" in err


def test_export_phase_records_elapsed() -> None:
    diag = ExportRunDiagnostics()
    with export_phase(diag, "step"):
        pass
    assert diag.timings == [("step", pytest.approx(0.0, abs=0.5))]
