"""Tests for LLM export markdown size breakdown helpers."""

from __future__ import annotations

from src.drive_cache_stats import record_integration_load_attempt, reset_drive_cache_load_stats
from src.export_llm_context_snapshot import (
    _doc_payload_component_bytes,
    _markdown_section_byte_breakdown,
    emit_export_size_breakdown_stderr,
)


def test_markdown_section_byte_breakdown_splits_headers() -> None:
    md = "# Title\n\nintro\n\n## 1. Pendo\n\n{}\n\n## 2. Jira\n\n[]\n"
    parts = dict(_markdown_section_byte_breakdown(md))
    assert "header / preamble" in parts
    assert "1. Pendo" in parts
    assert "2. Jira" in parts
    assert sum(parts.values()) <= len(md.encode("utf-8")) + 50


def test_doc_payload_component_bytes_orders_by_size() -> None:
    doc = {
        "pendo": {"a": 1},
        "jira_help": {"b": 2},
        "salesforce_comprehensive_portfolio": {"big": "x" * 5000},
    }
    rows = _doc_payload_component_bytes(doc)
    assert rows[0][0] == "salesforce_comprehensive_portfolio"


def test_emit_export_size_breakdown_stderr(capsys) -> None:
    from src.export_run_diagnostics import ExportRunDiagnostics

    reset_drive_cache_load_stats()
    record_integration_load_attempt(hit=True)
    md = "## 1. Pendo\n\n{}\n\n## 7. Risk\n\nline\n"
    doc = {
        "pendo": {"x": 1},
        "jira_help": {},
        "_portfolio_raw": {
            "_llm_export_salesforce_comprehensive": {
                "customers_fetched": 5,
                "customers_drive_cache_hit": 4,
                "customers_salesforce_fetch": 1,
            },
        },
    }
    diag = ExportRunDiagnostics()
    diag.record_phase("portfolio snapshot", 12.5)
    diag.record_phase("Drive upload", 3.2)
    emit_export_size_breakdown_stderr(md, doc, diag)
    err = capsys.readouterr().err
    assert "Export completed" in err
    assert "uploaded" in err
    assert "Markdown sections" in err
    assert "Payload components" in err
    assert "Timing" in err
    assert "portfolio snapshot" in err
    assert "00:00:13" in err  # 12.5s rounded
    assert "Cache" in err
    assert "integration" in err
    assert "salesforce_comprehensive" in err
    assert "Pendo" in err
    assert "─" * 60 in err


def test_emit_export_summary_includes_warnings(capsys) -> None:
    from src.export_run_diagnostics import ExportRunDiagnostics

    diag = ExportRunDiagnostics()
    diag.add_warning("Pendo prefix missing for Acme")
    emit_export_size_breakdown_stderr("## 1. Test\n", {"pendo": {}}, diag)
    err = capsys.readouterr().err
    assert "completed with 1 warning" in err
    assert "Warnings (1)" in err
    assert "Pendo prefix missing" in err
    assert "Failures" not in err
