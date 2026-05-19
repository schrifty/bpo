"""Tests for LLM export markdown size breakdown helpers."""

from __future__ import annotations

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
    md = "## 1. Pendo\n\n{}\n\n## 7. Risk\n\nline\n"
    doc = {"pendo": {"x": 1}, "jira_help": {}}
    emit_export_size_breakdown_stderr(md, doc)
    err = capsys.readouterr().err
    assert "Export size breakdown" in err
    assert "total uploaded" in err
    assert "markdown sections" in err
    assert "document payloads" in err
    assert "Pendo" in err
