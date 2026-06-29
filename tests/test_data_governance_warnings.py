"""Tests for data-governance warning collection and export markdown."""

from __future__ import annotations

from src.data_governance_warnings import (
    build_data_governance_warning_entries,
    clear_data_governance_warnings,
    record_data_governance_warning,
    render_data_governance_markdown_lines,
)
from src.export_run_diagnostics import ExportRunDiagnostics


def test_record_and_render_governance_warning():
    clear_data_governance_warnings()
    record_data_governance_warning(
        "help_jsm_org_fallback",
        "HELP scope: no JSM Organizations match for 'Acme'; using summary/description fallback. "
        "Salesforce: 1 active Customer Entity.",
        context={"customer_name": "Acme"},
    )
    entries = build_data_governance_warning_entries()
    assert len(entries) == 1
    assert entries[0]["category"] == "help_jsm_org_fallback"
    md = "\n".join(render_data_governance_markdown_lines(entries))
    assert "HELP Jira scope" in md
    assert "Acme" in md


def test_build_merges_report_meta_and_diag_warnings():
    clear_data_governance_warnings()
    diag = ExportRunDiagnostics()
    diag.add_warning(
        "Portfolio (Salesforce allowlist): no Pendo customer prefix matches "
        "Salesforce label 'Globex' (visitor / sitename token) — skipping."
    )
    report = {
        "_salesforce_portfolio_allowlist_meta": {
            "salesforce_labels_unmatched": [
                {
                    "salesforce_label": "Globex",
                    "salesforce_activity": {
                        "matched": True,
                        "active": True,
                        "churned": False,
                    },
                }
            ]
        },
        "_llm_export_customer_filter": {"warnings": ["dropped inactive customer Foo"]},
    }
    entries = build_data_governance_warning_entries(report, diag)
    messages = [e["message"] for e in entries]
    assert any("Globex" in m for m in messages)
    assert any("Customer filter:" in m and "Foo" in m for m in messages)
    # diag log is superseded by richer allowlist meta (same SF label)
    globex_entries = [e for e in entries if "Globex" in e.get("message", "")]
    assert len(globex_entries) == 1
    assert globex_entries[0].get("source") == "salesforce_allowlist_meta"


def test_render_empty_governance_section():
    lines = render_data_governance_markdown_lines([])
    assert any("No data-governance warnings" in ln for ln in lines)
