"""Tests for deck governance assembly and Data Sources & Quality slide."""

from __future__ import annotations

from src.deck_governance import (
    attach_deck_governance,
    collect_source_ids_for_slide_plan,
    ordered_dq_display_names_for_slide_plan,
)
from src.qa import qa
from src.slide_data_quality import data_quality_slide


def _eng_portfolio_plan() -> list[dict]:
    return [
        {"slide_type": "eng_exec_summary", "title": "Exec"},
        {"slide_type": "cursor_cost", "title": "Cursor Spend"},
        {"slide_type": "cursor_usage", "title": "Cursor Usage"},
        {"slide_type": "data_quality", "title": "Data Sources & Quality"},
    ]


def test_ordered_dq_includes_cursor_and_atlassian_for_eng_deck() -> None:
    labels = ordered_dq_display_names_for_slide_plan(_eng_portfolio_plan())
    assert labels is not None
    assert "Cursor" in labels
    assert "Atlassian Jira" in labels
    assert "Atlassian Teams" in labels
    assert "JIRA" not in labels


def test_collect_source_ids_includes_cursor() -> None:
    ids = collect_source_ids_for_slide_plan(_eng_portfolio_plan())
    assert "cursor" in ids
    assert "atlassian_jira" in ids
    assert "atlassian_teams" in ids


def test_attach_governance_surfaces_cursor_warnings() -> None:
    qa.begin("test")
    report = {
        "days": 30,
        "generated": "2026-06-15",
        "eng_portfolio": {"team_roster": {"teams": [{"name": "Dev - Core"}]}},
        "cursor_usage": {
            "configured": True,
            "generated_at": "2026-06-15T12:00:00",
            "window_days": 30,
            "warnings": ["8 users had tokens but no accepted-line data"],
            "cost_engineers": {"configured": True},
        },
        "jira": {"jql_queries": [{"description": "LEAN open", "jql": "project = LEAN"}]},
    }
    plan = _eng_portfolio_plan()
    attach_deck_governance(report, plan, deck_id="engineering-portfolio")

    gov = report.get("_governance") or {}
    assert gov.get("deck_id") == "engineering-portfolio"
    assert "Cursor" in (gov.get("source_status") or {})
    assert any("accepted-line" in d.get("message", "") for d in gov.get("discrepancies") or [])
    assert any("Dev -" in line for line in gov.get("scope") or [])
    assert gov.get("lineage")


def test_data_quality_slide_renders_governance_sections() -> None:
    qa.begin("test")
    report = {
        "days": 30,
        "_slide_plan": _eng_portfolio_plan(),
        "cursor_usage": {"configured": True, "window_days": 30, "cost_engineers": {"configured": True}},
        "eng_portfolio": {"team_roster": {"teams": []}},
        "jira": {"jql_queries": [{"description": "LEAN", "jql": "project = LEAN"}]},
    }
    attach_deck_governance(report, report["_slide_plan"], deck_id="engineering-portfolio")

    reqs: list = []
    data_quality_slide(reqs, "sid_dq", report, 0)
    texts = []
    for r in reqs:
        if isinstance(r, dict) and "insertText" in r:
            texts.append(r["insertText"].get("text", ""))
    blob = "\n".join(texts)
    assert "Data Sources & Quality" in blob
    assert "Scope & filters" in blob
    assert "Lineage" in blob
    assert "Cursor" in blob
    assert "Atlassian Jira" in blob
    assert "JIRA" not in blob
