"""Render tests for the cortex_showcase slide builders (src/slide_cortex_showcase.py)."""

from __future__ import annotations

from typing import Any

from src.slide_cortex_showcase import (
    cortex_dogfood_slide,
    cortex_economics_slide,
    cortex_graph_breadth_slide,
    cortex_overview_slide,
    cortex_title_slide,
)

_BUILDERS = [
    cortex_title_slide,
    cortex_overview_slide,
    cortex_graph_breadth_slide,
    cortex_dogfood_slide,
    cortex_economics_slide,
]


def _meta(*, live: bool = True) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "graph_breadth": {
            "data_elements": 467,
            "aliases_terms": 938,
            "source_systems": 8,
            "system_of_record": ["salesforce"],
            "enrichment_sources": ["pendo", "cs_report", "atlassian_jira", "github", "cursor", "leandna", "atlassian_teams"],
            "report_blobs_mapped": 44,
            "report_blobs_per_source": {"pendo": 27, "atlassian_jira": 5, "github": 4, "salesforce": 3},
        },
        "output_surface": {
            "slide_builders": 139,
            "slide_builder_modules": 32,
            "portfolio_deck_types": ["engineering-portfolio"],
        },
        "governance_assets": {"config_yaml_files": 28, "governance_docs": 15},
        "export_economics": [
            {"artifact": "LLM-Context-All_Customers.md", "bytes": 512975, "tokens": 152840,
             "pct_of_budget": 34.0, "sections": 17, "token_budget": 450000},
        ],
        "token_budget": 450000,
    }
    if live:
        meta["live_volume"] = {
            "window_days": 30,
            "salesforce": {"portfolio_customers": 424},
            "jira_engineering": {
                "in_flight_tickets": 1095, "closed_tickets_window": 299, "open_bugs": 153,
                "blockers_criticals": 15, "contributors": 43, "themes": 162, "window_days": 30,
            },
            "github": {"unavailable": "not configured"},
            "cursor": {"unavailable": "not configured"},
        }
    return meta


def _report(meta: dict[str, Any], slide_type: str) -> dict[str, Any]:
    return {"cortex_meta": meta, "_current_slide": {"slide_type": slide_type, "title": "T"}}


def _created_object_ids(reqs: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for r in reqs:
        for key in ("createShape", "createSlide", "createTable"):
            if key in r:
                ids.append(r[key]["objectId"])
    return ids


def _all_text(reqs: list[dict[str, Any]]) -> str:
    return " ".join(r["insertText"]["text"] for r in reqs if "insertText" in r)


def test_each_builder_emits_slide_and_is_id_unique() -> None:
    meta = _meta(live=True)
    for idx, builder in enumerate(_BUILDERS):
        reqs: list[dict[str, Any]] = []
        sid = f"s_{idx}"
        ret = builder(reqs, sid, _report(meta, sid), idx)
        assert ret == idx + 1, (builder.__name__, ret)
        assert any("createSlide" in r for r in reqs), builder.__name__
        assert any("insertText" in r for r in reqs), builder.__name__
        created = _created_object_ids(reqs)
        assert len(created) == len(set(created)), f"{builder.__name__} produced duplicate object ids"


def test_title_shows_headline_stats() -> None:
    reqs: list[dict[str, Any]] = []
    cortex_title_slide(reqs, "s_t", _report(_meta(), "s_t"), 0)
    text = _all_text(reqs)
    assert "Cortex" in text
    assert "467 data elements" in text
    assert "8 source systems" in text


def test_dogfood_renders_live_jira_kpis() -> None:
    reqs: list[dict[str, Any]] = []
    cortex_dogfood_slide(reqs, "s_d", _report(_meta(live=True), "s_d"), 0)
    text = _all_text(reqs)
    assert "1,095" in text  # in-flight tickets
    assert "153" in text    # open bugs


def test_dogfood_degrades_without_live_jira() -> None:
    reqs: list[dict[str, Any]] = []
    cortex_dogfood_slide(reqs, "s_d2", _report(_meta(live=False), "s_d2"), 0)
    text = _all_text(reqs)
    # No fabricated KPIs; an explicit note instead.
    assert "unavailable" in text.lower()
    created = _created_object_ids(reqs)
    assert len(created) == len(set(created))


def test_economics_computes_tokens_per_customer() -> None:
    reqs: list[dict[str, Any]] = []
    cortex_economics_slide(reqs, "s_e", _report(_meta(live=True), "s_e"), 0)
    text = _all_text(reqs)
    # 152,840 tokens / 424 customers ~= 360 tokens each.
    assert "424 customers" in text
    assert "360" in text


def test_graph_breadth_lists_source_and_authority() -> None:
    reqs: list[dict[str, Any]] = []
    cortex_graph_breadth_slide(reqs, "s_g", _report(_meta(), "s_g"), 0)
    text = _all_text(reqs)
    assert "Salesforce" in text
    assert "Report blobs" in text
