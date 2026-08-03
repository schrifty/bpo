"""Tests for the cortex_meta report blob (src/cortex_meta_report.py)."""

from __future__ import annotations

from src import cortex_meta_report as cmr


def test_build_cortex_meta_report_static_shape() -> None:
    meta = cmr.build_cortex_meta_report(days=30, live=False, export_globs=())
    # Static facts read from repo config / registries — must be present and non-trivial.
    gb = meta["graph_breadth"]
    assert gb["data_elements"] > 100
    assert gb["aliases_terms"] >= gb["data_elements"]
    assert gb["source_systems"] == 8
    assert gb["system_of_record"] == ["salesforce"]
    assert "salesforce" not in gb["enrichment_sources"]
    assert gb["report_blobs_mapped"] > 0

    os_ = meta["output_surface"]
    assert os_["slide_builders"] > 0
    assert os_["slide_builder_modules"] > 0
    assert "engineering-portfolio" in os_["portfolio_deck_types"]

    assert meta["token_budget"] > 0
    assert meta["export_economics"] == []  # no globs matched
    assert "live_volume" not in meta  # live not requested
    assert meta["generated_at"]


def test_guard_captures_failure_as_unavailable() -> None:
    def boom() -> dict:
        raise RuntimeError("no creds")

    out = cmr._guard(boom)
    assert out == {"unavailable": "no creds"}


def test_guard_passes_through_success() -> None:
    assert cmr._guard(lambda: {"n": 3}) == {"n": 3}


def test_live_volume_guards_each_source(monkeypatch) -> None:
    # Every source raises -> each becomes an explicit unavailable, never a fabricated count.
    monkeypatch.setattr(cmr, "_salesforce_counts", lambda: (_ for _ in ()).throw(RuntimeError("sf down")))
    monkeypatch.setattr(cmr, "_jira_engineering_counts", lambda days: (_ for _ in ()).throw(RuntimeError("jira down")))
    monkeypatch.setattr(cmr, "_github_counts", lambda days: (_ for _ in ()).throw(RuntimeError("gh down")))
    monkeypatch.setattr(cmr, "_cursor_counts", lambda days: (_ for _ in ()).throw(RuntimeError("cur down")))

    lv = cmr._live_volume_facts(30)
    assert lv["window_days"] == 30
    for src in ("salesforce", "jira_engineering", "github", "cursor"):
        assert "unavailable" in lv[src]


def test_build_with_live_includes_live_volume(monkeypatch) -> None:
    monkeypatch.setattr(cmr, "_salesforce_counts", lambda: {"portfolio_customers": 424})
    monkeypatch.setattr(cmr, "_jira_engineering_counts", lambda days: {"in_flight_tickets": 1})
    monkeypatch.setattr(cmr, "_github_counts", lambda days: {"unavailable": "x"})
    monkeypatch.setattr(cmr, "_cursor_counts", lambda days: {"unavailable": "y"})

    meta = cmr.build_cortex_meta_report(days=14, live=True, export_globs=())
    lv = meta["live_volume"]
    assert lv["salesforce"]["portfolio_customers"] == 424
    assert lv["window_days"] == 14
