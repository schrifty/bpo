"""Unit tests for QBR template flow (no live Google/Pendo)."""
import datetime

import pytest
from unittest.mock import MagicMock, patch

from src import qbr_template


def test_quarter_range_from_health_report():
    r = qbr_template._quarter_range_from_health_report(
        {"quarter": "Q1 2026", "quarter_start": "2026-01-01", "quarter_end": "2026-03-31"},
    )
    assert r is not None
    assert r.label == "Q1 2026"
    assert r.start == datetime.date(2026, 1, 1)
    assert r.end == datetime.date(2026, 3, 31)
    assert qbr_template._quarter_range_from_health_report({"days": 30}) is None


def test_normalize_manifest_plan():
    raw = {
        "hide": {"title_contains": [" DOI "], "indices": ["3", 4, 5.0]},
        "move_to_end_title_contains": ["summary"],
        "notes": "ok",
    }
    p = qbr_template._normalize_manifest_plan(raw)
    assert p["hide"]["title_contains"] == ["DOI"]
    assert p["hide"]["indices"] == [3, 4, 5]
    assert p["move_to_end_title_contains"] == ["summary"]


def test_normalize_manifest_plan_empty_defaults():
    p = qbr_template._normalize_manifest_plan({})
    assert p["hide"]["indices"] == []
    assert p["hide"]["title_contains"] == []


def test_resolve_hide_object_ids_skips_title():
    inv = [
        {"index": 1, "title": "Cover", "objectId": "t1"},
        {"index": 2, "title": "DOI metrics", "objectId": "s2"},
        {"index": 3, "title": "Agenda", "objectId": "s3"},
    ]
    plan = {
        "hide": {"title_contains": ["DOI"], "indices": [3]},
        "move_to_end_title_contains": [],
        "notes": "",
    }
    hide = qbr_template.resolve_hide_object_ids(plan, inv)
    assert "s2" in hide
    assert "s3" in hide
    assert "t1" not in hide


def test_compute_adapt_page_ids_includes_hidden_template_excludes_exec_and_title():
    title = "title_oid"
    exec_ids = frozenset({"e1", "e2"})
    slides = [
        {"objectId": title},
        {"objectId": "e1"},
        {"objectId": "e2"},
        {"objectId": "h1"},
        {"objectId": "t2"},
    ]
    adapt = qbr_template.compute_adapt_page_ids(slides, title, exec_ids)
    assert adapt == ["h1", "t2"]


@pytest.mark.slow
@patch.object(qbr_template, "apply_cohort_bundle_links_to_notable_signals", return_value=0)
@patch.object(qbr_template, "create_cohort_deck")
@patch.object(qbr_template, "create_health_deck")
@patch.object(qbr_template, "_find_or_create_folder", return_value="bundlefold")
@patch.object(qbr_template, "adapt_custom_slides")
@patch.object(qbr_template, "call_manifest_planner")
@patch.object(qbr_template, "llm_client")
@patch.object(qbr_template, "PendoClient")
@patch.object(qbr_template, "resolve_qbr_template_and_manifest", return_value=("tpl", "manifest"))
@patch.object(qbr_template, "get_qbr_output_folder_id", return_value="outfold")
def test_run_qbr_from_template_smoke(
    mock_out,
    mock_resolve_assets,
    mock_pendo_cls,
    mock_llm,
    mock_plan,
    mock_adapt,
    mock_find_or_create,
    mock_create_health_deck,
    mock_create_cohort_deck,
    mock_cohort_links,
):
    mock_create_health_deck.return_value = {
        "presentation_id": "companion1",
        "url": "https://docs.example/companion",
    }
    mock_create_cohort_deck.return_value = {
        "presentation_id": "cohort1",
        "url": "https://docs.example/cohort",
    }
    mock_pc = MagicMock()
    mock_pc.get_sites_by_customer.return_value = {"customer_list": ["Acme Corp", "Other"]}
    mock_pc.get_customer_health_report.return_value = {
        "customer": "Acme Corp",
        "account": {"csm": "x", "total_sites": 1, "total_visitors": 1},
        "days": 30,
    }
    mock_pendo_cls.return_value = mock_pc

    mock_plan.return_value = {
        "hide": {"title_contains": [], "indices": []},
        "move_to_end_title_contains": [],
        "notes": "none",
    }

    slides_svc = MagicMock()
    drive_svc = MagicMock()
    drive_svc.files().copy.return_value.execute.return_value = {"id": "presNEW"}

    pres_seq = {
        "slides": [
            {"objectId": "t0", "pageElements": []},
            {"objectId": "a1", "pageElements": []},
        ]
    }
    pres_final = {
        "slides": [
            {"objectId": "t0", "pageElements": []},
            {"objectId": "a1", "pageElements": []},
        ]
    }
    slides_svc.presentations().get.return_value.execute.side_effect = [pres_seq, pres_final]

    mock_gs = MagicMock(return_value=(slides_svc, drive_svc, None))
    with patch.object(qbr_template, "get_qbr_generator_folder_id_for_drive_config", return_value="gen_folder"):
        with patch.object(qbr_template, "_get_service", mock_gs):
            with patch.object(qbr_template, "_detect_customer", return_value="Acme Corp"):
                r = qbr_template.run_qbr_from_template("acme")

    assert r.get("ok") is True
    assert r["customer"] == "Acme Corp"
    assert "insert_executive_summary" not in r
    assert "exec_slides_inserted" not in r
    assert r.get("bundle_folder_id") == "bundlefold"
    assert len(r.get("companion_decks", [])) == len(qbr_template.QBR_BUNDLE_COMPANION_DECKS)
    assert mock_cohort_links.call_count >= 1
    mock_create_health_deck.assert_called()
    assert mock_create_health_deck.call_args.kwargs.get("output_folder_id") == "bundlefold"
    copy_kw = drive_svc.files().copy.call_args
    assert copy_kw[1]["body"]["parents"] == ["bundlefold"]
    mock_adapt.assert_called_once()
    _args, kwargs = mock_adapt.call_args
    adapt_ids = _args[2]
    assert adapt_ids == ["a1"]


@pytest.mark.slow
@patch.object(qbr_template, "apply_qbr_template_style_strip_after_adapt")
@patch.object(qbr_template, "find_qbr_agenda_page_id", return_value=None)
@patch.object(qbr_template, "run_qbr_adapt_hints_phase")
@patch.object(qbr_template, "resolve_deck", return_value={"slides": []})
@patch.object(qbr_template, "apply_cohort_bundle_links_to_notable_signals", return_value=0)
@patch.object(qbr_template, "create_cohort_deck")
@patch.object(qbr_template, "create_health_deck")
@patch.object(qbr_template, "_find_or_create_folder", return_value="bundlefold")
@patch.object(qbr_template, "adapt_custom_slides")
@patch.object(qbr_template, "call_manifest_planner")
@patch.object(qbr_template, "llm_client")
@patch.object(qbr_template, "PendoClient")
@patch.object(qbr_template, "resolve_qbr_template_and_manifest", return_value=("tpl", "manifest"))
@patch.object(qbr_template, "get_qbr_output_folder_id", return_value="outfold")
def test_run_qbr_from_template_main_only_skips_companions(
    mock_out,
    mock_resolve_assets,
    mock_pendo_cls,
    mock_llm,
    mock_plan,
    mock_adapt,
    mock_find_or_create,
    mock_create_health_deck,
    mock_create_cohort_deck,
    mock_cohort_links,
    mock_resolve_deck,
    mock_hints,
    mock_find_agenda,
    mock_strip,
):
    mock_create_health_deck.return_value = {
        "presentation_id": "companion1",
        "url": "https://docs.example/companion",
    }
    mock_pc = MagicMock()
    mock_pc.get_sites_by_customer.return_value = {"customer_list": ["Acme Corp", "Other"]}
    mock_pc.get_customer_health_report.return_value = {
        "customer": "Acme Corp",
        "account": {"csm": "x", "total_sites": 1, "total_visitors": 1},
        "days": 30,
    }
    mock_pendo_cls.return_value = mock_pc

    mock_plan.return_value = {
        "insert_executive_summary": True,
        "hide": {"title_contains": [], "indices": []},
        "move_to_end_title_contains": [],
        "notes": "none",
    }

    slides_svc = MagicMock()
    drive_svc = MagicMock()
    drive_svc.files().copy.return_value.execute.return_value = {"id": "presNEW"}

    pres_seq = {
        "slides": [
            {"objectId": "t0", "pageElements": []},
            {"objectId": "a1", "pageElements": []},
        ]
    }
    pres_final = {
        "slides": [
            {"objectId": "t0", "pageElements": []},
            {"objectId": "a1", "pageElements": []},
        ]
    }
    slides_svc.presentations().get.return_value.execute.side_effect = [pres_seq, pres_final]

    mock_gs = MagicMock(return_value=(slides_svc, drive_svc, None))
    with patch.object(qbr_template, "get_qbr_generator_folder_id_for_drive_config", return_value="gen_folder"):
        with patch.object(qbr_template, "_get_service", mock_gs):
            with patch.object(qbr_template, "_detect_customer", return_value="Acme Corp"):
                r = qbr_template.run_qbr_from_template("acme", companion_bundle=False)

    assert r.get("ok") is True
    assert r.get("companion_bundle") is False
    assert r.get("companion_decks") == []
    mock_create_health_deck.assert_not_called()
    mock_create_cohort_deck.assert_not_called()
    mock_cohort_links.assert_not_called()
