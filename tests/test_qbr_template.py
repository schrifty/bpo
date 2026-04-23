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
        "insert_executive_summary": "true",
        "hide": {"title_contains": [" DOI "], "indices": ["3", 4, 5.0]},
        "move_to_end_title_contains": ["summary"],
        "notes": "ok",
    }
    p = qbr_template._normalize_manifest_plan(raw)
    assert p["insert_executive_summary"] is True
    assert p["hide"]["title_contains"] == ["DOI"]
    assert p["hide"]["indices"] == [3, 4, 5]
    assert p["move_to_end_title_contains"] == ["summary"]


def test_normalize_manifest_plan_defaults_no_exec_insert():
    p = qbr_template._normalize_manifest_plan({})
    assert p["insert_executive_summary"] is False


def test_resolve_hide_object_ids_skips_title():
    inv = [
        {"index": 1, "title": "Cover", "objectId": "t1"},
        {"index": 2, "title": "DOI metrics", "objectId": "s2"},
        {"index": 3, "title": "Agenda", "objectId": "s3"},
    ]
    plan = {
        "insert_executive_summary": False,
        "hide": {"title_contains": ["DOI"], "indices": [3]},
        "move_to_end_title_contains": [],
        "notes": "",
    }
    hide = qbr_template.resolve_hide_object_ids(plan, inv)
    assert "s2" in hide
    assert "s3" in hide
    assert "t1" not in hide


@patch.object(qbr_template, "set_speaker_notes", return_value=True)
@patch.object(qbr_template, "_apply_slide_skipped")
@patch.object(qbr_template, "resolve_deck")
def test_insert_executive_summary_marks_slides_skipped(mock_resolve, mock_apply_skip, _mock_notes):
    """Inserted exec-summary slides get isSkipped so Present skips them from the start."""
    mock_resolve.return_value = {"slides": [{"id": "title", "slide_type": "title"}]}

    def fake_title(reqs, sid, _report, idx):
        reqs.append({"createSlide": {"objectId": sid, "insertionIndex": idx}})
        return idx + 1

    slides_svc = MagicMock()
    report = {
        "customer": "Acme",
        "account": {"csm": "x", "total_sites": 1, "total_visitors": 1},
        "days": 30,
        "generated": "2026-01-01",
        "quarter": "Q1",
        "quarter_start": "2026-01-01",
        "quarter_end": "2026-03-31",
    }
    with patch.object(qbr_template, "_SLIDE_BUILDERS", {"title": fake_title}):
        ids, built, sig_pages = qbr_template._insert_executive_summary_slides(
            slides_svc, "pres123", report, "Acme"
        )
    assert ids == ["qbr_es_title_1"]
    assert built == 1
    assert sig_pages == []
    mock_apply_skip.assert_called_once()
    _svc, pres_id, oids = mock_apply_skip.call_args[0]
    assert pres_id == "pres123"
    assert oids == {"qbr_es_title_1"}
    slides_svc.presentations.return_value.batchUpdate.assert_called()


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
@patch.object(qbr_template, "_insert_executive_summary_slides", return_value=(["e1", "e2"], 2, []))
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
    mock_insert,
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
            {"objectId": "e1", "pageElements": []},
            {"objectId": "e2", "pageElements": []},
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
@patch.object(qbr_template, "apply_cohort_bundle_links_to_notable_signals", return_value=0)
@patch.object(qbr_template, "create_cohort_deck")
@patch.object(qbr_template, "create_health_deck")
@patch.object(qbr_template, "_find_or_create_folder", return_value="bundlefold")
@patch.object(qbr_template, "adapt_custom_slides")
@patch.object(qbr_template, "_insert_executive_summary_slides")
@patch.object(qbr_template, "call_manifest_planner")
@patch.object(qbr_template, "llm_client")
@patch.object(qbr_template, "PendoClient")
@patch.object(qbr_template, "resolve_qbr_template_and_manifest", return_value=("tpl", "manifest"))
@patch.object(qbr_template, "get_qbr_output_folder_id", return_value="outfold")
def test_run_qbr_skips_exec_insert_when_manifest_false(
    mock_out,
    mock_resolve_assets,
    mock_pendo_cls,
    mock_llm,
    mock_plan,
    mock_insert,
    mock_adapt,
    mock_find_or_create,
    mock_create_health_deck,
    mock_create_cohort_deck,
    mock_cohort_links,
):
    mock_create_health_deck.return_value = {"presentation_id": "x", "url": "https://x"}
    mock_create_cohort_deck.return_value = {"presentation_id": "c", "url": "https://c"}
    mock_pc = MagicMock()
    mock_pc.get_sites_by_customer.return_value = {"customer_list": ["Acme Corp"]}
    mock_pc.get_customer_health_report.return_value = {
        "customer": "Acme Corp",
        "account": {"csm": "x", "total_sites": 1, "total_visitors": 1},
        "days": 30,
    }
    mock_pendo_cls.return_value = mock_pc

    mock_plan.return_value = {
        "insert_executive_summary": False,
        "hide": {"title_contains": [], "indices": []},
        "move_to_end_title_contains": [],
        "notes": "no exec",
    }

    slides_svc = MagicMock()
    drive_svc = MagicMock()
    drive_svc.files().copy.return_value.execute.return_value = {"id": "presX"}
    pres = {
        "slides": [
            {"objectId": "t0", "pageElements": []},
            {"objectId": "a1", "pageElements": []},
        ]
    }
    slides_svc.presentations().get.return_value.execute.side_effect = [pres, pres]

    mock_gs = MagicMock(return_value=(slides_svc, drive_svc, None))
    with patch.object(qbr_template, "get_qbr_generator_folder_id_for_drive_config", return_value="gen_folder"):
        with patch.object(qbr_template, "_get_service", mock_gs):
            with patch.object(qbr_template, "_detect_customer", return_value="Acme Corp"):
                r = qbr_template.run_qbr_from_template("acme")

    assert r.get("ok") is True
    mock_insert.assert_not_called()
    _args, _ = mock_adapt.call_args
    assert _args[2] == ["a1"]
