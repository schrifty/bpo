"""Tests for CSR customer dumps (no Drive/network)."""

from __future__ import annotations

import datetime as dt

import pytest

from src.export_csr_dump import (
    chicago_export_date,
    csr_dump_stem,
    customer_exports_folder_for_csr_name,
    infer_csr_dump_slot,
    render_csr_dump_markdown,
    rollup_csr_site_rows,
)
from src.export_drive_layout import (
    csr_dump_report_title,
    csr_report_date_label,
    historical_run_slot_label,
    is_csr_customer_success_report_filename,
    is_historical_run_slot_subfolder,
    is_managed_export_filename,
    parse_csr_report_title_date,
)
from src.cs_report_client import distinct_csr_week_customers
from src.job_runner import build_step_argv, load_job_spec


def test_csr_dump_titles_are_dated_site_bu_entity() -> None:
    day = dt.date(2026, 8, 21)
    assert csr_report_date_label(day) == "21-Aug-2026"
    assert csr_dump_report_title("site", day) == "CustomerSuccessReport-21-Aug-2026"
    assert csr_dump_report_title("bu", day) == "BU_CustomerSuccessReport-21-Aug-2026"
    assert csr_dump_report_title("entity", day) == "Entity_CustomerSuccessReport-21-Aug-2026"
    assert parse_csr_report_title_date("BU_CustomerSuccessReport-21-Aug-2026.md") == day
    assert is_csr_customer_success_report_filename("Entity_CustomerSuccessReport-21-Aug-2026")
    assert is_managed_export_filename("CustomerSuccessReport-21-Aug-2026.md")
    assert is_managed_export_filename(csr_dump_stem("Ford") + "-persistent.md")


def test_run_slot_labels() -> None:
    assert historical_run_slot_label("0600") == "0600"
    assert is_historical_run_slot_subfolder("1800")
    assert not is_historical_run_slot_subfolder("2026-08-21")
    assert not is_historical_run_slot_subfolder("2026-08")
    with pytest.raises(ValueError):
        historical_run_slot_label("0615")


def test_infer_slot_from_cdt_aligned_utc_hours() -> None:
    utc = dt.timezone.utc
    assert infer_csr_dump_slot(dt.datetime(2026, 8, 21, 5, 0, tzinfo=utc)) == "0000"
    assert infer_csr_dump_slot(dt.datetime(2026, 8, 21, 11, 0, tzinfo=utc)) == "0600"
    assert infer_csr_dump_slot(dt.datetime(2026, 8, 21, 17, 0, tzinfo=utc)) == "1200"
    assert infer_csr_dump_slot(dt.datetime(2026, 8, 21, 23, 0, tzinfo=utc)) == "1800"


def test_chicago_date_at_midnight_cdt() -> None:
    utc = dt.timezone.utc
    # 05:00 UTC = midnight CDT on 21 Aug 2026
    assert chicago_export_date(dt.datetime(2026, 8, 21, 5, 0, tzinfo=utc)) == dt.date(2026, 8, 21)


def test_folder_mapping_prefers_cohort_and_aliases() -> None:
    assert customer_exports_folder_for_csr_name("Safran SA") == "Safran"
    assert customer_exports_folder_for_csr_name("Johnson Controls") == "JCI"
    assert customer_exports_folder_for_csr_name("Cirtec Medical Corp") == "Cirtec"
    assert customer_exports_folder_for_csr_name("Unmapped Widget Co") == "Unmapped Widget Co"


def test_rollup_sums_counts_and_means_percents() -> None:
    sites = [
        {
            "business_unit": "Cabin",
            "entity": "Tijuana C40",
            "factory": "Tijuana C44",
            "health_score": "GREEN",
            "shortages": 10,
            "on_hand_value": 100,
            "clear_to_build_pct": 80.0,
        },
        {
            "business_unit": "Cabin",
            "entity": "Tijuana C40",
            "factory": "Tijuana C45",
            "health_score": "RED",
            "shortages": 5,
            "on_hand_value": 50,
            "clear_to_build_pct": 40.0,
        },
        {
            "business_unit": "Seats",
            "entity": "Montreal CG0",
            "factory": "Montreal CG1",
            "health_score": "YELLOW",
            "shortages": 1,
            "on_hand_value": 10,
            "clear_to_build_pct": 90.0,
        },
    ]
    bu = {r["business_unit"]: r for r in rollup_csr_site_rows(sites, level="bu")}
    assert bu["Cabin"]["factory_count"] == 2
    assert bu["Cabin"]["shortages"] == 15
    assert bu["Cabin"]["on_hand_value"] == 150
    assert bu["Cabin"]["clear_to_build_pct"] == 60.0
    assert bu["Cabin"]["health_score"] == "RED"
    assert "factory" not in bu["Cabin"]
    assert bu["Seats"]["factory_count"] == 1
    entities = rollup_csr_site_rows(sites, level="entity")
    assert {r["entity"] for r in entities} == {"Tijuana C40", "Montreal CG0"}
    tijuana = next(r for r in entities if r["entity"] == "Tijuana C40")
    assert tijuana["shortages"] == 15
    assert tijuana["factory_count"] == 2


def _render_md(level: str, presented, columns, **overrides) -> str:
    day = dt.date(2026, 8, 21)
    titles = {lvl: csr_dump_report_title(lvl, day) for lvl in ("site", "bu", "entity")}
    kwargs = dict(
        csr_customer="Ford",
        folder="Ford",
        slot="0600",
        export_date=day,
        level=level,
        titles=titles,
        presented=presented,
        columns=columns,
        site_count=2,
        bu_count=1,
        entity_count=2,
        source_meta={"file": "CS Report.xlsx", "modified": "2026-08-21T08:00:00.000Z"},
        spreadsheet_urls={"site": "https://docs.google.com/spreadsheets/d/abc/edit"},
        mapped=True,
    )
    kwargs.update(overrides)
    return render_csr_dump_markdown(**kwargs)


def test_render_csr_dump_markdown_carries_grain_rows() -> None:
    presented = [
        {"Factory": "Chicago Assembly", "Shortages": 12},
        {"Factory": "Dearborn Truck", "Shortages": 3},
    ]
    md = _render_md("site", presented, ["Factory", "Shortages"])
    assert "site level" in md
    assert "| Factory | Shortages |" in md
    assert "| Chicago Assembly | 12 |" in md
    assert "| Dearborn Truck | 3 |" in md
    assert "BU_CustomerSuccessReport-21-Aug-2026" in md
    assert "Entity_CustomerSuccessReport-21-Aug-2026" in md
    assert "`0600`" in md
    assert "Historical Data/2026-08-21/0600/" in md
    assert "abc" in md
    assert "Join note" not in md
    # Site grain is raw CSR data, so the rollup caveat does not belong there.
    assert "unweighted means" not in md


def test_render_csr_dump_markdown_bu_notes_rollup_and_differs_from_site() -> None:
    site_md = _render_md("site", [{"Factory": "Chicago Assembly"}], ["Factory"])
    bu_md = _render_md("bu", [{"Business Unit": "Ford Blue", "Site count": 2}], ["Business Unit", "Site count"])
    assert "business unit level" in bu_md
    assert "| Ford Blue | 2 |" in bu_md
    assert "unweighted means" in bu_md
    assert bu_md != site_md


def test_render_csr_dump_markdown_escapes_pipes_and_handles_empty() -> None:
    md = _render_md("entity", [{"Entity": "A | B"}], ["Entity"])
    assert "| A \\| B |" in md
    empty = _render_md("entity", [], ["Entity"])
    assert "No rows at this grain" in empty


def test_build_step_argv_and_job_specs() -> None:
    assert build_step_argv({"command": "export-csr", "slot": "0600"}) == [
        "--export-csr",
        "--slot",
        "0600",
    ]
    spec = load_job_spec("csr-customer-dump-0600")
    assert spec.steps[0]["command"] == "export-csr"
    assert spec.steps[0]["slot"] == "0600"
    assert load_job_spec("csr-customer-dump-0000").steps[0]["slot"] == "0000"
    assert load_job_spec("csr-customer-dump-1800").steps[0]["slot"] == "1800"


def test_distinct_csr_week_customers_filters_delta() -> None:
    rows = [
        {"customer": "Ford", "delta": "week"},
        {"customer": "Ford", "delta": "week"},
        {"customer": "Safran SA", "delta": "month"},
        {"customer": "Safran SA", "delta": "week"},
        {"customer": "", "delta": "week"},
    ]
    assert distinct_csr_week_customers(rows) == ["Ford", "Safran SA"]
