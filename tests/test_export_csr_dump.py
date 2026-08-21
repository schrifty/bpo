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
)
from src.export_drive_layout import (
    historical_run_slot_label,
    is_historical_run_slot_subfolder,
    is_managed_export_filename,
)
from src.cs_report_client import distinct_csr_week_customers
from src.job_runner import build_step_argv, load_job_spec


def test_csr_dump_stem_and_managed_filename() -> None:
    stem = csr_dump_stem("Ford")
    assert stem == "Ford CSR Dump"
    assert is_managed_export_filename(f"{stem}-persistent.md")
    assert is_managed_export_filename(stem)
    assert is_managed_export_filename(f"{stem}-persistent")


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


def test_render_csr_dump_markdown_index() -> None:
    md = render_csr_dump_markdown(
        csr_customer="Ford",
        folder="Ford",
        slot="0600",
        export_date=dt.date(2026, 8, 21),
        factory_count=12,
        source_meta={"file": "CS Report.xlsx", "modified": "2026-08-21T08:00:00.000Z"},
        spreadsheet_url="https://docs.google.com/spreadsheets/d/abc/edit",
        mapped=True,
    )
    assert "Ford CSR Dump" in md
    assert "`0600`" in md
    assert "Historical Data/2026-08-21/0600/" in md
    assert "12" in md
    assert "abc" in md
    assert "Join note" not in md


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
