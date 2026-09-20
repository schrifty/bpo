"""Tests for the uncapped CSR entity-week JSON/CSV export."""

from __future__ import annotations

from src.cs_report_client import CSR_MERGED_SITE_EXPORT_COLUMNS, csr_export_column_label
from src.export_csr_entities_week import (
    CSR_ENTITIES_WEEK_STEM,
    align_csr_entities_week_rows,
    build_csr_entities_week_document,
    csr_entities_week_columns,
    render_csr_entities_week_csv,
    render_csr_entities_week_json,
    write_csr_entities_week_local,
)
from src.export_drive_layout import is_managed_export_filename, is_output_root_resident_filename, persistent_filename


def _sites_ford() -> list[dict]:
    return [
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


def _sites_safran() -> list[dict]:
    return [
        {
            "business_unit": "Seats",
            "entity": "Safran Seats France",
            "factory": "Plant A",
            "health_score": "GREEN",
            "shortages": 2,
        }
    ]


def test_csr_entities_week_columns_include_identity_and_every_csr_label() -> None:
    cols = csr_entities_week_columns()
    assert cols[:3] == ["csr_customer", "customer_exports_folder", "Site count"]
    for internal in CSR_MERGED_SITE_EXPORT_COLUMNS:
        assert csr_export_column_label(internal) in cols


def test_build_document_is_uncapped_and_aligns_all_columns(monkeypatch) -> None:
    by_name = {"Ford": _sites_ford(), "Safran SA": _sites_safran()}
    monkeypatch.setattr(
        "src.export_csr_entities_week.csr_site_entries_for_exact_week_customer",
        lambda name, _rows: by_name[name],
    )
    monkeypatch.setattr(
        "src.export_csr_entities_week.customer_exports_folder_for_csr_name",
        lambda name: "Ford" if name == "Ford" else "Safran",
    )
    monkeypatch.setattr(
        "src.export_csr_entities_week.csr_latest_report_meta",
        lambda: {"file": "customer-success-report.xlsx", "modified": "2026-09-20T04:23:03Z"},
    )
    week = [
        {"customer": "Ford", "delta": "week"},
        {"customer": "Safran SA", "delta": "week"},
        {"customer": "Ford", "delta": "month"},
    ]
    doc = build_csr_entities_week_document(week, exported_at_utc="2026-09-20T12:00:00Z")
    assert doc["grain"] == "entity"
    assert doc["delta"] == "week"
    assert doc["uncapped"] is True
    assert "field_legend" not in doc
    assert doc["row_count"] == 3
    assert doc["source"]["file"] == "customer-success-report.xlsx"
    entity_label = csr_export_column_label("entity")
    shortage_label = csr_export_column_label("shortages")
    names = {(r["csr_customer"], r.get(entity_label)) for r in doc["rows"]}
    assert names == {
        ("Ford", "Tijuana C40"),
        ("Ford", "Montreal CG0"),
        ("Safran SA", "Safran Seats France"),
    }
    tijuana = next(r for r in doc["rows"] if r.get(entity_label) == "Tijuana C40")
    assert tijuana["Site count"] == 2
    assert tijuana[shortage_label] == 15
    assert tijuana["customer_exports_folder"] == "Ford"
    aligned = align_csr_entities_week_rows(doc["rows"], doc["columns"])
    assert all(set(row) == set(doc["columns"]) for row in aligned)


def test_csv_and_json_and_local_write(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "src.export_csr_entities_week.csr_site_entries_for_exact_week_customer",
        lambda _name, _rows: _sites_safran(),
    )
    monkeypatch.setattr(
        "src.export_csr_entities_week.customer_exports_folder_for_csr_name",
        lambda _name: "Safran",
    )
    monkeypatch.setattr(
        "src.export_csr_entities_week.csr_latest_report_meta",
        lambda: {"file": "csr.xlsx", "modified": "t"},
    )
    doc = build_csr_entities_week_document([{"customer": "Safran SA", "delta": "week"}])
    raw_json = render_csr_entities_week_json(doc)
    assert '"uncapped": true' in raw_json
    csv_text = render_csr_entities_week_csv(doc)
    header = csv_text.splitlines()[0]
    assert header.startswith("csr_customer,customer_exports_folder,Site count,")
    assert "Safran SA" in csv_text
    paths = write_csr_entities_week_local(doc, tmp_path)
    assert paths["json"].endswith(f"{CSR_ENTITIES_WEEK_STEM}.json")
    assert (tmp_path / f"{CSR_ENTITIES_WEEK_STEM}.csv").is_file()
    json_dir = tmp_path / "json-only"
    json_paths = write_csr_entities_week_local(doc, json_dir, json_only=True)
    assert "csv" not in json_paths
    assert (json_dir / f"{CSR_ENTITIES_WEEK_STEM}.json").is_file()
    assert not (json_dir / f"{CSR_ENTITIES_WEEK_STEM}.csv").exists()


def test_persistent_names_stay_in_output_root() -> None:
    json_name = persistent_filename(CSR_ENTITIES_WEEK_STEM, ext=".json")
    csv_name = persistent_filename(CSR_ENTITIES_WEEK_STEM, ext=".csv")
    assert json_name == "csr-entities-week-persistent.json"
    assert is_managed_export_filename(json_name)
    assert is_output_root_resident_filename(json_name)
    assert is_output_root_resident_filename(csv_name)
