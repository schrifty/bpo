"""Tests for Pendo export Google Sheet / xlsx workbook."""

from __future__ import annotations

from pathlib import Path

from src.export_pendo_spreadsheet import (
    build_pendo_export_workbook_tables,
    upload_pendo_export_spreadsheet,
    write_pendo_export_xlsx,
)

_SAMPLE_REPORT = {
    "meta": {
        "exported_at_utc": "2026-06-29T12:00:00Z",
        "pendo_prefix": "Ford",
        "customer_query": "Ford",
        "days": 30,
        "compare_days": 30,
        "window_start": "2026-05-30",
        "window_end": "2026-06-29",
    },
    "headline": {
        "active_users_7d": 4,
        "total_visitors": 10,
        "vs_prior_period": {"total_events_pct_change": -3.1},
    },
    "engagement": {"account": {"total_sites": 2}, "signals": ["Low adoption"]},
    "sites": {"sites": [{"sitename": "Essex", "visitors": 5, "total_events": 10}]},
    "features": {"top_pages": [{"name": "Dashboard", "events": 100}]},
    "core_feature_checklist": {
        "summary": {"total_tracked": 2, "adopted": 1},
        "entries": [{"label": "Kei AI", "status": "not_adopted"}],
    },
    "unused_features": {"catalog_total": 2, "unused_count": 1, "unused_features": [{"name": "Widget"}]},
    "depth": {"total_feature_events": 50, "breakdown": [{"category": "read", "events": 40}]},
    "people": {"champions": [{"visitor_id": "v1", "email": "a@ford.com", "role": "Buyer"}]},
    "exports": {"total_exports": 5, "by_feature": [{"feature": "Excel export", "exports": 5}]},
    "frustration": {"total_frustration_signals": 1, "top_pages": [{"page": "Shortages", "rageClickCount": 1}]},
    "kei": {"adoption_pct": 12.5},
    "trends": {"comparison": {"total_events_pct_change": -3.1}, "weekly_active_users": [{"week_start": "2026-06-02", "active_users": 4}]},
    "csr": {
        "csr_loaded": True,
        "summary": {
            "factory_count": 1,
            "total_savings": 1000,
            "inventory_totals": {"on_hand": 500, "on_order": 200},
        },
        "merged_sites": [
            {
                "factory": "Plant A",
                "health_score": "GREEN",
                "shortages": 3,
                "supplier_commit_date_pct": 90.0,
            }
        ],
    },
}


def test_build_workbook_tables_has_all_tabs_and_customerndx() -> None:
    tables = build_pendo_export_workbook_tables(_SAMPLE_REPORT)
    assert set(tables) == {
        "meta",
        "headline",
        "engagement",
        "sites",
        "features",
        "core_features",
        "unused_features",
        "depth",
        "people",
        "exports",
        "frustration",
        "kei",
        "trends",
        "csr_factories",
        "csr_summary",
    }
    sites = tables["sites"]
    assert sites[0][0] == "customerndx"
    assert sites[1][0] == "Ford"
    assert "Essex" in sites[1]


def test_people_and_kei_tabs_include_visitor_id() -> None:
    report = {
        **_SAMPLE_REPORT,
        "kei": {
            "adoption_pct": 12.5,
            "users": [{"visitor_id": "v1", "email": "a@ford.com", "role": "Buyer", "queries": 3}],
        },
    }
    tables = build_pendo_export_workbook_tables(report)
    people = tables["people"]
    assert "visitor_id" in people[0]
    assert "v1" in people[1]
    kei = tables["kei"]
    assert any(row and "v1" in row for row in kei)


def test_write_pendo_export_xlsx(tmp_path: Path) -> None:
    out = tmp_path / "Ford Export (30d).xlsx"
    write_pendo_export_xlsx(out, _SAMPLE_REPORT)
    assert out.exists()
    assert out.stat().st_size > 500


def test_sheets_values_update_retries_on_429(monkeypatch) -> None:
    from unittest.mock import MagicMock

    from googleapiclient.errors import HttpError
    from httplib2 import Response

    from src.slides_api import sheets_spreadsheet_values_update

    calls = {"n": 0}
    resp = Response({"status": "429"})
    err = HttpError(resp, b'{"error": {"message": "Quota exceeded"}}')

    sheets_svc = MagicMock()
    update = sheets_svc.spreadsheets.return_value.values.return_value.update.return_value
    update.execute.side_effect = [err, None]

    monkeypatch.setattr("src.slides_api._sheets_write_interval_sec", lambda: 0.0)
    monkeypatch.setattr("src.slides_api.time.sleep", lambda _s: calls.__setitem__("n", calls["n"] + 1))

    sheets_spreadsheet_values_update(
        sheets_svc,
        spreadsheet_id="ss1",
        range_str="'trends'!A1",
        values=[["a"]],
    )
    assert update.execute.call_count == 2
    assert calls["n"] == 1


def test_sheets_spreadsheet_create_retries_on_503(monkeypatch) -> None:
    from unittest.mock import MagicMock

    from googleapiclient.errors import HttpError
    from httplib2 import Response

    from src.slides_api import sheets_spreadsheet_create

    sleeps: list[float] = []
    resp = Response({"status": "503"})
    err = HttpError(resp, b'{"error": {"message": "The service is currently unavailable."}}')

    sheets_svc = MagicMock()
    create = sheets_svc.spreadsheets.return_value.create.return_value
    create.execute.side_effect = [err, {"spreadsheetId": "ss-ok"}]

    monkeypatch.setattr("src.slides_api._sheets_write_interval_sec", lambda: 0.0)
    monkeypatch.setattr("src.slides_api.time.sleep", lambda s: sleeps.append(s))

    out = sheets_spreadsheet_create(
        sheets_svc,
        body={"properties": {"title": "t"}},
        fields="spreadsheetId",
    )
    assert out == {"spreadsheetId": "ss-ok"}
    assert create.execute.call_count == 2
    assert len(sleeps) == 1
    assert sleeps[0] >= 1.0


def test_sheets_spreadsheet_create_does_not_retry_400(monkeypatch) -> None:
    from unittest.mock import MagicMock

    import pytest
    from googleapiclient.errors import HttpError
    from httplib2 import Response

    from src.slides_api import sheets_spreadsheet_create

    resp = Response({"status": "400"})
    err = HttpError(resp, b'{"error": {"message": "Bad Request"}}')

    sheets_svc = MagicMock()
    create = sheets_svc.spreadsheets.return_value.create.return_value
    create.execute.side_effect = err

    monkeypatch.setattr("src.slides_api._sheets_write_interval_sec", lambda: 0.0)
    monkeypatch.setattr("src.slides_api.time.sleep", lambda _s: (_ for _ in ()).throw(AssertionError("no sleep")))

    with pytest.raises(HttpError):
        sheets_spreadsheet_create(
            sheets_svc,
            body={"properties": {"title": "t"}},
            fields="spreadsheetId",
        )
    assert create.execute.call_count == 1


def test_sheets_spreadsheet_get_retries_on_503(monkeypatch) -> None:
    from unittest.mock import MagicMock

    from googleapiclient.errors import HttpError
    from httplib2 import Response

    from src.slides_api import sheets_spreadsheet_get

    sleeps: list[float] = []
    resp = Response({"status": "503"})
    err = HttpError(resp, b'{"error": {"message": "The service is currently unavailable."}}')

    sheets_svc = MagicMock()
    getter = sheets_svc.spreadsheets.return_value.get.return_value
    getter.execute.side_effect = [err, {"spreadsheetId": "ss-ok", "sheets": []}]

    monkeypatch.setattr("src.slides_api._sheets_write_interval_sec", lambda: 0.0)
    monkeypatch.setattr("src.slides_api.time.sleep", lambda s: sleeps.append(s))

    out = sheets_spreadsheet_get(sheets_svc, spreadsheet_id="ss-ok", fields="spreadsheetId")
    assert out["spreadsheetId"] == "ss-ok"
    assert getter.execute.call_count == 2
    assert len(sleeps) == 1


def test_sheets_api_retries_on_read_timeout(monkeypatch) -> None:
    from unittest.mock import MagicMock

    from src.slides_api import sheets_spreadsheet_get

    sleeps: list[float] = []
    sheets_svc = MagicMock()
    getter = sheets_svc.spreadsheets.return_value.get.return_value
    getter.execute.side_effect = [TimeoutError("The read operation timed out"), {"spreadsheetId": "ss-ok"}]

    monkeypatch.setattr("src.slides_api._sheets_write_interval_sec", lambda: 0.0)
    monkeypatch.setattr("src.slides_api.time.sleep", lambda s: sleeps.append(s))

    out = sheets_spreadsheet_get(sheets_svc, spreadsheet_id="ss-ok", fields="spreadsheetId")
    assert out == {"spreadsheetId": "ss-ok"}
    assert getter.execute.call_count == 2
    assert len(sleeps) == 1


def test_rows_to_grid_json_encodes_nested_cell_values() -> None:
    from datetime import datetime

    from src.export_pendo_spreadsheet import _rows_to_grid

    grid = _rows_to_grid(
        [{"customerndx": "Ford", "section": "entry", "unused_feature": {"feature_id": "x", "name": "Widget"}}]
    )
    assert grid[1][0] == "Ford"
    assert '"feature_id"' in str(grid[1][-1])


def test_upload_pendo_export_spreadsheet_updates_existing_in_place(monkeypatch) -> None:
    from unittest.mock import MagicMock

    creates: list[object] = []
    deletes: list[str] = []
    updates: list[str] = []

    monkeypatch.setattr("src.drive_config.dedupe_duplicate_names_in_folder", lambda *_a, **_k: "ss-existing")
    monkeypatch.setattr("src.drive_config.find_file_in_folder", lambda *_a, **_k: "ss-existing")
    monkeypatch.setattr("src.charts._build_sheets_service", lambda: MagicMock())
    drive = MagicMock()
    drive.files.return_value.delete.return_value.execute.side_effect = lambda: deletes.append("deleted")
    monkeypatch.setattr("src.slides_api._get_service", lambda **_k: (None, drive, None))
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_create",
        lambda *_a, **_k: creates.append("created") or {"spreadsheetId": "new"},
    )
    monkeypatch.setattr("src.slides_api.sheets_spreadsheet_batch_update", lambda *_a, **_k: None)
    monkeypatch.setattr("src.slides_api.sheets_spreadsheet_values_clear", lambda *_a, **_k: None)
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_get",
        lambda *_a, **_k: {"sheets": [{"properties": {"title": "meta"}}]},
    )
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_values_update",
        lambda *_a, **kwargs: updates.append(str(kwargs.get("spreadsheet_id") or "")),
    )

    assert (
        upload_pendo_export_spreadsheet(_SAMPLE_REPORT, "Pendo Detailed Export  (GE, 7d)-persistent", "folder")
        == "ss-existing"
    )
    assert creates == []
    assert deletes == []
    assert "ss-existing" in updates
    drive.files.return_value.update.assert_not_called()


def test_upload_pendo_export_spreadsheet_creates_when_missing(monkeypatch) -> None:
    from unittest.mock import MagicMock

    monkeypatch.setattr("src.drive_config.dedupe_duplicate_names_in_folder", lambda *_a, **_k: None)
    monkeypatch.setattr("src.drive_config.find_file_in_folder", lambda *_a, **_k: None)
    monkeypatch.setattr("src.charts._build_sheets_service", lambda: MagicMock())
    drive = MagicMock()
    monkeypatch.setattr("src.slides_api._get_service", lambda **_k: (None, drive, None))
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_create",
        lambda *_a, **_k: {"spreadsheetId": "ss-new"},
    )
    updates: list[str] = []
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_values_update",
        lambda *_a, **kwargs: updates.append(str(kwargs.get("spreadsheet_id") or "")),
    )

    assert upload_pendo_export_spreadsheet(_SAMPLE_REPORT, "Title", "folder") == "ss-new"
    drive.files.return_value.update.assert_called()
    assert updates
    assert all(u == "ss-new" for u in updates)


def test_rows_to_grid_serializes_datetime_csr_dates() -> None:
    from datetime import datetime

    from src.export_pendo_spreadsheet import _rows_to_grid

    grid = _rows_to_grid(
        [
            {
                "customerndx": "Ford",
                "factory": "Plant A",
                "start_date": datetime(2026, 1, 1),
                "date_modified": datetime(2026, 7, 17, 14, 30, 0),
            }
        ]
    )
    assert grid[0] == ["customerndx", "factory", "start_date", "date_modified"]
    assert grid[1][2] == "2026-01-01"
    assert grid[1][3] == "2026-07-17T14:30:00"
