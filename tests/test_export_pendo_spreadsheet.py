"""Tests for Pendo export Google Sheet / xlsx workbook."""

from __future__ import annotations

from pathlib import Path

from src.export_pendo_spreadsheet import (
    build_pendo_export_workbook_tables,
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
    "people": {"champions": [{"email": "a@ford.com", "role": "Buyer"}]},
    "exports": {"total_exports": 5, "by_feature": [{"feature": "Excel export", "exports": 5}]},
    "frustration": {"total_frustration_signals": 1, "top_pages": [{"page": "Shortages", "rageClickCount": 1}]},
    "kei": {"adoption_pct": 12.5},
    "trends": {"comparison": {"total_events_pct_change": -3.1}, "weekly_active_users": [{"week_start": "2026-06-02", "active_users": 4}]},
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
    }
    sites = tables["sites"]
    assert sites[0][0] == "customerndx"
    assert sites[1][0] == "Ford"
    assert "Essex" in sites[1]


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


def test_rows_to_grid_json_encodes_nested_cell_values() -> None:
    from src.export_pendo_spreadsheet import _rows_to_grid

    grid = _rows_to_grid(
        [{"customerndx": "Ford", "section": "entry", "unused_feature": {"feature_id": "x", "name": "Widget"}}]
    )
    assert grid[1][0] == "Ford"
    assert '"feature_id"' in str(grid[1][-1])
