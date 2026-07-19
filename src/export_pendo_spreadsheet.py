"""Google Sheet (and local .xlsx) export for customer Pendo usage reports."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .config import logger

# Top-level report keys → tab titles (≤100 chars, Sheets-safe).
_PENDO_EXPORT_TABS: tuple[tuple[str, str], ...] = (
    ("meta", "meta"),
    ("headline", "headline"),
    ("engagement", "engagement"),
    ("sites", "sites"),
    ("features", "features"),
    ("core_feature_checklist", "core_features"),
    ("unused_features", "unused_features"),
    ("depth", "depth"),
    ("people", "people"),
    ("exports", "exports"),
    ("frustration", "frustration"),
    ("kei", "kei"),
    ("trends", "trends"),
    ("site_detail", "site_detail"),
    ("user_roster", "user_roster"),
    ("csr", "csr_factories"),
    ("csr_summary", "csr_summary"),
)

_SHEET_TITLE_BAD = re.compile(r"[:\\/?*\[\]]")


def _safe_sheet_title(name: str) -> str:
    title = _SHEET_TITLE_BAD.sub("_", (name or "sheet").strip())[:100]
    return title or "sheet"


def _customerndx(report: dict[str, Any]) -> str:
    meta = report.get("meta") or {}
    return str(meta.get("pendo_prefix") or meta.get("customer_query") or "customer")


def _flatten_scalars(value: Any, *, prefix: str = "") -> dict[str, Any]:
    """Flatten nested dicts into dot-column names; skip lists (handled as separate rows)."""
    if not isinstance(value, dict):
        return {prefix: value} if prefix else {}
    out: dict[str, Any] = {}
    for key, raw in value.items():
        col = f"{prefix}.{key}" if prefix else str(key)
        if raw is None:
            out[col] = ""
        elif isinstance(raw, dict):
            out.update(_flatten_scalars(raw, prefix=col))
        elif isinstance(raw, list):
            continue
        else:
            out[col] = raw
    return out


def _cell_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.isoformat()
        if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _rows_to_grid(rows: list[dict[str, Any]]) -> list[list[Any]]:
    if not rows:
        return [["customerndx"], [""]]
    columns: list[str] = ["customerndx"]
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    grid: list[list[Any]] = [columns]
    for row in rows:
        grid.append([_cell_value(row.get(col, "")) for col in columns])
    return grid


def _build_meta_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    meta = report.get("meta") or {}
    return [{"customerndx": customer, **_flatten_scalars(meta)}]


def _build_headline_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    headline = report.get("headline") or {}
    return [{"customerndx": customer, **_flatten_scalars(headline)}]


def _build_engagement_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    eng = report.get("engagement") or {}
    rows: list[dict[str, Any]] = []
    for section, payload in (
        ("account", eng.get("account")),
        ("engagement", eng.get("engagement")),
        ("benchmarks", eng.get("benchmarks")),
    ):
        if isinstance(payload, dict) and payload:
            rows.append({"customerndx": customer, "section": section, **_flatten_scalars(payload)})
    for idx, signal in enumerate(eng.get("signals") or []):
        if isinstance(signal, dict):
            rows.append({"customerndx": customer, "section": "signal", "signal_index": idx, **_flatten_scalars(signal)})
        else:
            rows.append({"customerndx": customer, "section": "signal", "signal_index": idx, "text": signal})
    return rows


def _build_sites_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    sites = (report.get("sites") or {}).get("sites") or []
    return [{"customerndx": customer, **{k: v for k, v in s.items() if not isinstance(v, (dict, list))}} for s in sites if isinstance(s, dict)]


def _build_features_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    feat = report.get("features") or {}
    rows: list[dict[str, Any]] = []
    for section, key in (("top_pages", "top_pages"), ("top_features", "top_features")):
        for item in feat.get(key) or []:
            if isinstance(item, dict):
                rows.append({"customerndx": customer, "section": section, **item})
    insights = feat.get("feature_adoption_insights")
    if isinstance(insights, dict) and insights:
        rows.append({"customerndx": customer, "section": "adoption_insights", **_flatten_scalars(insights)})
    return rows


def _build_core_feature_checklist_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    checklist = report.get("core_feature_checklist") or {}
    rows: list[dict[str, Any]] = []
    summary = checklist.get("summary") or {}
    if summary:
        rows.append({"customerndx": customer, "section": "summary", **summary})
    for entry in checklist.get("entries") or []:
        if isinstance(entry, dict):
            rows.append({"customerndx": customer, "section": "entry", **entry})
    return rows


def _build_unused_features_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    unused = report.get("unused_features") or {}
    rows: list[dict[str, Any]] = []
    rows.append(
        {
            "customerndx": customer,
            "section": "summary",
            "catalog_total": unused.get("catalog_total"),
            "unused_count": unused.get("unused_count"),
            "truncated": unused.get("truncated"),
        }
    )
    for item in unused.get("unused_features") or []:
        if isinstance(item, dict):
            rows.append({"customerndx": customer, "section": "unused", **item})
        else:
            rows.append({"customerndx": customer, "section": "unused", "name": item})
    return rows


def _build_depth_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    depth = report.get("depth") or {}
    if depth.get("error"):
        return [{"customerndx": customer, "section": "error", "error": depth.get("error")}]
    rows: list[dict[str, Any]] = [
        {
            "customerndx": customer,
            "section": "summary",
            "total_feature_events": depth.get("total_feature_events"),
            "active_users": depth.get("active_users"),
            "write_ratio": depth.get("write_ratio"),
            "read_events": depth.get("read_events"),
            "write_events": depth.get("write_events"),
            "collab_events": depth.get("collab_events"),
        }
    ]
    for item in depth.get("breakdown") or []:
        if isinstance(item, dict):
            rows.append({"customerndx": customer, "section": "breakdown", **item})
    return rows


def _build_people_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    people = report.get("people") or {}
    if people.get("error"):
        return [{"customerndx": customer, "section": "error", "error": people.get("error")}]
    rows: list[dict[str, Any]] = []
    for cohort, key in (("champions", "champions"), ("at_risk", "at_risk_users")):
        for person in people.get(key) or []:
            if isinstance(person, dict):
                rows.append({"customerndx": customer, "cohort": cohort, **person})
    return rows


def _build_exports_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    exports = report.get("exports") or {}
    if exports.get("error"):
        return [{"customerndx": customer, "section": "error", "error": exports.get("error")}]
    rows: list[dict[str, Any]] = [
        {
            "customerndx": customer,
            "section": "summary",
            "total_exports": exports.get("total_exports"),
            "exports_per_active_user": exports.get("exports_per_active_user"),
            "active_users": exports.get("active_users"),
        }
    ]
    for item in exports.get("by_feature") or []:
        if isinstance(item, dict):
            rows.append({"customerndx": customer, "section": "by_feature", **item})
    for item in exports.get("top_exporters") or []:
        if isinstance(item, dict):
            rows.append({"customerndx": customer, "section": "top_exporter", **item})
    return rows


def _build_frustration_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    fr = report.get("frustration") or {}
    if fr.get("error"):
        return [{"customerndx": customer, "section": "error", "error": fr.get("error")}]
    rows: list[dict[str, Any]] = []
    totals = fr.get("totals") or {}
    if totals or fr.get("total_frustration_signals") is not None:
        rows.append(
            {
                "customerndx": customer,
                "section": "totals",
                "total_frustration_signals": fr.get("total_frustration_signals"),
                **totals,
            }
        )
    for section, key in (("top_pages", "top_pages"), ("top_features", "top_features")):
        for item in fr.get(key) or []:
            if isinstance(item, dict):
                rows.append({"customerndx": customer, "section": section, **item})
    return rows


def _build_kei_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    kei = report.get("kei") or {}
    if not kei or kei.get("error"):
        return [{"customerndx": customer, "note": kei.get("error") or "no Kei data"}]
    return [{"customerndx": customer, **_flatten_scalars(kei)}]


def _build_trends_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    trends = report.get("trends") or {}
    rows: list[dict[str, Any]] = []
    comparison = trends.get("comparison") or {}
    if comparison:
        rows.append({"customerndx": customer, "section": "comparison", **_flatten_scalars(comparison)})
    for period_key in ("current_period", "prior_period"):
        period = trends.get(period_key)
        if isinstance(period, dict) and period:
            rows.append({"customerndx": customer, "section": period_key, **_flatten_scalars(period)})
    for week in trends.get("weekly_active_users") or []:
        if isinstance(week, dict):
            rows.append({"customerndx": customer, "section": "weekly", **week})
    return rows


def _build_site_detail_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for site in report.get("site_detail") or []:
        if not isinstance(site, dict):
            continue
        cur = site.get("activity_current") or {}
        prior = site.get("activity_prior") or {}
        cmp_ = site.get("activity_pct_change") or {}
        eng = site.get("engagement") or {}
        rows.append(
            {
                "customerndx": customer,
                "section": "site_summary",
                "sitename": site.get("sitename"),
                "visitors": site.get("visitors"),
                "users_total": site.get("users_total"),
                "active_7d": eng.get("active_7d"),
                "active_30d": eng.get("active_30d"),
                "dormant": eng.get("dormant"),
                "total_events": cur.get("total_events"),
                "page_minutes": cur.get("page_minutes"),
                "feature_events": cur.get("feature_events"),
                "prior_total_events": prior.get("total_events"),
                "events_pct_change": cmp_.get("total_events"),
            }
        )
        for item in site.get("top_pages") or []:
            if isinstance(item, dict):
                rows.append({"customerndx": customer, "section": "site_top_page", "sitename": site.get("sitename"), **item})
        for item in site.get("top_features") or []:
            if isinstance(item, dict):
                rows.append({"customerndx": customer, "section": "site_top_feature", "sitename": site.get("sitename"), **item})
        for user in site.get("users") or []:
            if isinstance(user, dict):
                rows.append({"customerndx": customer, "section": "site_user", "sitename": site.get("sitename"), **user})
    return rows


def _build_user_roster_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for user in report.get("user_roster") or []:
        if not isinstance(user, dict):
            continue
        row = {"customerndx": customer, **{k: v for k, v in user.items() if k != "sites"}}
        sites = user.get("sites")
        if isinstance(sites, list):
            row["sites"] = ", ".join(str(s) for s in sites if s)
        rows.append(row)
    return rows


def _build_csr_factory_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    csr = report.get("csr")
    if not isinstance(csr, dict) or not csr.get("csr_loaded"):
        return []
    from .cs_report_client import csr_merged_site_export_columns

    merged = csr.get("merged_sites") if isinstance(csr.get("merged_sites"), list) else []
    columns = csr_merged_site_export_columns(merged)
    rows: list[dict[str, Any]] = []
    for site in merged:
        if not isinstance(site, dict):
            continue
        row = {"customerndx": customer}
        for col in columns:
            row[col] = site.get(col, "")
        rows.append(row)
    return rows


def _build_csr_summary_rows(report: dict[str, Any], customer: str) -> list[dict[str, Any]]:
    csr = report.get("csr")
    if not isinstance(csr, dict):
        return []
    summary = csr.get("summary") if isinstance(csr.get("summary"), dict) else {}
    rows: list[dict[str, Any]] = [{"customerndx": customer, "section": "summary", **_flatten_scalars(summary)}]
    inv = summary.get("inventory_totals")
    if isinstance(inv, dict):
        rows.append(
            {
                "customerndx": customer,
                "section": "inventory_totals",
                **_flatten_scalars(inv, prefix="inventory_totals"),
            }
        )
    return rows


_TAB_BUILDERS = {
    "meta": _build_meta_rows,
    "headline": _build_headline_rows,
    "engagement": _build_engagement_rows,
    "sites": _build_sites_rows,
    "features": _build_features_rows,
    "core_feature_checklist": _build_core_feature_checklist_rows,
    "unused_features": _build_unused_features_rows,
    "depth": _build_depth_rows,
    "people": _build_people_rows,
    "exports": _build_exports_rows,
    "frustration": _build_frustration_rows,
    "kei": _build_kei_rows,
    "trends": _build_trends_rows,
    "site_detail": _build_site_detail_rows,
    "user_roster": _build_user_roster_rows,
    "csr": _build_csr_factory_rows,
    "csr_summary": _build_csr_summary_rows,
}


def build_pendo_export_workbook_tables(report: dict[str, Any]) -> dict[str, list[list[Any]]]:
    """Return tab title → grid (including header row) for all export sections."""
    customer = _customerndx(report)
    tables: dict[str, list[list[Any]]] = {}
    for report_key, tab_title in _PENDO_EXPORT_TABS:
        if report_key in ("site_detail", "user_roster") and not report.get(report_key):
            continue
        if report_key in ("csr", "csr_summary") and not (report.get("csr") or {}).get("csr_loaded"):
            continue
        builder = _TAB_BUILDERS[report_key]
        rows = builder(report, customer)
        tables[_safe_sheet_title(tab_title)] = _rows_to_grid(rows)
    return tables


def write_pendo_export_xlsx(path: Path, report: dict[str, Any]) -> None:
    """Write a multi-tab .xlsx workbook (local / --no-drive)."""
    from openpyxl import Workbook

    tables = build_pendo_export_workbook_tables(report)
    wb = Workbook()
    default = wb.active
    first = True
    for tab_title, grid in tables.items():
        ws = default if first else wb.create_sheet(title=tab_title)
        if first:
            ws.title = tab_title
            first = False
        for row in grid:
            ws.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def upload_pendo_export_spreadsheet(report: dict[str, Any], title: str, folder_id: str) -> str:
    """Create or replace a Google Sheet in ``folder_id``. Returns spreadsheet file id."""
    from googleapiclient.errors import HttpError

    from .charts import _build_sheets_service
    from .drive_config import dedupe_duplicate_names_in_folder, drive_api_lock, find_file_in_folder
    from .slides_api import _get_service

    mime = "application/vnd.google-apps.spreadsheet"
    dedupe_duplicate_names_in_folder(folder_id, title)
    existing = find_file_in_folder(title, folder_id, mime_type=mime)
    if existing:
        with drive_api_lock:
            _, drive, _ = _get_service()
            try:
                drive.files().delete(fileId=existing).execute()
            except HttpError as exc:
                logger.warning("Could not remove prior Pendo export spreadsheet %s: %s", existing, exc)

    tables = build_pendo_export_workbook_tables(report)
    sheets_svc = _build_sheets_service()
    _, drive_svc, _ = _get_service()

    from .slides_api import sheets_spreadsheet_create, sheets_spreadsheet_values_update

    sheet_defs = [{"properties": {"title": tab}} for tab in tables]
    ss = sheets_spreadsheet_create(
        sheets_svc,
        body={"properties": {"title": title}, "sheets": sheet_defs},
        fields="spreadsheetId,sheets.properties.title",
    )
    ss_id = ss["spreadsheetId"]

    with drive_api_lock:
        drive_svc.files().update(fileId=ss_id, addParents=folder_id, fields="id,parents").execute()

    for tab_title, grid in tables.items():
        sheets_spreadsheet_values_update(
            sheets_svc,
            spreadsheet_id=ss_id,
            range_str=f"'{tab_title}'!A1",
            values=grid,
        )

    logger.info("Created Pendo export spreadsheet %s (%s)", ss_id, title)
    return ss_id


def spreadsheet_url(file_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
