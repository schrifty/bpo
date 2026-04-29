"""Salesforce comprehensive deck slide builders."""

from __future__ import annotations

import json
from typing import Any

from .slide_primitives import (
    background as _bg,
    missing_data_slide as _missing_data_slide,
    simple_table as _simple_table,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import (
    append_slide as _slide,
    append_text_box as _box,
    append_wrapped_text_box as _wrap_box,
)
from .slides_theme import BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, LIGHT, MARGIN, NAVY, _cap_chunk_list

SF_TABLE_MAX_COLS = 5
SF_TABLE_MAX_CELL_CHARS = 24
SF_TABLE_MAX_ROWS_PER_PAGE = 7
SF_TABLE_ROW_H = 30.0


def sf_format_cell(val: Any, max_len: int = SF_TABLE_MAX_CELL_CHARS) -> str:
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        value = json.dumps(val, default=str)
    else:
        value = str(val)
    value = " ".join(value.split())
    if len(value) > max_len:
        return value[: max_len - 1] + "…"
    return value


def sf_records_to_table(
    records: list[dict[str, Any]],
    *,
    max_cols: int = SF_TABLE_MAX_COLS,
    max_rows: int = SF_TABLE_MAX_ROWS_PER_PAGE,
) -> tuple[list[str], list[list[str]]]:
    if not records:
        return [], []
    keys: list[str] = []
    for rec in records[:40]:
        for key in rec.keys():
            if key not in keys:
                keys.append(key)
    keys = keys[:max_cols]
    rows = [[sf_format_cell(rec.get(key)) for key in keys] for rec in records[:max_rows]]
    headers = [sf_format_cell(key, max_len=20) for key in keys]
    return headers, rows


def _sf_record_keys(records: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    for rec in records[:40]:
        for key in rec.keys():
            if key not in keys:
                keys.append(key)
    return keys


def sf_category_records(sfc: dict[str, Any], category: str) -> list[dict[str, Any]]:
    """Resolve records for a deck ``sf_category``."""
    category = (category or "").strip()
    if category == "entity_accounts":
        return list(sfc.get("accounts") or [])
    return list((sfc.get("categories") or {}).get(category) or [])


def filter_salesforce_comprehensive_slide_plan(
    slide_plan: list[dict[str, Any]],
    sfc: dict[str, Any],
) -> list[dict[str, Any]]:
    """Drop ``salesforce_category`` entries with no rows."""
    out: list[dict[str, Any]] = []
    for entry in slide_plan:
        slide_type = entry.get("slide_type", entry.get("id", ""))
        if slide_type != "salesforce_category":
            out.append(entry)
            continue
        if sf_category_records(sfc, entry.get("sf_category") or ""):
            out.append(entry)
    return out


def salesforce_comprehensive_cover_slide(reqs: list, sid: str, report: dict[str, Any], idx: int) -> int:
    """Intro slide for the Salesforce comprehensive deck."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Salesforce Comprehensive Export")
    sfc = report.get("salesforce_comprehensive") or {}
    customer = report.get("customer", "")
    parts: list[str] = []
    error = sfc.get("error")
    if error:
        parts.append(f"Setup: {error}")
    if not sfc.get("matched"):
        parts.append(f'No Customer Entity account matched for "{customer}".')
    else:
        account_count = len(sfc.get("accounts") or [])
        parts.append(f"Customer: {customer}")
        parts.append(f"Matched {account_count} Customer Entity account(s).")
        expanded_ids = sfc.get("account_ids_expanded") or sfc.get("account_ids") or []
        if len(expanded_ids) > account_count:
            parts.append(
                f"Queries include {len(expanded_ids)} account Id(s) (entity row(s) plus child accounts via ParentId)."
            )
        elif expanded_ids:
            parts.append(f"Queries scoped to {len(expanded_ids)} account Id(s).")
        row_limit = sfc.get("row_limit", 75)
        parts.append(
            f"Each related object is capped at ~{row_limit} rows (API first page); not a full data warehouse export."
        )
    parts.append("Products and price books are org-wide samples (not filtered to this account).")
    body = "\n".join(parts)
    object_id = f"{sid}_body"
    body_h = max(80.0, BODY_BOTTOM - BODY_Y - 8)
    _wrap_box(reqs, object_id, sid, MARGIN, BODY_Y, CONTENT_W, body_h, body)
    _style(reqs, object_id, 0, len(body), size=11, color=NAVY, font=FONT)
    return idx + 1


def _salesforce_toc_label(entry: dict[str, Any]) -> str:
    title = str(entry.get("title") or "").strip()
    if title.lower().startswith("salesforce"):
        title = title.split("—", 1)[-1].strip() if "—" in title else title
        title = title.split("-", 1)[-1].strip() if title.lower().startswith("salesforce -") else title
    return title or str(entry.get("sf_category") or entry.get("id") or "Section").replace("_", " ").title()


def salesforce_comprehensive_toc_slide(reqs: list, sid: str, report: dict[str, Any], idx: int) -> int:
    """Table of contents for categories retained in the comprehensive export."""
    plan = list(report.get("_slide_plan") or [])
    entries = [
        entry
        for entry in plan
        if entry.get("slide_type") in ("salesforce_category", "data_quality")
    ]
    if not entries:
        return _missing_data_slide(reqs, sid, report, idx, "Salesforce table of contents entries")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "").strip() or "Table of Contents"
    _slide_title(reqs, sid, title)

    customer = (report.get("customer") or "").strip()
    subtitle = (
        f"Sections included for {customer} after empty Salesforce categories are omitted."
        if customer
        else "Sections included after empty Salesforce categories are omitted."
    )
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 20, subtitle)
    _style(reqs, f"{sid}_sub", 0, len(subtitle), size=10, color=GRAY, font=FONT)

    max_items = 20
    labels = [_salesforce_toc_label(e) for e in entries[:max_items]]
    left = labels[:10]
    right = labels[10:]
    col_gap = 28.0
    col_w = (CONTENT_W - col_gap) / 2
    y0 = BODY_Y + 38
    row_h = 20.0

    def _render_col(items: list[str], start_num: int, x: float) -> None:
        for row_index, label in enumerate(items):
            n = start_num + row_index
            line = f"{n}.   {label}"
            oid = f"{sid}_toc_{n}"
            _box(reqs, oid, sid, x, y0 + row_index * row_h, col_w, row_h, line)
            _style(reqs, oid, 0, len(line), size=11, color=NAVY, font=FONT)
            _style(reqs, oid, 0, len(str(n)) + 1, bold=True)

    _render_col(left, 1, MARGIN)
    _render_col(right, 1 + len(left), MARGIN + col_w + col_gap)

    if len(entries) > max_items:
        note = f"{len(entries) - max_items} additional section(s) omitted from this contents slide."
        _box(reqs, f"{sid}_more", sid, MARGIN, BODY_BOTTOM - 18, CONTENT_W, 16, note)
        _style(reqs, f"{sid}_more", 0, len(note), size=8, color=GRAY, font=FONT)

    return idx + 1


def salesforce_category_slide(reqs: list, sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    """One table per mainstream Salesforce category."""
    entry = report.get("_current_slide") or {}
    category = (entry.get("sf_category") or "").strip()
    title = (entry.get("title") or category.replace("_", " ").title())[:100]
    sfc = report.get("salesforce_comprehensive") or {}

    if "salesforce_comprehensive" not in report:
        return _missing_data_slide(reqs, sid, report, idx, "salesforce_comprehensive payload")

    if not category:
        return _missing_data_slide(reqs, sid, report, idx, "sf_category not set on slide")

    records = sf_category_records(sfc, category)
    error_note = (sfc.get("category_errors") or {}).get(category)

    # Slides grows table rows when 9pt text wraps in narrow cells; keep this conservative.
    row_h = SF_TABLE_ROW_H
    y0 = BODY_Y + (38 if error_note else 0)
    bottom_pad = 34.0
    avail_h = BODY_BOTTOM - y0 - bottom_pad
    rows_per_page = min(SF_TABLE_MAX_ROWS_PER_PAGE, max(2, int(avail_h // row_h) - 1))

    if not records:
        _slide(reqs, sid, idx)
        _bg(reqs, sid, LIGHT)
        _slide_title(reqs, sid, title)
        y = BODY_Y
        if error_note:
            banner = f"Query error: {error_note[:140]}"
            banner_id = f"{sid}_warn"
            _box(reqs, banner_id, sid, MARGIN, y, CONTENT_W, 32, banner)
            _style(reqs, banner_id, 0, len(banner), size=8, color=GRAY, font=FONT)
            y += 38
        msg = "No records for this category." + (" (see query error above)" if error_note else "")
        empty_id = f"{sid}_empty"
        _box(reqs, empty_id, sid, MARGIN, y, CONTENT_W, 36, msg)
        _style(reqs, empty_id, 0, len(msg), size=11, color=NAVY, font=FONT)
        return idx + 1

    chunks = _cap_chunk_list(
        [records[i: i + rows_per_page] for i in range(0, len(records), rows_per_page)]
    )
    object_ids: list[str] = []
    for page_index, chunk in enumerate(chunks):
        page_sid = f"{sid}_p{page_index}" if len(chunks) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        _bg(reqs, page_sid, LIGHT)
        page_title = title if len(chunks) == 1 else f"{title} ({page_index + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, page_title)
        y = BODY_Y
        if page_index == 0 and error_note:
            banner = f"Query error: {error_note[:140]}"
            banner_id = f"{page_sid}_warn"
            _box(reqs, banner_id, page_sid, MARGIN, y, CONTENT_W, 32, banner)
            _style(reqs, banner_id, 0, len(banner), size=8, color=GRAY, font=FONT)
            y += 38
        all_keys = _sf_record_keys(records)
        headers, rows = sf_records_to_table(chunk, max_rows=len(chunk))
        if not headers:
            continue
        column_count = len(headers)
        col_w = CONTENT_W / max(1, column_count)
        col_widths = [col_w] * column_count
        _simple_table(reqs, f"{page_sid}_tbl", page_sid, MARGIN, y, col_widths, row_h, headers, rows)
        omitted_cols = max(0, len(all_keys) - len(headers))
        omitted_rows = max(0, len(records) - (page_index * rows_per_page + len(chunk)))
        note_parts: list[str] = []
        if omitted_cols:
            note_parts.append(f"{omitted_cols} column(s) omitted for fit")
        if omitted_rows:
            note_parts.append(f"{omitted_rows} additional row(s) continue on following page(s)")
        if note_parts:
            note = "; ".join(note_parts) + "."
            note_y = min(BODY_BOTTOM - 18, y + (1 + len(rows)) * row_h + 6)
            _box(reqs, f"{page_sid}_fit_note", page_sid, MARGIN, note_y, CONTENT_W, 14, note)
            _style(reqs, f"{page_sid}_fit_note", 0, len(note), size=7, color=GRAY, font=FONT)
    return idx + len(chunks), object_ids
