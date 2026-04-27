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


def sf_format_cell(val: Any, max_len: int = 44) -> str:
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        value = json.dumps(val, default=str)
    else:
        value = str(val)
    if len(value) > max_len:
        return value[: max_len - 1] + "…"
    return value


def sf_records_to_table(
    records: list[dict[str, Any]],
    *,
    max_cols: int = 7,
    max_rows: int = 12,
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
    return keys, rows


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
    _slide_title(reqs, sid, "Salesforce — comprehensive export")
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

    # Slides grows table rows when 9pt text wraps in narrow cells; 12pt nominal height underruns badly.
    row_h = 28.0
    y0 = BODY_Y + (38 if error_note else 0)
    bottom_pad = 10.0
    avail_h = BODY_BOTTOM - y0 - bottom_pad
    rows_per_page = max(2, int(avail_h // row_h) - 1)

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
        headers, rows = sf_records_to_table(chunk, max_rows=len(chunk))
        if not headers:
            continue
        column_count = len(headers)
        col_w = min(118.0, CONTENT_W / max(1, column_count))
        col_widths = [col_w] * column_count
        _simple_table(reqs, f"{page_sid}_tbl", page_sid, MARGIN, y, col_widths, row_h, headers, rows)
    return idx + len(chunks), object_ids
