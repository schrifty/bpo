"""QBR deployment slide builder."""

from __future__ import annotations

import datetime
from typing import Any

from .cs_report_client import get_csr_section
from .slide_primitives import (
    missing_data_slide as _missing_data_slide,
    simple_table as _simple_table,
    slide_title as _slide_title,
    style as _style,
    table_cell_bg as _table_cell_bg,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, _cap_chunk_list


def qbr_deployment_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
) -> int | tuple[int, list[str]]:
    """Deployment overview: site count and status table from Pendo data."""
    all_sites = report.get("sites", [])
    if not all_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo site list for deployment summary")

    customer = report.get("customer", report.get("account", {}).get("customer", ""))
    raw_gen = report.get("generated", "")
    try:
        generated = datetime.datetime.strptime(raw_gen, "%Y-%m-%d").strftime("%B %-d, %Y")
    except (ValueError, TypeError):
        generated = raw_gen or datetime.datetime.now().strftime("%B %-d, %Y")
    subtitle = f"As of {generated}"

    cs_health = get_csr_section(report).get("platform_health") or {}
    site_health = {}
    for row in cs_health.get("sites", []):
        name = row.get("site", "")
        status = row.get("health_status", "")
        if name and status:
            site_health[name.lower()] = status

    customer_prefix = customer.strip()

    def _short_site(name: str) -> str:
        short = name
        if customer_prefix and short.lower().startswith(customer_prefix.lower()):
            short = short[len(customer_prefix) :].lstrip(" -·")
        return short[:25] if len(short) > 25 else short

    headers = ["Site", "Users", "Status", "Last Active"]
    col_widths = [220, 60, 80, 130]
    row_h = 26
    max_rows = max(1, (BODY_BOTTOM - (BODY_Y + 14)) // row_h - 1)
    site_chunks = _cap_chunk_list([all_sites[i : i + max_rows] for i in range(0, len(all_sites), max_rows)])
    status_colors = {
        "GREEN": {"red": 0.1, "green": 0.6, "blue": 0.2},
        "YELLOW": {"red": 0.9, "green": 0.7, "blue": 0.1},
        "RED": {"red": 0.85, "green": 0.15, "blue": 0.15},
    }
    oids: list[str] = []

    for page_index, sites_to_show in enumerate(site_chunks):
        page_sid = f"{sid}_p{page_index}" if len(site_chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        ttl = (
            "Deployment — Number of Sites"
            if len(site_chunks) == 1
            else f"Deployment — Sites ({page_index + 1} of {len(site_chunks)})"
        )
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_sub", page_sid, MARGIN, BODY_Y - 10, CONTENT_W, 18, subtitle)
        _style(reqs, f"{page_sid}_sub", 0, len(subtitle), size=10, color=GRAY, font=FONT)

        rows_data = []
        for site in sites_to_show:
            site_name = _short_site(site.get("sitename", "?"))
            visitors = str(site.get("visitors", 0))
            health = site_health.get(site.get("sitename", "").lower(), "—")
            last_active_raw = site.get("last_active", "—")
            try:
                last_active = datetime.datetime.strptime(str(last_active_raw)[:10], "%Y-%m-%d").strftime("%b %-d, %Y")
            except (ValueError, TypeError):
                last_active = str(last_active_raw)[:10] if last_active_raw else "—"
            rows_data.append([site_name, visitors, health, last_active])

        tbl_id = f"{page_sid}_tbl"
        _simple_table(reqs, tbl_id, page_sid, MARGIN, BODY_Y + 14, col_widths, row_h, headers, rows_data)
        for row_index, row in enumerate(rows_data):
            status = row[2].upper() if len(row) > 2 else ""
            if status in status_colors:
                _table_cell_bg(reqs, tbl_id, row_index + 1, 2, status_colors[status])

    return idx + len(site_chunks), oids
