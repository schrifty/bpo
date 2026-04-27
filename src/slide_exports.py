"""Export Behavior slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import missing_data_slide as _missing_data_slide, slide_title as _slide_title, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, NAVY, _cap_page_count, _list_data_rows_fit_span


def exports_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    exports = report.get("exports", report)
    by_feature = exports.get("by_feature", [])
    top_exporters = exports.get("top_exporters", [])
    total = exports.get("total_exports", 0)

    if not by_feature and total == 0:
        return _missing_data_slide(reqs, sid, report, idx, "export / benchmark data")

    per_user = exports.get("exports_per_active_user", 0)
    active = exports.get("active_users", 0)
    header = f"{total:,} exports  ·  {per_user}/active user  ·  {active} active users"
    list_top = BODY_Y + 24
    # Use full body band so feature and exporter columns share one line budget.
    list_bottom = min(list_top + 270, BODY_BOTTOM - 4)
    list_h = max(120.0, float(list_bottom) - float(list_top))
    line_budget = _list_data_rows_fit_span(
        y_top=list_top,
        y_bottom=list_top + list_h,
        font_body_pt=10,
        reserved_header_lines=1,
        max_rows_cap=40,
    )
    # By Feature: one line per row. Top Exporters: two lines per user (email + detail).
    max_features = line_budget
    max_exporters = max(1, line_budget // 2)
    feature_pages = (len(by_feature) + max_features - 1) // max_features if by_feature else 0
    exporter_pages = (len(top_exporters) + max_exporters - 1) // max_exporters if top_exporters else 0
    num_pages = _cap_page_count(max(feature_pages, exporter_pages, 1))
    object_ids: list[str] = []
    for page in range(num_pages):
        page_sid = f"{sid}_p{page}" if num_pages > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page)
        title = "Export Behavior" if num_pages == 1 else f"Export Behavior ({page + 1} of {num_pages})"
        _slide_title(reqs, page_sid, title)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=GRAY, font=FONT)

        feature_lines = ["By Feature"]
        feature_slice = by_feature[page * max_features: (page + 1) * max_features]
        start_index = page * max_features
        for index, feature in enumerate(feature_slice, start=start_index + 1):
            name = feature["feature"][:36] if len(feature["feature"]) > 36 else feature["feature"]
            feature_lines.append(f"  {index}. {name}  ({feature['exports']:,})")
        if not feature_slice and page == 0 and not by_feature:
            feature_lines.append("  No export data")
        feature_text = "\n".join(feature_lines)
        _box(reqs, f"{page_sid}_bf", page_sid, MARGIN, BODY_Y + 24, 340, 270, feature_text)
        _style(reqs, f"{page_sid}_bf", 0, len(feature_text), size=10, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid}_bf", 0, len("By Feature"), bold=True, size=11, color=BLUE)

        exporter_lines = ["Top Exporters"]
        exporter_slice = top_exporters[page * max_exporters: (page + 1) * max_exporters]
        for user in exporter_slice:
            email = user["email"] or "unknown"
            if len(email) > 32:
                email = email[:29] + "..."
            exporter_lines.append(f"  {email}")
            exporter_lines.append(f"    {user['role']}  ·  {user['exports']:,} exports")
        if not exporter_slice and page == 0 and not top_exporters:
            exporter_lines.append("  No export users")
        exporter_text = "\n".join(exporter_lines)
        _box(reqs, f"{page_sid}_te", page_sid, 400, BODY_Y + 24, 280, 270, exporter_text)
        _style(reqs, f"{page_sid}_te", 0, len(exporter_text), size=10, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid}_te", 0, len("Top Exporters"), bold=True, size=11, color=BLUE)
    return idx + num_pages, object_ids
