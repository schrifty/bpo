"""Engagement Breakdown slide builder."""

from __future__ import annotations

from typing import Any

from .config import logger
from .slide_primitives import (
    kpi_metric_card as _kpi_metric_card,
    slide_chart_legend as _slide_chart_legend,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, NAVY


def engagement_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Engagement Breakdown")

    eng = report["engagement"]
    total = report["account"]["total_visitors"]

    kpi_h = 54
    gap = 16.0
    kpi_y = BODY_Y + 8
    kpi_w = (CONTENT_W - 2 * gap) / 3
    tier_specs = [
        ("Active (7d)", eng["active_7d"], BLUE),
        ("Active (8–30d)", eng["active_30d"], BLUE),
        ("Dormant (30d+)", eng["dormant"], BLUE),
    ]
    for index, (label, count, accent) in enumerate(tier_specs):
        _kpi_metric_card(
            reqs,
            f"{sid}_ek{index}",
            sid,
            MARGIN + index * (kpi_w + gap),
            kpi_y,
            kpi_w,
            kpi_h,
            label,
            f"{count}",
            accent=accent,
            value_pt=22,
        )
    content_top = kpi_y + kpi_h + 12

    charts = report.get("_charts")
    has_chart = False

    if charts and total > 0:
        try:
            from .charts import BRAND_SERIES_COLORS as _BSC
            from .charts import embed_chart

            active_7d = eng["active_7d"]
            active_30d = eng["active_30d"]
            dormant = eng["dormant"]
            donut_labels = ["Active (7d)", "Active (8–30d)", "Dormant (30d+)"]
            ss_id, chart_id = charts.add_pie_chart(
                title="User Engagement",
                labels=donut_labels,
                values=[active_7d, active_30d, dormant],
                donut=True,
            )
            legend_h = 22
            chart_w = 320
            chart_h = max(120, int(BODY_BOTTOM - content_top - 8 - legend_h))
            embed_chart(reqs, f"{sid}_donut", sid, ss_id, chart_id, MARGIN, content_top, chart_w, chart_h)
            legend_entries = [(label, _BSC[i]) for i, label in enumerate(donut_labels) if i < len(_BSC)]
            _slide_chart_legend(reqs, sid, f"{sid}_dleg", MARGIN, content_top + chart_h + 4, legend_entries)
            has_chart = True
        except Exception as e:
            logger.warning("Chart embed failed for engagement slide: %s", e)

    chart_used_w = 344 if has_chart else 0
    text_x = MARGIN + chart_used_w if has_chart else MARGIN
    text_w = CONTENT_W - chart_used_w if has_chart else CONTENT_W
    col_gap = 40
    col_w = (text_w - col_gap) // 2 if not has_chart else text_w

    total_label = f"{total:,} tracked users"
    roles_y = content_top + 18
    active_roles = list(eng["role_active"].items())[:6]
    dormant_roles = list(eng["role_dormant"].items())[:6]

    if has_chart:
        _box(reqs, f"{sid}_tot", sid, text_x, content_top, text_w, 14, total_label)
        _style(reqs, f"{sid}_tot", 0, len(total_label), size=9, color=GRAY, font=FONT)
        x = text_x
        y = roles_y
        if active_roles:
            header = "Active Roles"
            _box(reqs, f"{sid}_ah", sid, x, y, col_w, 22, header)
            _style(reqs, f"{sid}_ah", 0, len(header), bold=True, size=14, color=BLUE, font=FONT)
            y += 28
            for role_index, (role, count) in enumerate(active_roles):
                if y + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_ar{role_index}", sid, x, y, col_w, 18, line)
                _style(reqs, f"{sid}_ar{role_index}", 0, len(line), size=13, color=NAVY, font=FONT)
                _style(reqs, f"{sid}_ar{role_index}", 0, len(f"{count:>4}"), bold=True, size=13, color=BLUE, font=FONT)
                y += 22
        if dormant_roles and y + 50 < BODY_BOTTOM:
            y += 12
            header = "Dormant Roles"
            _box(reqs, f"{sid}_dh", sid, x, y, col_w, 22, header)
            _style(reqs, f"{sid}_dh", 0, len(header), bold=True, size=14, color=GRAY, font=FONT)
            y += 28
            for role_index, (role, count) in enumerate(dormant_roles):
                if y + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_dr{role_index}", sid, x, y, col_w, 18, line)
                _style(reqs, f"{sid}_dr{role_index}", 0, len(line), size=13, color=GRAY, font=FONT)
                _style(reqs, f"{sid}_dr{role_index}", 0, len(f"{count:>4}"), bold=True, size=13)
                y += 22
    else:
        _box(reqs, f"{sid}_tot", sid, MARGIN, content_top, CONTENT_W, 14, total_label)
        _style(reqs, f"{sid}_tot", 0, len(total_label), size=9, color=GRAY, font=FONT)
        left_x = MARGIN
        right_x = MARGIN + col_w + col_gap
        left_y = roles_y
        if active_roles:
            header = "Active Roles"
            _box(reqs, f"{sid}_ah", sid, left_x, left_y, col_w, 22, header)
            _style(reqs, f"{sid}_ah", 0, len(header), bold=True, size=14, color=BLUE, font=FONT)
            left_y += 28
            for role_index, (role, count) in enumerate(active_roles):
                if left_y + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_ar{role_index}", sid, left_x, left_y, col_w, 18, line)
                _style(reqs, f"{sid}_ar{role_index}", 0, len(line), size=13, color=NAVY, font=FONT)
                _style(reqs, f"{sid}_ar{role_index}", 0, len(f"{count:>4}"), bold=True, size=13, color=BLUE, font=FONT)
                left_y += 22
        right_y = roles_y
        if dormant_roles and right_y + 50 < BODY_BOTTOM:
            header = "Dormant Roles"
            _box(reqs, f"{sid}_dh", sid, right_x, right_y, col_w, 22, header)
            _style(reqs, f"{sid}_dh", 0, len(header), bold=True, size=14, color=GRAY, font=FONT)
            right_y += 28
            for role_index, (role, count) in enumerate(dormant_roles):
                if right_y + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_dr{role_index}", sid, right_x, right_y, col_w, 18, line)
                _style(reqs, f"{sid}_dr{role_index}", 0, len(line), size=13, color=GRAY, font=FONT)
                _style(reqs, f"{sid}_dr{role_index}", 0, len(f"{count:>4}"), bold=True, size=13)
                right_y += 22

    return idx + 1
