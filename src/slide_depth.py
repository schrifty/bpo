"""Behavioral Depth slide builder."""

from __future__ import annotations

from typing import Any

from .config import logger
from .slide_primitives import (
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    slide_chart_legend as _slide_chart_legend,
    slide_title as _slide_title,
)
from .slide_requests import append_slide as _slide
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, MARGIN, _single_embedded_chart_layout


def depth_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    depth = report.get("depth", report)
    breakdown = depth.get("breakdown", [])
    if not breakdown:
        return _missing_data_slide(reqs, sid, report, idx, "depth-of-use breakdown data")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Behavioral Depth")

    write_ratio = depth.get("write_ratio", 0)
    total = depth.get("total_feature_events", 0)
    active = depth.get("active_users", 0)

    kpi_h = 54
    gap = 16.0
    chart_gap = 16.0
    kpi_y = BODY_Y + 8
    kpi_w = (CONTENT_W - 2 * gap) / 3
    _kpi_metric_card(
        reqs,
        f"{sid}_dk0",
        sid,
        MARGIN,
        kpi_y,
        kpi_w,
        kpi_h,
        "Feature interactions",
        f"{total:,}",
        accent=BLUE,
        value_pt=20,
    )
    _kpi_metric_card(
        reqs,
        f"{sid}_dk1",
        sid,
        MARGIN + kpi_w + gap,
        kpi_y,
        kpi_w,
        kpi_h,
        "Active users",
        f"{active}",
        accent=BLUE,
        value_pt=20,
    )
    _kpi_metric_card(
        reqs,
        f"{sid}_dk2",
        sid,
        MARGIN + 2 * (kpi_w + gap),
        kpi_y,
        kpi_w,
        kpi_h,
        "Write ratio",
        f"{write_ratio}%",
        accent=BLUE,
        value_pt=20,
    )
    chart_top = kpi_y + kpi_h + chart_gap

    charts = report.get("_charts")
    read_e = depth.get("read_events", 0)
    write_e = depth.get("write_events", 0)
    collab_e = depth.get("collab_events", 0)

    if charts:
        try:
            from .charts import BRAND_SERIES_COLORS as _BSC
            from .charts import embed_chart

            bottom_pad = 16
            chart_h = BODY_BOTTOM - chart_top - bottom_pad

            top = breakdown[:8]
            labels = [b["category"] for b in top]
            read_vals = [b.get("read", 0) for b in top]
            write_vals = [b.get("write", 0) for b in top]
            collab_vals = [b.get("collab", 0) for b in top]
            has_rwc = any(v > 0 for v in read_vals + write_vals + collab_vals)
            pie_ok = read_e + write_e + collab_e > 0

            if has_rwc and pie_ok:
                combined_gap = 8.0
                legend_h = 22
                left_w = (CONTENT_W - combined_gap) * 0.58
                right_w = CONTENT_W - combined_gap - left_w
                vis_chart_h = chart_h - legend_h
                rwc_labels = ["Read", "Write", "Collab"]
                ss_id, chart_id = charts.add_bar_chart(
                    title="Feature Category Depth",
                    labels=labels,
                    series={"Read": read_vals, "Write": write_vals, "Collab": collab_vals},
                    horizontal=True,
                    stacked=True,
                    suppress_legend=True,
                )
                embed_chart(
                    reqs,
                    f"{sid}_chart",
                    sid,
                    ss_id,
                    chart_id,
                    MARGIN,
                    chart_top,
                    left_w,
                    vis_chart_h,
                )
                legend_entries = [(label, _BSC[i]) for i, label in enumerate(rwc_labels) if i < len(_BSC)]
                _slide_chart_legend(reqs, sid, f"{sid}_bleg", MARGIN, chart_top + vis_chart_h + 4, legend_entries)

                ss_id2, pie_id = charts.add_pie_chart(
                    title="Read / Write / Collab",
                    labels=rwc_labels,
                    values=[read_e, write_e, collab_e],
                    donut=True,
                )
                pie_x = MARGIN + left_w + combined_gap
                embed_chart(reqs, f"{sid}_pie", sid, ss_id2, pie_id, pie_x, chart_top, right_w, vis_chart_h)
            elif has_rwc:
                legend_h = 22
                bx, by, bw, bh = _single_embedded_chart_layout(
                    y_top=chart_top,
                    bottom_pad=bottom_pad + legend_h,
                    pie_or_donut=False,
                )
                rwc_labels = ["Read", "Write", "Collab"]
                ss_id, chart_id = charts.add_bar_chart(
                    title="Feature Category Depth",
                    labels=labels,
                    series={"Read": read_vals, "Write": write_vals, "Collab": collab_vals},
                    horizontal=True,
                    stacked=True,
                    suppress_legend=True,
                )
                embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id, bx, by, bw, bh)
                legend_entries = [(label, _BSC[i]) for i, label in enumerate(rwc_labels) if i < len(_BSC)]
                _slide_chart_legend(reqs, sid, f"{sid}_bleg", bx, by + bh + 4, legend_entries)
            elif pie_ok:
                legend_h = 22
                px, py, pw, ph = _single_embedded_chart_layout(
                    y_top=chart_top,
                    bottom_pad=bottom_pad + legend_h,
                    pie_or_donut=True,
                )
                pie_labels = ["Read", "Write", "Collab"]
                ss_id2, pie_id = charts.add_pie_chart(
                    title="Read / Write / Collab",
                    labels=pie_labels,
                    values=[read_e, write_e, collab_e],
                    donut=True,
                )
                embed_chart(reqs, f"{sid}_pie", sid, ss_id2, pie_id, px, py, pw, ph)
                legend_entries = [(label, _BSC[i]) for i, label in enumerate(pie_labels) if i < len(_BSC)]
                _slide_chart_legend(reqs, sid, f"{sid}_pleg", px, py + ph + 4, legend_entries)
        except Exception as e:
            logger.warning("Chart embed failed for depth slide: %s", e)

    return idx + 1
