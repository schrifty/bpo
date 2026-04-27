"""LeanDNA Lean Projects slide builders."""

from __future__ import annotations

from typing import Any

from .slide_primitives import clean_table as _clean_table, kpi_metric_card as _kpi_metric_card, missing_data_slide as _missing_data_slide, rect as _rect, slide_title as _slide_title, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slide_utils import slide_size as _sz, slide_transform as _tf
from .slides_theme import BODY_Y, BLUE, CONTENT_W, FONT, GRAY, MARGIN, NAVY


GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
ORANGE = {"red": 0.9, "green": 0.4, "blue": 0.0}


def _fmt_money(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:,.0f}" if value > 0 else "$0"


def _table_cell_text(reqs: list[dict[str, Any]], table_id: str, row: int, col: int, text: str) -> None:
    if not text:
        return
    reqs.append({"insertText": {"objectId": table_id, "cellLocation": {"rowIndex": row, "columnIndex": col}, "text": str(text), "insertionIndex": 0}})


def _table_cell_style(
    reqs: list[dict[str, Any]],
    table_id: str,
    row: int,
    col: int,
    text_len: int,
    *,
    bold: bool = False,
    color: dict[str, float] | None = None,
    size: int = 8,
    align: str | None = None,
) -> None:
    if text_len > 0:
        style: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
        fields = ["fontSize", "fontFamily"]
        if bold:
            style["bold"] = True
            fields.append("bold")
        if color:
            style["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
            fields.append("foregroundColor")
        reqs.append({"updateTextStyle": {"objectId": table_id, "cellLocation": {"rowIndex": row, "columnIndex": col}, "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len}, "style": style, "fields": ",".join(fields)}})
    if align:
        reqs.append({"updateParagraphStyle": {"objectId": table_id, "cellLocation": {"rowIndex": row, "columnIndex": col}, "textRange": {"type": "ALL"}, "style": {"alignment": align}, "fields": "alignment"}})


def _table_cell_bg(reqs: list[dict[str, Any]], table_id: str, row: int, col: int, color: dict[str, float]) -> None:
    reqs.append({"updateTableCellProperties": {"objectId": table_id, "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1}, "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}}, "fields": "tableCellBackgroundFill"}})


def _placeholder(reqs: list[dict[str, Any]], sid: str, oid: str, y: float, h: float, text: str) -> None:
    _rect(reqs, f"{oid}_bg", sid, MARGIN, y, CONTENT_W, h, {"red": 0.95, "green": 0.95, "blue": 0.95})
    _box(reqs, oid, sid, MARGIN, y + h / 2 - 20, CONTENT_W, 40, text)
    _style(reqs, oid, 0, len(text), size=14, color=GRAY, font=FONT)


def lean_projects_portfolio_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> list[str]:
    """Lean Projects Portfolio — Top 10 projects by savings."""
    ldna_projects = report.get("leandna_lean_projects") or {}
    if not ldna_projects.get("enabled"):
        return _missing_data_slide(reqs, sid, report, idx, "LeanDNA Lean Projects not configured")

    top_projects = ldna_projects.get("top_projects") or []
    if not top_projects:
        return _missing_data_slide(reqs, sid, report, idx, "No Lean projects found for period")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Lean Projects Portfolio — Top 10 by Savings")

    headers = ["Project Name", "Stage", "State", "Manager", "Actual", "Target", "Achieve %"]
    col_widths = [180, 70, 50, 90, 70, 70, 55]
    row_h = 24
    max_rows = min(len(top_projects), 10)
    table_top = BODY_Y + 12
    num_rows = 1 + max_rows
    table_id = f"{sid}_tbl"
    reqs.append({"createTable": {"objectId": table_id, "elementProperties": {"pageObjectId": sid, "size": _sz(sum(col_widths), num_rows * row_h), "transform": _tf(MARGIN, table_top)}, "rows": num_rows, "columns": len(headers)}})
    _clean_table(reqs, table_id, num_rows, len(headers))

    for col_index, header in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, col_index, header)
        _table_cell_style(reqs, table_id, 0, col_index, len(header), bold=True, color=NAVY, size=8, align="END" if col_index >= 4 else None)

    for row_index, project in enumerate(top_projects[:max_rows], start=1):
        actual = float(project.get("savings_actual", 0.0) or 0.0)
        target = float(project.get("savings_target", 0.0) or 0.0)
        achievement = (actual / target * 100) if target > 0 else 0.0
        state = project.get("state") or "unknown"
        values = [
            (project.get("name") or "Unknown")[:40],
            project.get("stage") or "Unknown",
            state,
            (project.get("project_manager") or "")[:25],
            _fmt_money(actual),
            _fmt_money(target),
            f"{achievement:.0f}%",
        ]
        for col_index, value in enumerate(values):
            _table_cell_text(reqs, table_id, row_index, col_index, value)
            if col_index == 2:
                cell_color = {"good": {"red": 0.8, "green": 1.0, "blue": 0.8}, "warn": {"red": 1.0, "green": 0.95, "blue": 0.7}, "bad": {"red": 1.0, "green": 0.8, "blue": 0.8}}.get(state)
                if cell_color:
                    _table_cell_bg(reqs, table_id, row_index, col_index, cell_color)
            _table_cell_style(reqs, table_id, row_index, col_index, len(value), size=7, align="END" if col_index >= 4 else None)

    return [sid]


def lean_projects_savings_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> list[str]:
    """Lean Projects Savings — monthly trend and KPIs."""
    ldna_projects = report.get("leandna_lean_projects") or {}
    if not ldna_projects.get("enabled"):
        return _missing_data_slide(reqs, sid, report, idx, "LeanDNA Lean Projects not configured")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Lean Projects Savings Tracking")

    chart_y = BODY_Y + 12
    chart_h = 200
    _placeholder(reqs, sid, f"{sid}_chart_text", chart_y, chart_h, "[Stacked Column Chart: Monthly Savings (Actual vs Target)]\n(Chart generation TODO)")

    kpi_y = chart_y + chart_h + 18
    kpi_h = 58
    kpi_gap = 18
    kpi_w = (CONTENT_W - 3 * kpi_gap) / 4
    total_actual = float(ldna_projects.get("total_savings_actual", 0.0) or 0.0)
    achievement = float(ldna_projects.get("savings_achievement_pct", 0.0) or 0.0)

    _kpi_metric_card(reqs, f"{sid}_k0", sid, MARGIN, kpi_y, kpi_w, kpi_h, "Total Projects", f"{ldna_projects.get('total_projects', 0):,}", accent=BLUE, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k1", sid, MARGIN + kpi_w + kpi_gap, kpi_y, kpi_w, kpi_h, "Active Projects", f"{ldna_projects.get('active_projects', 0):,}", accent=BLUE, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k2", sid, MARGIN + 2 * (kpi_w + kpi_gap), kpi_y, kpi_w, kpi_h, "Total Savings", _fmt_money(total_actual), accent=GREEN, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k3", sid, MARGIN + 3 * (kpi_w + kpi_gap), kpi_y, kpi_w, kpi_h, "Achievement", f"{achievement:.0f}%", accent=GREEN if achievement >= 100 else ORANGE, value_pt=18)
    return [sid]
