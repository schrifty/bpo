"""LeanDNA Material Shortage slide builders."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .slide_primitives import clean_table as _clean_table, kpi_metric_card as _kpi_metric_card, missing_data_slide as _missing_data_slide, rect as _rect, slide_title as _slide_title, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slide_utils import slide_size as _sz, slide_transform as _tf
from .slides_theme import BODY_Y, BLUE, CONTENT_W, FONT, GRAY, MARGIN, NAVY


ORANGE = {"red": 0.9, "green": 0.4, "blue": 0.0}
SLIDES_NEEDING_LEANDNA_SHORTAGE = frozenset(("shortage_forecast", "critical_shortages_detail", "shortage_deliveries"))


def leandna_shortage_unavailable_message(ldna: dict[str, Any]) -> str:
    """Explain why Material Shortage slides cannot render."""
    if not ldna:
        return (
            "LeanDNA Material Shortage — data not in report; regenerating the deck will fetch "
            "trends if LEANDNA_DATA_API_BEARER_TOKEN is set"
        )
    if not ldna.get("enabled"):
        reason = (ldna.get("reason") or "").strip()
        if reason == "bearer_token_not_configured":
            return "LeanDNA Material Shortage — set LEANDNA_DATA_API_BEARER_TOKEN (Data API access)"
        if reason:
            return f"LeanDNA Material Shortage — {reason[:85]}"
    error = (ldna.get("error") or "").strip()
    if error and error not in ("no_shortage_items_returned",):
        return f"LeanDNA Material Shortage — {error[:90]}"
    return "LeanDNA Material Shortage — not configured or unavailable"


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
        reqs.append({
            "updateTextStyle": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                "style": style,
                "fields": ",".join(fields),
            }
        })
    if align:
        reqs.append({
            "updateParagraphStyle": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "textRange": {"type": "ALL"},
                "style": {"alignment": align},
                "fields": "alignment",
            }
        })


def _table_cell_bg(reqs: list[dict[str, Any]], table_id: str, row: int, col: int, color: dict[str, float]) -> None:
    reqs.append({
        "updateTableCellProperties": {
            "objectId": table_id,
            "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
            "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
            "fields": "tableCellBackgroundFill",
        }
    })


def _placeholder(reqs: list[dict[str, Any]], sid: str, oid: str, y: float, h: float, text: str) -> None:
    _rect(reqs, f"{oid}_bg", sid, MARGIN, y, CONTENT_W, h, {"red": 0.95, "green": 0.95, "blue": 0.95})
    _box(reqs, oid, sid, MARGIN, y + h / 2 - 20, CONTENT_W, 40, text)
    _style(reqs, oid, 0, len(text), size=14, color=GRAY, font=FONT)


def _fmt_money(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:,.0f}" if value > 0 else "-"


def critical_shortages_detail_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> list[str]:
    """Critical Material Shortages table."""
    ldna_shortage = report.get("leandna_shortage_trends") or {}
    if not ldna_shortage.get("enabled"):
        return _missing_data_slide(reqs, sid, report, idx, leandna_shortage_unavailable_message(ldna_shortage))

    critical_timeline = ldna_shortage.get("critical_timeline") or []
    if not critical_timeline:
        return _missing_data_slide(reqs, sid, report, idx, "No critical shortages found")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Critical Material Shortages — Next 90 Days")

    headers = ["Item Code", "Description", "Site", "First Critical", "Days Short", "CTB Impact", "PO Status"]
    col_widths = [80, 140, 70, 75, 55, 65, 60]
    row_h = 24
    max_rows = min(len(critical_timeline), 20)
    table_top = BODY_Y + 12
    num_rows = 1 + max_rows
    table_id = f"{sid}_tbl"
    reqs.append({"createTable": {"objectId": table_id, "elementProperties": {"pageObjectId": sid, "size": _sz(sum(col_widths), num_rows * row_h), "transform": _tf(MARGIN, table_top)}, "rows": num_rows, "columns": len(headers)}})
    _clean_table(reqs, table_id, num_rows, len(headers))

    for col_index, header in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, col_index, header)
        _table_cell_style(reqs, table_id, 0, col_index, len(header), bold=True, color=NAVY, size=8, align="END" if col_index >= 3 else None)

    for row_index, item in enumerate(critical_timeline[:max_rows], start=1):
        first_crit = item.get("firstCriticalWeek") or ""
        if first_crit:
            try:
                from dateutil import parser

                parsed = parser.parse(first_crit)
                first_crit_display = parsed.strftime("%m/%d")
                days_until = (parsed.replace(tzinfo=timezone.utc) - datetime.now(timezone.utc)).days
            except Exception:
                first_crit_display = first_crit[:10]
                days_until = 999
        else:
            first_crit_display = "-"
            days_until = 999

        values = [
            (item.get("itemCode") or "")[:20],
            (item.get("itemDescription") or "")[:35],
            (item.get("site") or "")[:15],
            first_crit_display,
            str(item.get("daysInShortage") or "-"),
            _fmt_money(float(item.get("ctbImpact") or 0)),
            item.get("poStatus") or "Unknown",
        ]
        for col_index, value in enumerate(values):
            _table_cell_text(reqs, table_id, row_index, col_index, value)
            if col_index == 3 and days_until < 30:
                cell_color = {"red": 1.0, "green": 0.8, "blue": 0.8} if days_until < 7 else {"red": 1.0, "green": 0.9, "blue": 0.7} if days_until < 14 else {"red": 1.0, "green": 1.0, "blue": 0.8}
                _table_cell_bg(reqs, table_id, row_index, col_index, cell_color)
            _table_cell_style(reqs, table_id, row_index, col_index, len(value), size=7, align="END" if col_index >= 3 else None)

    return [sid]


def shortage_forecast_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> list[str]:
    """Shortage Forecast slide with chart placeholder and KPI cards."""
    ldna_shortage = report.get("leandna_shortage_trends") or {}
    if not ldna_shortage.get("enabled"):
        return _missing_data_slide(reqs, sid, report, idx, leandna_shortage_unavailable_message(ldna_shortage))
    forecast = ldna_shortage.get("forecast") or {}
    buckets = forecast.get("buckets") or []
    if not buckets:
        return _missing_data_slide(reqs, sid, report, idx, "No shortage forecast data available")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Material Shortage Forecast — Next 12 Weeks")
    chart_y = BODY_Y + 12
    chart_h = 200
    _placeholder(reqs, sid, f"{sid}_chart_text", chart_y, chart_h, "[Stacked Area Chart: Weekly Shortage Forecast]\n(Chart generation TODO)")

    kpi_y = chart_y + chart_h + 18
    kpi_h = 58
    kpi_gap = 18
    kpi_w = (CONTENT_W - 3 * kpi_gap) / 4
    total_items = ldna_shortage.get("total_items_in_shortage", 0)
    critical_items = ldna_shortage.get("critical_items", 0)
    peak_week = forecast.get("peak_week") or "N/A"
    total_value = forecast.get("total_shortage_value", 0)
    if peak_week != "N/A":
        try:
            from dateutil import parser

            peak_week_display = parser.parse(peak_week).strftime("%b %d")
        except Exception:
            peak_week_display = peak_week[:10]
    else:
        peak_week_display = "N/A"

    _kpi_metric_card(reqs, f"{sid}_k0", sid, MARGIN, kpi_y, kpi_w, kpi_h, "Total Items in Shortage", f"{total_items:,}", accent=BLUE, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k1", sid, MARGIN + kpi_w + kpi_gap, kpi_y, kpi_w, kpi_h, "Critical Items", f"{critical_items:,}", accent=ORANGE if critical_items > 10 else BLUE, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k2", sid, MARGIN + 2 * (kpi_w + kpi_gap), kpi_y, kpi_w, kpi_h, "Peak Week", peak_week_display, accent=BLUE, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k3", sid, MARGIN + 3 * (kpi_w + kpi_gap), kpi_y, kpi_w, kpi_h, "Shortage Value", _fmt_money(float(total_value)).replace("-", "$0"), accent=BLUE, value_pt=18)
    return [sid]


def shortage_deliveries_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> list[str]:
    """Shortage Resolution scheduled deliveries slide."""
    ldna_shortage = report.get("leandna_shortage_trends") or {}
    if not ldna_shortage.get("enabled"):
        return _missing_data_slide(reqs, sid, report, idx, leandna_shortage_unavailable_message(ldna_shortage))

    deliveries = ldna_shortage.get("scheduled_deliveries") or {}
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Shortage Resolution — Scheduled Deliveries")
    chart_y = BODY_Y + 12
    chart_h = 220
    _placeholder(reqs, sid, f"{sid}_chart_text", chart_y, chart_h, "[Dual Chart: Shortage vs Scheduled Deliveries]\n(Chart generation TODO)")

    kpi_y = chart_y + chart_h + 18
    kpi_h = 58
    kpi_gap = 22
    kpi_w = (CONTENT_W - 2 * kpi_gap) / 3
    _kpi_metric_card(reqs, f"{sid}_k0", sid, MARGIN, kpi_y, kpi_w, kpi_h, "Items with Schedules", f"{deliveries.get('items_with_schedules', 0):,}", accent=BLUE, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k1", sid, MARGIN + kpi_w + kpi_gap, kpi_y, kpi_w, kpi_h, "Avg Deliveries/Item", f"{deliveries.get('avg_deliveries_per_item', 0):.1f}", accent=BLUE, value_pt=18)
    _kpi_metric_card(reqs, f"{sid}_k2", sid, MARGIN + 2 * (kpi_w + kpi_gap), kpi_y, kpi_w, kpi_h, "Next 7 Days Qty", f"{deliveries.get('next_n_days_scheduled_qty', 0):,.0f}", accent=BLUE, value_pt=18)
    return [sid]
