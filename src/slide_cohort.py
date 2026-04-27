"""Cohort review slide builders."""

from __future__ import annotations

from typing import Any

from .slide_loader import cohort_profiles_max_physical_slides
from .slide_pipeline_traces import fmt_platform_value_dollar as _fmt_platform_value_dollar
from .slide_primitives import (
    background as _bg,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box, append_wrapped_text_box as _wrap_box
from .slides_theme import (
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    FONT_SERIF,
    GRAY,
    LIGHT,
    LTBLUE,
    MARGIN,
    NAVY,
    WHITE,
    _CohortSummaryLabels,
    _cap_chunk_list,
    _cohort_summary_metrics,
    _date_range,
)


def cohort_summary_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Portfolio-wide cohort summary with aggregate KPIs across all cohorts."""
    metrics = _cohort_summary_metrics(report)
    if not metrics:
        return _missing_data_slide(reqs, sid, report, idx, "cohort_digest (no cohort data)")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)

    labels = _CohortSummaryLabels
    total_customers = metrics["total_customers"]
    num_cohorts = metrics["num_cohorts"]
    total_users = metrics["total_users"]
    total_active = metrics["total_active"]
    overall_active_pct = metrics["overall_active_pct"]
    total_arr = metrics["total_arr"]
    med_login = metrics["med_login"]
    med_write = metrics["med_write"]
    med_exports = metrics["med_exports"]
    med_kei = metrics["med_kei"]
    biggest_lbl = metrics["biggest_lbl"]

    _slide_title(reqs, sid, "Cohort Summary")

    row1_y = BODY_Y + 8
    card_h = 58
    gap = 12
    cards_per_row = 3
    card_w = (CONTENT_W - gap * (cards_per_row - 1)) / cards_per_row

    _kpi_metric_card(reqs, f"{sid}_c0", sid, MARGIN, row1_y, card_w, card_h, labels.TOTAL_CUSTOMERS, str(total_customers), accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c1", sid, MARGIN + card_w + gap, row1_y, card_w, card_h, labels.COHORTS, str(num_cohorts), accent=BLUE)
    arr_str = _fmt_platform_value_dollar(total_arr) if total_arr > 0 else "—"
    _kpi_metric_card(reqs, f"{sid}_c2", sid, MARGIN + 2 * (card_w + gap), row1_y, card_w, card_h, labels.TOTAL_ARR, arr_str, accent=BLUE)

    row2_y = row1_y + card_h + gap
    _kpi_metric_card(reqs, f"{sid}_c3", sid, MARGIN, row2_y, card_w, card_h, labels.TOTAL_USERS, f"{total_users:,}", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c4", sid, MARGIN + card_w + gap, row2_y, card_w, card_h, labels.ACTIVE_USERS_7D, f"{total_active:,}", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c5", sid, MARGIN + 2 * (card_w + gap), row2_y, card_w, card_h, labels.ACTIVE_RATE, f"{overall_active_pct}%", accent=BLUE)

    row3_y = row2_y + card_h + gap
    _kpi_metric_card(reqs, f"{sid}_c6", sid, MARGIN, row3_y, card_w, card_h, labels.WEEKLY_ACTIVE_MEDIAN, f"{med_login}%" if med_login is not None else "—", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c7", sid, MARGIN + card_w + gap, row3_y, card_w, card_h, labels.WRITE_RATIO_MEDIAN, f"{med_write}%" if med_write is not None else "—", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c8", sid, MARGIN + 2 * (card_w + gap), row3_y, card_w, card_h, labels.KEI_ADOPTION_MEDIAN, f"{med_kei}%" if med_kei is not None else "—", accent=BLUE)

    row4_y = row3_y + card_h + gap
    cards_r4 = 2
    card_w4 = (CONTENT_W - gap * (cards_r4 - 1)) / cards_r4
    _kpi_metric_card(reqs, f"{sid}_c9", sid, MARGIN, row4_y, card_w4, card_h, labels.EXPORTS_MEDIAN, f"{med_exports:.0f}" if med_exports is not None else "—", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c10", sid, MARGIN + card_w4 + gap, row4_y, card_w4, card_h, labels.LARGEST_COHORT, biggest_lbl, accent=BLUE, value_pt=14)

    return idx + 1


def cohort_deck_title_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Title for manufacturing cohort deck."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    customer_count = report.get("customer_count", 0)
    days = report.get("days", 30)
    quarter_label = report.get("quarter")
    title = "Manufacturing cohort review"
    subtitle = f"{customer_count} customers in scope  ·  {_date_range(days, quarter_label, report.get('quarter_start'), report.get('quarter_end'))}"

    _box(reqs, f"{sid}_t", sid, MARGIN, 100, CONTENT_W, 80, title)
    _style(reqs, f"{sid}_t", 0, len(title), bold=True, size=32, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 188, CONTENT_W, 36, subtitle)
    _style(reqs, f"{sid}_s", 0, len(subtitle), size=14, color=LTBLUE, font=FONT)

    note = "Cohorts from cohorts.yaml · see docs/CUSTOMER_COHORTS.md"
    _box(reqs, f"{sid}_n", sid, MARGIN, 240, CONTENT_W, 20, note)
    _style(reqs, f"{sid}_n", 0, len(note), size=10, color=GRAY, font=FONT)

    generated = report.get("generated", "")
    if generated:
        _box(reqs, f"{sid}_d", sid, MARGIN, 340, CONTENT_W, 20, generated)
        _style(reqs, f"{sid}_d", 0, len(generated), size=10, color=GRAY, font=FONT)

    return idx + 1


def cohort_profiles_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    """Up to ``rollup_params.max_physical_slides`` cohort profile pages."""
    digest = report.get("cohort_digest") or {}
    cap = cohort_profiles_max_physical_slides()
    rows = sorted(
        [(key, value) for key, value in digest.items() if isinstance(value, dict) and int(value.get("n") or 0) > 0],
        key=lambda item: (item[0] == "unclassified", -int(item[1].get("n") or 0)),
    )[:cap]
    if not rows:
        return _missing_data_slide(reqs, sid, report, idx, "cohort_digest (no customers in cohort buckets)")

    total_customers = report.get("customer_count", 0)
    arr_map = report.get("_arr_by_customer") or {}
    object_ids: list[str] = []
    blocks_for_notes: list[dict[str, Any]] = []
    page_count = len(rows)
    for page_index, (_cohort_id, block) in enumerate(rows):
        page_sid = f"{sid}_p{page_index}" if page_count > 1 else sid
        object_ids.append(page_sid)
        blocks_for_notes.append(block)
        _slide(reqs, page_sid, idx + page_index)
        _bg(reqs, page_sid, WHITE)
        cohort_n = block["n"]
        cohort_arr = sum(arr_map.get(customer, 0) for customer in (block.get("customers") or []))
        title = f"{block['display_name']} ({cohort_n} of {total_customers} customers"
        if cohort_arr > 0:
            title += f", {_fmt_platform_value_dollar(cohort_arr)} ARR"
        title += ")"
        _slide_title(reqs, page_sid, title)

        mlogin = block.get("median_login_pct")
        mlogin_s = "—" if mlogin is None else f"{mlogin}%"
        mw = block.get("median_write_ratio")
        mw_s = "—" if mw is None else f"{mw}%"
        med_exports = block.get("median_exports")
        med_exports_s = "—" if med_exports is None else f"{med_exports:.0f}"

        header = (
            f"{block['total_active_users']:,} active (7d) / "
            f"{block['total_users']:,} total users across cohort"
        )
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 20, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=12, color=GRAY, font=FONT)

        kei_pct = block.get("kei_adoption_pct", 0)
        stats = (
            f"Weekly active rate (median) {mlogin_s}  ·  "
            f"write-to-total ratio (median) {mw_s}  ·  "
            f"Kei adopters (% of customers) {kei_pct}%  ·  "
            f"exports per customer (median, 30d) {med_exports_s}"
        )
        _box(reqs, f"{page_sid}_st", page_sid, MARGIN, BODY_Y + 24, CONTENT_W, 36, stats)
        _style(reqs, f"{page_sid}_st", 0, len(stats), size=12, color=NAVY, font=FONT)

        customers = block.get("customers") or []
        arr_map = report.get("_arr_by_customer") or {}

        def fmt_arr(value: float) -> str:
            abs_value = abs(value)
            if abs_value >= 1_000_000:
                return f"${value / 1_000_000:,.1f}M"
            if abs_value >= 1_000:
                return f"${value / 1_000:,.0f}K"
            return f"${value:,.0f}"

        decorated = [(name, arr_map.get(name, 0.0)) for name in customers]
        decorated.sort(key=lambda item: -item[1])

        def label(name: str, arr: float) -> str:
            return f"• {name} — {fmt_arr(arr)}" if arr else f"• {name}"

        midpoint = (len(decorated) + 1) // 2
        col_left = decorated[:midpoint]
        col_right = decorated[midpoint:]

        accounts_y = BODY_Y + 66
        accounts_h = BODY_BOTTOM - accounts_y - 8
        col_w = (CONTENT_W - 24) // 2

        has_arr = any(arr > 0 for _, arr in decorated)
        left_header = "Accounts (by ARR)" if has_arr else "Accounts"
        left_lines = [left_header] + [label(name, arr) for name, arr in col_left]
        left_body = "\n".join(left_lines)
        _wrap_box(reqs, f"{page_sid}_accL", page_sid, MARGIN, accounts_y, col_w, accounts_h, left_body)
        _style(reqs, f"{page_sid}_accL", 0, len(left_body), size=11, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid}_accL", 0, len(left_header), bold=True, size=12, color=BLUE, font=FONT)

        if col_right:
            right_lines = [""] + [label(name, arr) for name, arr in col_right]
            right_body = "\n".join(right_lines)
            _wrap_box(reqs, f"{page_sid}_accR", page_sid, MARGIN + col_w + 24, accounts_y, col_w, accounts_h, right_body)
            _style(reqs, f"{page_sid}_accR", 0, len(right_body), size=11, color=NAVY, font=FONT)

    report["_cohort_profile_speaker_note_blocks"] = blocks_for_notes

    if page_count == 1:
        return idx + 1
    return idx + page_count, object_ids


COHORT_FINDING_ROW_H_PT = 38
COHORT_FINDING_ROW_GAP_PT = 6


def cohort_findings_rows_per_page() -> int:
    """How many wrapped bullet rows fit between BODY_Y and BODY_BOTTOM."""
    available = float(BODY_BOTTOM) - float(BODY_Y) - 8.0
    step = float(COHORT_FINDING_ROW_H_PT + COHORT_FINDING_ROW_GAP_PT)
    return max(1, int(available // step))


def cohort_findings_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    bullets = list(report.get("cohort_findings_bullets") or [])
    if not bullets:
        return _missing_data_slide(reqs, sid, report, idx, "cohort_findings_bullets")

    max_rows = min(cohort_findings_rows_per_page(), 28)
    chunks = _cap_chunk_list(
        [bullets[i: i + max_rows] for i in range(0, len(bullets), max_rows)]
    )
    object_ids: list[str] = []
    for page_index, chunk in enumerate(chunks):
        page_sid = f"{sid}_p{page_index}" if len(chunks) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        _bg(reqs, page_sid, LIGHT)
        title = (
            "Notable findings — cohort differences"
            if len(chunks) == 1
            else f"Notable findings — cohort differences ({page_index + 1} of {len(chunks)})"
        )
        _slide_title(reqs, page_sid, title)
        base = page_index * max_rows
        y = float(BODY_Y)
        for item_index, raw in enumerate(chunk, start=base + 1):
            line = raw if len(raw) <= 220 else raw[:217] + "…"
            prefix = f"{item_index}.   "
            full = f"{prefix}{line}"
            object_id = f"{page_sid}_cf{item_index}"
            _wrap_box(reqs, object_id, page_sid, MARGIN, int(y), CONTENT_W, COHORT_FINDING_ROW_H_PT, full)
            _style(reqs, object_id, 0, len(full), size=12, color=NAVY, font=FONT)
            _style(reqs, object_id, 0, len(prefix), bold=True, color=BLUE)
            y += float(COHORT_FINDING_ROW_H_PT + COHORT_FINDING_ROW_GAP_PT)
    return idx + len(chunks), object_ids
