"""Portfolio review slide builders."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    background as _bg,
    missing_data_slide as _missing_data_slide,
    rect as _rect,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slide_signals import render_signal_list_slide
from .slides_theme import (
    BLUE,
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
    _cap_chunk_list,
    _date_range,
)


def _fmt_portfolio_usd(n: Any) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        v = 0.0
    return f"${v:,.0f}"


def portfolio_revenue_book_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Salesforce rollup: ARR, churn vs active, pipeline, opps, top accounts (portfolio window)."""
    book = report.get("portfolio_revenue_book")
    if not isinstance(book, dict):
        book = {}
    if not book.get("configured"):
        return _missing_data_slide(
            reqs, sid, report, idx, "Salesforce revenue book (credentials not configured)",
        )
    if book.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, str(book.get("error")))

    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "").strip() or "Revenue book (Salesforce)"

    lines: list[str] = []
    pc = int(book.get("pendo_customers") or 0)
    sm = int(book.get("salesforce_matched_customers") or 0)
    su = int(book.get("salesforce_unmatched_customers") or 0)
    lines.append(f"Pendo customers in this window: {pc:,}")
    lines.append(f"Salesforce matched (≥1 Customer Entity row): {sm:,}  ·  Unmatched names: {su:,}")
    lines.append("")
    lines.append(f"Contract ARR on matched Entity rows: {_fmt_portfolio_usd(book.get('total_arr'))}")
    lines.append(
        f"  → On active-status contracts: {_fmt_portfolio_usd(book.get('active_installed_base_arr'))} "
        f"({int(book.get('active_customer_count') or 0):,} customers)"
    )
    lines.append(
        f"  → On churned-status contracts: {_fmt_portfolio_usd(book.get('churned_contract_arr'))} "
        f"({int(book.get('churned_customer_count') or 0):,} customers)"
    )
    lines.append("")
    lines.append(f"Pipeline ARR (advanced stages, deduped accounts): {_fmt_portfolio_usd(book.get('pipeline_arr'))}")
    lines.append(
        f"Opportunities with CloseDate this fiscal year (matched accounts): "
        f"{int(book.get('opportunity_count_this_year') or 0):,}"
    )
    top = book.get("top_customers_by_arr") or []
    if isinstance(top, list) and top:
        lines.append("")
        lines.append("Top customers by contract ARR (matched):")
        for i, row in enumerate(top[:8], start=1):
            if not isinstance(row, dict):
                continue
            nm = str(row.get("customer") or "").strip() or "?"
            arr = _fmt_portfolio_usd(row.get("arr"))
            tag = "" if row.get("active") else "  [churned in SF]"
            lines.append(f"  {i}. {nm} — {arr}{tag}")
    churn_sample = book.get("churned_customer_names_sample") or []
    if isinstance(churn_sample, list) and churn_sample:
        lines.append("")
        lines.append(f"Churned-status sample: {', '.join(str(x) for x in churn_sample[:8])}")
    lines.append("")
    lines.append(
        "New logos / expansion vs prior periods are not computed here — use Salesforce reports for motion history."
    )

    body = "\n".join(lines)

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)
    oid = f"{sid}_body"
    _box(reqs, oid, sid, MARGIN, BODY_Y, CONTENT_W, 300, body)
    _style(reqs, oid, 0, len(body), size=10, color=NAVY, font=FONT)
    foot = (
        "ARR and status from matched Customer Entity accounts; pipeline uses the same advanced-stage "
        "definition as per-account decks."
    )
    fid = f"{sid}_foot"
    _box(reqs, fid, sid, MARGIN, 400, CONTENT_W, 36, foot)
    _style(reqs, fid, 0, len(foot), size=8, color=GRAY, font=FONT)
    return idx + 1


def portfolio_title_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    customer_count = report.get("customer_count", 0)
    days = report.get("days", 30)
    quarter_label = report.get("quarter")
    title = "Portfolio Health Review"
    subtitle = f"{customer_count} customers  ·  {_date_range(days, quarter_label, report.get('quarter_start'), report.get('quarter_end'))}"

    _box(reqs, f"{sid}_t", sid, MARGIN, 100, CONTENT_W, 80, title)
    _style(reqs, f"{sid}_t", 0, len(title), bold=True, size=36, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 190, CONTENT_W, 30, subtitle)
    _style(reqs, f"{sid}_s", 0, len(subtitle), size=15, color=LTBLUE, font=FONT)

    generated = report.get("generated", "")
    if generated:
        _box(reqs, f"{sid}_d", sid, MARGIN, 340, CONTENT_W, 20, generated)
        _style(reqs, f"{sid}_d", 0, len(generated), size=10, color=GRAY, font=FONT)

    return idx + 1


def portfolio_signals_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    signals = report.get("portfolio_signals", [])
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio action signals")

    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "").strip() or "Critical Signals"
    lines = [
        f"{str(signal.get('customer') or '').strip()}:  {str(signal.get('signal') or '').strip()}"
        for signal in signals
        if isinstance(signal, dict) and (signal.get("customer") or signal.get("signal"))
    ]
    return render_signal_list_slide(
        reqs,
        sid,
        report,
        idx,
        signals=lines,
        title=title,
        missing_label="portfolio action signals",
    )


def portfolio_trends_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    trends_data = report.get("portfolio_trends", {})
    trends = trends_data.get("trends", [])
    if not trends:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio trends")

    type_colors = {
        "concern": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "opportunity": BLUE,
        "positive": {"red": 0.1, "green": 0.6, "blue": 0.2},
        "insight": NAVY,
    }

    per_page = 8
    trend_chunks = _cap_chunk_list(
        [trends[i: i + per_page] for i in range(0, len(trends), per_page)]
    )
    object_ids: list[str] = []
    for page_index, trend_chunk in enumerate(trend_chunks):
        page_sid = f"{sid}_p{page_index}" if len(trend_chunks) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        _bg(reqs, page_sid, LIGHT)
        title = (
            "Aggregate Trends"
            if len(trend_chunks) == 1
            else f"Aggregate Trends ({page_index + 1} of {len(trend_chunks)})"
        )
        _slide_title(reqs, page_sid, title)
        total_active = trends_data.get("total_active_users", 0)
        total_users = trends_data.get("total_users", 0)
        login_pct = trends_data.get("overall_login_pct", 0)
        header = f"{total_active:,} active users of {total_users:,} total  ·  {login_pct}% login rate"
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 20, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=12, color=NAVY, font=FONT, bold=True)
        y = BODY_Y + 36
        for row_index, trend in enumerate(trend_chunk):
            trend_type = trend.get("type", "insight")
            badge = f"[{trend_type.upper()}]"
            text = trend["trend"]
            customers = trend.get("customers", "")
            line = f"{badge}  {text}"
            if customers:
                line += f"\n     {customers}"
            object_id = f"{page_sid}_t{row_index}"
            _box(reqs, object_id, page_sid, MARGIN, y, CONTENT_W, 34, line)
            _style(reqs, object_id, 0, len(line), size=10, color=NAVY, font=FONT)
            _style(reqs, object_id, 0, len(badge), bold=True, size=10, color=type_colors.get(trend_type, NAVY))
            if customers:
                customer_start = line.index(customers)
                _style(reqs, object_id, customer_start, customer_start + len(customers), size=8, color=GRAY)
            y += 38
    return idx + len(trend_chunks), object_ids


def portfolio_leaders_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    leaders = report.get("portfolio_leaders", {})
    if not leaders:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio leaders")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Customer Leaders")

    categories = [
        ("kei_adoption", "Kei AI Adoption", "adoption_rate", "%"),
        ("executive_engagement", "Executive Engagement", "executives", ""),
        ("write_depth", "Write Depth", "write_ratio", "%"),
        ("export_intensity", "Export Volume", "total_exports", ""),
        ("login_rate", "Weekly Active Rate", "login_pct", "%"),
    ]

    col_w = (CONTENT_W - 20) // 3
    col_h = 150
    positions = [
        (MARGIN, BODY_Y),
        (MARGIN + col_w + 10, BODY_Y),
        (MARGIN + 2 * (col_w + 10), BODY_Y),
        (MARGIN, BODY_Y + col_h + 10),
        (MARGIN + col_w + 10, BODY_Y + col_h + 10),
        (MARGIN + 2 * (col_w + 10), BODY_Y + col_h + 10),
    ]

    for category_index, (key, label, metric, unit) in enumerate(categories):
        entries = leaders.get(key, [])
        if not entries or category_index >= len(positions):
            continue
        x, y = positions[category_index]

        _rect(reqs, f"{sid}_bg{category_index}", sid, x, y, col_w, col_h, LIGHT)

        _box(reqs, f"{sid}_cat{category_index}", sid, x + 8, y + 6, col_w - 16, 18, label)
        _style(reqs, f"{sid}_cat{category_index}", 0, len(label), bold=True, size=10, color=BLUE, font=FONT)

        lines = []
        for entry in entries[:5]:
            value = entry.get(metric, 0)
            if isinstance(value, float):
                value = round(value)
            lines.append(f"{entry['rank']}.  {entry['customer']}  —  {value}{unit}")
        text = "\n".join(lines)

        object_id = f"{sid}_ent{category_index}"
        _box(reqs, object_id, sid, x + 8, y + 28, col_w - 16, col_h - 34, text)
        _style(reqs, object_id, 0, len(text), size=9, color=NAVY, font=FONT)

        offset = 0
        for line in lines:
            dot_end = line.index(".")
            _style(reqs, object_id, offset, offset + dot_end + 1, bold=True, color=BLUE, size=9)
            offset += len(line) + 1

    return idx + 1
