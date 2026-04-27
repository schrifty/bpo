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


def portfolio_title_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    customer_count = report.get("customer_count", 0)
    days = report.get("days", 30)
    quarter_label = report.get("quarter")
    title = "Book of Business Review"
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

    max_rows = 12
    chunks = _cap_chunk_list(
        [signals[i: i + max_rows] for i in range(0, len(signals), max_rows)]
    )
    object_ids: list[str] = []
    for page_index, chunk in enumerate(chunks):
        page_sid = f"{sid}_p{page_index}" if len(chunks) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        _bg(reqs, page_sid, WHITE)
        title = (
            "Critical Signals Across Portfolio"
            if len(chunks) == 1
            else f"Critical Signals ({page_index + 1} of {len(chunks)})"
        )
        _slide_title(reqs, page_sid, title)
        y = BODY_Y
        for row_index, signal in enumerate(chunk):
            severity = signal.get("severity", 0)
            dot = "\u25cf "
            dot_color = {"red": 0.85, "green": 0.15, "blue": 0.15} if severity >= 2 else {
                "red": 0.9,
                "green": 0.65,
                "blue": 0.0,
            }
            customer = signal["customer"]
            signal_text = signal["signal"]
            line = f"{dot}{customer}:  {signal_text}"
            object_id = f"{page_sid}_r{row_index}"
            _box(reqs, object_id, page_sid, MARGIN, y, CONTENT_W, 20, line)
            _style(reqs, object_id, 0, len(line), size=9, color=NAVY, font=FONT)
            _style(reqs, object_id, 0, len(dot), color=dot_color, size=10)
            _style(reqs, object_id, len(dot), len(dot) + len(customer), bold=True, size=9)
            y += 22
    return idx + len(chunks), object_ids


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
