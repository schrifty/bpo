"""Customer usage slide builders."""

from __future__ import annotations

from typing import Any, Callable

from .slide_primitives import (
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import (
    append_slide as _slide,
    append_text_box as _box,
    append_wrapped_text_box as _wrap_box,
)
from .slides_theme import (
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    GRAY,
    MARGIN,
    NAVY,
    _cap_page_count,
    _list_data_rows_fit_span,
)


def features_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> tuple[int, list[str]] | int:
    pages = report["top_pages"]
    features = report["top_features"]
    if not pages and not features:
        return _missing_data_slide(reqs, sid, report, idx, "top pages / feature adoption data")

    font_body = 12
    font_header = 14
    col_gap = 24
    col_w = (CONTENT_W - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_w + col_gap
    insights = report.get("feature_adoption_insights") or {}
    insights_text = (insights.get("narrative") or "").strip() if isinstance(insights, dict) else ""
    frustration = report.get("frustration") or {}
    frustration_note = ""
    if isinstance(frustration, dict) and not frustration.get("error"):
        total_frustration = int(frustration.get("total_frustration_signals") or 0)
        if total_frustration > 0:
            totals = frustration.get("totals") if isinstance(frustration.get("totals"), dict) else {}
            rage = int(totals.get("rageClickCount") or 0)
            top_features = frustration.get("top_features") if isinstance(frustration.get("top_features"), list) else []
            hotspot = ""
            if top_features and isinstance(top_features[0], dict):
                hotspot = str(top_features[0].get("feature") or "")[:36]
            suffix = f" — hotspot: {hotspot}" if hotspot else ""
            frustration_note = f"UX friction (rage/dead/error/U-turn): {total_frustration:,} signals; rage {rage:,}{suffix}"
    if insights_text and frustration_note:
        insights_band = 120
    elif insights_text:
        insights_band = 74
    elif frustration_note:
        insights_band = 52
    else:
        insights_band = 0
    tight_bottom = BODY_BOTTOM - insights_band
    max_items = _list_data_rows_fit_span(
        y_top=BODY_Y,
        y_bottom=tight_bottom,
        font_body_pt=font_body,
        reserved_header_lines=1,
        max_rows_cap=30,
    )

    def _render_column(
        page_sid: str,
        prefix: str,
        col_title: str,
        items: list[dict[str, Any]],
        name_key: str,
        events_key: str,
        events_suffix: str,
        start_rank: int,
        box_h: int,
    ) -> None:
        lines = [col_title]
        slice_items = items[start_rank: start_rank + max_items]
        for rank, item in enumerate(slice_items, start=start_rank + 1):
            name = (item[name_key] or "")[:32]
            if len(item.get(name_key) or "") > 32:
                name = name.rstrip() + "…"
            lines.append(f"  {rank}. {name}  ({item[events_key]:,} {events_suffix})")
        if not slice_items and start_rank == 0:
            lines.append("  No data")
        text = "\n".join(lines)
        oid = f"{page_sid}_{prefix}"
        _box(reqs, oid, page_sid, left_x if prefix == "pg" else right_x, BODY_Y, col_w, box_h, text)
        _style(reqs, oid, 0, len(text), size=font_body, color=NAVY, font=FONT)
        _style(reqs, oid, 0, len(col_title), bold=True, size=font_header, color=BLUE)

    page_count = (len(pages) + max_items - 1) // max_items if pages else 0
    feature_count = (len(features) + max_items - 1) // max_items if features else 0
    num_pages = _cap_page_count(max(page_count, feature_count, 1))
    object_ids: list[str] = []
    for page in range(num_pages):
        page_sid = f"{sid}_p{page}" if num_pages > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page)
        title = "Feature Adoption" if num_pages == 1 else f"Feature Adoption ({page + 1} of {num_pages})"
        _slide_title(reqs, page_sid, title)
        foot_parts: list[str] = []
        if page == 0 and insights_text:
            foot_parts.append(insights_text)
        if page == 0 and frustration_note:
            foot_parts.append(frustration_note)
        foot = "\n\n".join(foot_parts)
        col_bottom = BODY_BOTTOM - (insights_band if foot else 0)
        box_h = col_bottom - BODY_Y
        _render_column(page_sid, "pg", "Top Pages", pages, "name", "events", "events", page * max_items, box_h)
        _render_column(page_sid, "ft", "Top Features", features, "name", "events", "clicks", page * max_items, box_h)
        if foot:
            ins_oid = f"{page_sid}_usagepat"
            _wrap_box(reqs, ins_oid, page_sid, MARGIN, col_bottom, CONTENT_W, insights_band - 4, foot)
            _style(reqs, ins_oid, 0, len(foot), size=10, color=GRAY, font=FONT)
    return idx + num_pages, object_ids


def champions_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> tuple[int, list[str]] | int:
    all_champions = report["champions"]
    all_at_risk = report["at_risk_users"]
    if not all_champions and not all_at_risk:
        return _missing_data_slide(reqs, sid, report, idx, "champion / at-risk user data")

    champions_col_max = 5
    at_risk_col_max = 5

    def _days_inactive(user: dict[str, Any]) -> float:
        days = user.get("days_inactive")
        return float(days) if days is not None else 999.0

    champions = sorted(all_champions, key=_days_inactive)[:champions_col_max]
    at_risk = sorted(all_at_risk, key=_days_inactive)[:at_risk_col_max]

    user_h = 38
    col_gap = 30
    col_w = (CONTENT_W - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_w + col_gap

    def _render_users(
        page_sid: str,
        users: list[dict[str, Any]],
        x: float,
        label: str,
        label_color: dict[str, float],
        detail_fn: Callable[[dict[str, Any]], str],
        prefix: str,
        start_i: int,
    ) -> None:
        y = BODY_Y
        _box(reqs, f"{page_sid}_{prefix}h", page_sid, x, y, col_w, 22, label)
        _style(reqs, f"{page_sid}_{prefix}h", 0, len(label), bold=True, size=14, color=label_color, font=FONT)
        y += 28

        if not users and start_i == 0:
            empty = "No active users" if prefix == "c" else "All users active!"
            _box(reqs, f"{page_sid}_{prefix}e", page_sid, x, y, col_w, 20, empty)
            _style(reqs, f"{page_sid}_{prefix}e", 0, len(empty), size=12, color=GRAY, font=FONT, italic=True)
            return

        for user_index, user in enumerate(users):
            email = user["email"] or "unknown"
            if len(email) > 28:
                email = email[:25] + "..."
            detail = detail_fn(user)
            _box(reqs, f"{page_sid}_{prefix}{start_i + user_index}", page_sid, x, y, col_w, 18, email)
            _style(
                reqs,
                f"{page_sid}_{prefix}{start_i + user_index}",
                0,
                len(email),
                bold=True,
                size=12,
                color=NAVY,
                font=FONT,
            )
            _box(reqs, f"{page_sid}_{prefix}d{start_i + user_index}", page_sid, x + 8, y + 18, col_w - 8, 16, detail)
            _style(reqs, f"{page_sid}_{prefix}d{start_i + user_index}", 0, len(detail), size=10, color=GRAY, font=FONT)
            y += user_h

    def _champ_detail(user: dict[str, Any]) -> str:
        return f"{user['role']}  ·  last seen {user['last_visit']}"

    def _risk_detail(user: dict[str, Any]) -> str:
        days = f"{int(user['days_inactive'])}d ago" if user["days_inactive"] < 999 else "never"
        return f"{user['role']}  ·  {days}"

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Champions & At-Risk Users")
    _render_users(sid, champions, left_x, "Champions", BLUE, _champ_detail, "c", 0)
    _render_users(sid, at_risk, right_x, "At Risk  (2 wk – 6 mo inactive)", GRAY, _risk_detail, "r", 0)
    return idx + 1, [sid]
