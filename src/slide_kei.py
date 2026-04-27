"""Kei AI Adoption slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    kpi_metric_card as _kpi_metric_card,
    pill as _pill,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, NAVY, WHITE


def kei_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    kei = report.get("kei", report)
    total_q = kei.get("total_queries", 0)

    _slide(reqs, sid, idx)
    title = "Kei AI Adoption" if total_q > 0 else "Kei AI Adoption — No Usage"
    _slide_title(reqs, sid, title)

    unique = kei.get("unique_users", 0)
    adoption = kei.get("adoption_rate", 0)
    exec_users = kei.get("executive_users", 0)
    exec_queries = kei.get("executive_queries", 0)

    kpi_h = 58
    gap = 18.0
    krow = BODY_Y + 8
    kcw = (CONTENT_W - 2 * gap) / 3
    _kpi_metric_card(
        reqs,
        f"{sid}_k0",
        sid,
        MARGIN,
        krow,
        kcw,
        kpi_h,
        "Total queries",
        f"{total_q:,}",
        accent=BLUE,
        value_pt=22,
    )
    _kpi_metric_card(
        reqs,
        f"{sid}_k1",
        sid,
        MARGIN + kcw + gap,
        krow,
        kcw,
        kpi_h,
        "Adoption rate",
        f"{adoption}%",
        accent=BLUE,
        value_pt=22,
    )
    _kpi_metric_card(
        reqs,
        f"{sid}_k2",
        sid,
        MARGIN + 2 * (kcw + gap),
        krow,
        kcw,
        kpi_h,
        "Users with queries",
        f"{unique}",
        accent=BLUE,
        value_pt=22,
    )

    exec_y = krow + kpi_h + 10
    if exec_users > 0:
        exec_text = f"  {exec_users} executives ({exec_queries:,} queries)  "
        _pill(reqs, f"{sid}_exec", sid, MARGIN, exec_y, 260, 22, exec_text, BLUE, WHITE)
    else:
        exec_text = "  No executive Kei usage detected  "
        _pill(reqs, f"{sid}_exec", sid, MARGIN, exec_y, 260, 22, exec_text, GRAY, WHITE)

    users = kei.get("users", [])
    lines = ["Kei Users"]
    users_top = exec_y + 28 + 8
    for user in users[:8]:
        email = user.get("email", "unknown")
        if len(email) > 30:
            email = email[:27] + "..."
        role = user.get("role", "")
        exec_flag = " *" if user.get("is_executive") else ""
        lines.append(f"  {email}")
        lines.append(f"    {role}{exec_flag}  ·  {user.get('queries', 0):,} queries")
    if not users:
        lines.append("  No Kei usage in this period")
    text = "\n".join(lines)
    users_h = max(120.0, BODY_BOTTOM - users_top - 4)
    _box(reqs, f"{sid}_users", sid, MARGIN, users_top, CONTENT_W, users_h, text)
    _style(reqs, f"{sid}_users", 0, len(text), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_users", 0, len("Kei Users"), bold=True, size=11, color=BLUE)

    return idx + 1
