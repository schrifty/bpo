"""Engineering portfolio deck slide helpers."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any

from .config import logger
from .slide_primitives import (
    CHART_LEGEND_PT,
    background as _bg,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    rect as _rect,
    slide_title as _slide_title,
    style as _style,
    support_title_includes_project as _support_title_includes_project,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import (
    BODY_BOTTOM,
    BODY_Y,
    BLUE,
    CONTENT_W,
    FONT,
    GRAY,
    MARGIN,
    MAX_PAGINATED_SLIDE_PAGES,
    MONO,
    NAVY,
    SLIDE_H,
    WHITE,
    _cap_chunk_list,
)
from .charts import CHART_AXIS_PT
from .slide_utils import max_chars_one_line_for_table_col

GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
RED = {"red": 0.85, "green": 0.15, "blue": 0.15}

PROJECT_SLIDE_SUBTITLE = {
    "HELP": "Support",
    "CUSTOMER": "Implementation escalations",
    "LEAN": "Engineering escalations",
}

# Embedded column charts on small slide bands: axis/category text one step above CHART_AXIS_PT (12).
_SPRINT_SNAPSHOT_CHART_AXIS_PT = CHART_AXIS_PT + 2


def _format_sprint_name_for_display(name: str) -> str:
    """Normalize Jira sprint labels (e.g. ``Sprint590`` → ``Sprint 590``)."""
    s = (name or "").strip()
    if not s:
        return s
    return re.sub(r"(?i)\b(Sprint)(\d+)", r"\1 \2", s)


def _truncate_one_line(text: str, max_chars: int) -> str:
    t = (text or "").strip()
    if max_chars <= 0:
        return ""
    if len(t) <= max_chars:
        return t
    return t[: max_chars - 1].rstrip() + "…"


def _first_two_description_lines(description: str, line_chars: int) -> tuple[str, str]:
    """Plain-text Jira description as up to two lines (~*line_chars* each), word-aware."""
    body = " ".join((description or "").split())
    if not body or line_chars <= 0:
        return "", ""

    def _take_line(s: str) -> tuple[str, str]:
        if len(s) <= line_chars:
            return s, ""
        chunk = s[:line_chars]
        sp = chunk.rfind(" ")
        split_at = sp if sp >= line_chars // 2 else line_chars
        head = s[:split_at].rstrip()
        tail = s[split_at:].strip() if split_at < len(s) else ""
        return head, tail

    line1, rest = _take_line(body)
    if not rest:
        return line1, ""
    line2, tail = _take_line(rest)
    if tail:
        line2 = (line2 + "…").strip()
    return line1, line2


def _project_slide_bg(project: str) -> dict[str, float]:
    proj = (project or "").strip().upper()
    if proj == "CUSTOMER":
        return {"red": 0.95, "green": 0.98, "blue": 1.0}
    if proj == "LEAN":
        return {"red": 0.95, "green": 1.0, "blue": 0.97}
    if proj == "HELP":
        return {"red": 1.0, "green": 0.96, "blue": 0.96}
    return WHITE


def eng_insight_bullets(
    reqs: list[dict[str, Any]],
    sid: str,
    bullets: list[str],
    x: float,
    y: float,
    w: float,
) -> float:
    """Render 2-3 LeanDNA insight bullets. Returns new y position."""
    if not bullets:
        return y
    for bullet_index, bullet in enumerate(bullets[:3]):
        text = f"· {bullet}"
        _box(reqs, f"{sid}_ins{bullet_index}", sid, x, y, w, 22, text)
        _style(reqs, f"{sid}_ins{bullet_index}", 0, 2, bold=True, size=9, color=BLUE, font=FONT)
        _style(reqs, f"{sid}_ins{bullet_index}", 2, len(text), size=9, color=NAVY, font=FONT)
        y += 22
    return y


def eng_portfolio_title_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Cover slide for the engineering portfolio deck."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    title = "Engineering Review"
    _box(reqs, f"{sid}_t", sid, MARGIN, 100, CONTENT_W, 50, title)
    _style(reqs, f"{sid}_t", 0, len(title), bold=True, size=36, color=WHITE, font=FONT)

    eng = report.get("eng_portfolio") or {}
    sprint = eng.get("sprint") or {}
    sprint_name = _format_sprint_name_for_display(str(sprint.get("name", "") or ""))
    sprint_end = sprint.get("end", "")
    try:
        end_dt = datetime.strptime(sprint_end, "%Y-%m-%d")
        sprint_label = f"{sprint_name}  ·  ends {end_dt.strftime('%b %-d, %Y')}"
    except Exception:
        sprint_label = sprint_name or ""

    sub = f"Sprint: {sprint_label}" if sprint_label else ""
    if sub:
        _box(reqs, f"{sid}_sp", sid, MARGIN, 160, CONTENT_W, 24, sub)
        _style(reqs, f"{sid}_sp", 0, len(sub), size=14, color={"red": 0.6, "green": 0.8, "blue": 1.0}, font=FONT)

    generated = date.today().strftime("%B %-d, %Y")
    gen_text = f"Generated {generated}"
    _box(reqs, f"{sid}_g", sid, MARGIN, SLIDE_H - 60, CONTENT_W, 18, gen_text)
    _style(reqs, f"{sid}_g", 0, len(gen_text), size=10, color={"red": 0.5, "green": 0.6, "blue": 0.7}, font=FONT)
    return idx + 1


def eng_sprint_snapshot_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Sprint snapshot: current sprint state, type mix, active work by theme."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    sprint = eng.get("sprint") or {}
    sprint_name = _format_sprint_name_for_display(str(sprint.get("name", "") or "Current Sprint"))
    sprint_start = sprint.get("start", "")
    sprint_end = sprint.get("end", "")
    try:
        start_dt = datetime.strptime(sprint_start, "%Y-%m-%d")
        end_dt = datetime.strptime(sprint_end, "%Y-%m-%d")
        date_range = f"{start_dt.strftime('%b %-d')} – {end_dt.strftime('%b %-d, %Y')}"
    except Exception:
        date_range = f"{sprint_start} – {sprint_end}"

    in_flight = eng.get("in_flight_count", 0)
    closed = eng.get("closed_count", 0)
    by_status = eng.get("by_status", {})
    active = by_status.get("In Progress", 0) + by_status.get("In Review", 0)
    by_type = eng.get("by_type", {})
    bugs_in_flight = by_type.get("Bug", 0)

    title = f"{sprint_name}: {in_flight} Open, {active} Active, {bugs_in_flight} Bugs"
    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = f"{date_range}   ·   Closed this period: {closed}"
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=9, color=GRAY, font=FONT)

    body_top = BODY_Y + 18
    col_gap = 24
    left_w = (CONTENT_W - col_gap) * 3 // 5
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    left_y = body_top
    themes = eng.get("themes", [])

    theme_header = "Work In Progress — by Theme"
    _box(reqs, f"{sid}_tht", sid, left_x, left_y, left_w, 16, theme_header)
    _style(reqs, f"{sid}_tht", 0, len(theme_header), bold=True, size=11, color=NAVY, font=FONT)
    left_y += 18

    max_theme_total = max((theme["total"] for theme in themes), default=1) or 1
    bar_max = left_w - 120
    row_h = 16

    for row_index, theme in enumerate(themes[:14]):
        if left_y + row_h > BODY_BOTTOM - 80:
            break
        theme_name = theme["theme"][:24]
        total_n = theme["total"]
        active_n = theme["in_progress"]
        bugs_n = theme["bugs"]

        bar_w = max(4, int(total_n / max_theme_total * bar_max))
        label = f"{theme_name}"
        counts = f"{total_n}" + (f" ({active_n} act)" if active_n else "") + (f" {bugs_n}B" if bugs_n else "")
        _box(reqs, f"{sid}_tln{row_index}", sid, left_x, left_y, 96, row_h, label)
        _style(reqs, f"{sid}_tln{row_index}", 0, len(label), size=10, color=NAVY, font=FONT)

        bar_x = left_x + 100
        max_bar_w = left_w - 100 - 52 - 4
        bar_w_capped = min(bar_w, max_bar_w)
        bar_color = {"red": 0.9, "green": 0.4, "blue": 0.0} if bugs_n else BLUE
        _box(reqs, f"{sid}_tbar{row_index}", sid, bar_x, left_y + 4, bar_w_capped, 9, "")
        reqs.append(
            {
                "updateShapeProperties": {
                    "objectId": f"{sid}_tbar{row_index}",
                    "shapeProperties": {
                        "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": bar_color}}},
                        "outline": {
                            "outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                            "weight": {"magnitude": 0.75, "unit": "PT"},
                        },
                    },
                    "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
                }
            }
        )

        _box(reqs, f"{sid}_tcnt{row_index}", sid, bar_x + bar_w_capped + 4, left_y, 48, row_h, counts)
        _style(reqs, f"{sid}_tcnt{row_index}", 0, len(counts), size=10, color=RED if bugs_n else GRAY, font=FONT)
        left_y += row_h

    charts = report.get("_charts")

    right_y = body_top
    if by_type:
        typ_title = "Type Mix"
        _box(reqs, f"{sid}_typ_h", sid, right_x, right_y, right_w, 14, typ_title)
        _style(reqs, f"{sid}_typ_h", 0, len(typ_title), bold=True, size=11, color=NAVY, font=FONT)
        right_y += 16
        if charts:
            from .charts import embed_chart

            type_items = list(by_type.items())[:6]
            ss_id, chart_id = charts.add_bar_chart(
                title="Type Mix",
                labels=[ticket_type for ticket_type, _ in type_items],
                series={"Open tickets": [count for _, count in type_items]},
                horizontal=False,
                show_title=False,
                axis_font_size=_SPRINT_SNAPSHOT_CHART_AXIS_PT,
            )
            embed_chart(reqs, f"{sid}_type_mix", sid, ss_id, chart_id, right_x, right_y, right_w, 120, linked=False)
            right_y += 126

    by_assignee = eng.get("by_assignee", {})
    top_assignees = sorted(by_assignee.items(), key=lambda item: -item[1])[:7]
    if top_assignees:
        ass_title = "WIP by Engineer"
        _box(reqs, f"{sid}_ass_h", sid, right_x, right_y, right_w, 14, ass_title)
        _style(reqs, f"{sid}_ass_h", 0, len(ass_title), bold=True, size=11, color=NAVY, font=FONT)
        right_y += 16
        if charts:
            from .charts import embed_chart

            ss_id, chart_id = charts.add_bar_chart(
                title="WIP by Engineer",
                labels=[(name.split()[0] if name else "Unassigned") for name, _ in top_assignees],
                series={"Open tickets": [count for _, count in top_assignees]},
                horizontal=False,
                show_title=False,
                axis_font_size=_SPRINT_SNAPSHOT_CHART_AXIS_PT,
            )
            embed_chart(reqs, f"{sid}_wip_eng", sid, ss_id, chart_id, right_x, right_y, right_w, 120, linked=False)
            right_y += 126

    insights = (eng.get("insights") or {}).get("sprint_snapshot", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


def eng_bug_health_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Bug health: open bugs (full-width summaries), priority mix in subtitle, blockers under list."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    open_bugs = eng.get("open_bugs") or []
    blocker_crit = eng.get("blocker_critical") or []

    if blocker_crit:
        title = f"{len(open_bugs)} Open Bugs — {len(blocker_crit)} Blocker/Critical Need Attention"
    elif open_bugs:
        title = f"{len(open_bugs)} Open Bugs — No Blockers Currently Active"
    else:
        title = "Bug Backlog Clear — No Open Bugs"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    jira_base = eng.get("base_url", "")
    bar = f"Open bugs: {len(open_bugs)}   |   Blocker / Critical: {len(blocker_crit)}"
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 16, bar)
    _style(reqs, f"{sid}_bar", 0, len(bar), size=9, color=GRAY, font=FONT)
    _style(
        reqs,
        f"{sid}_bar",
        len("Open bugs: "),
        len(f"Open bugs: {len(open_bugs)}"),
        bold=True,
        color=RED if open_bugs else GREEN,
    )
    blocker_start = bar.index("Blocker")
    _style(
        reqs,
        f"{sid}_bar",
        blocker_start,
        blocker_start + len(f"Blocker / Critical: {len(blocker_crit)}"),
        bold=True,
        color=RED if blocker_crit else GREEN,
    )

    prio_color = {
        "Blocker": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "Critical": {"red": 0.9, "green": 0.4, "blue": 0.0},
        "Major": NAVY,
        "Minor": GRAY,
    }
    by_priority: dict[str, int] = {}
    for bug in open_bugs:
        priority = bug["priority"]
        short = priority.split(":")[0] if ":" in priority else priority
        by_priority[short] = by_priority.get(short, 0) + 1

    prio_order = ["Blocker", "Critical", "Major", "Minor"]
    body_top = BODY_Y + 18
    if by_priority:
        prio_label = "By priority:  "
        prio_chunks: list[tuple[int, int, dict[str, float]]] = []
        line2 = prio_label
        for pname in prio_order:
            cnt = int(by_priority.get(pname, 0) or 0)
            if cnt <= 0:
                continue
            sep = "  ·  " if line2 != prio_label else ""
            line2 += sep
            line2 += str(cnt)
            c0, c1 = len(line2) - len(str(cnt)), len(line2)
            prio_chunks.append((c0, c1, prio_color.get(pname, NAVY)))
            line2 += f" {pname}"
            n0, n1 = c1, len(line2)
            prio_chunks.append((n0, n1, NAVY))

        bar2_y = BODY_Y + 17
        _box(reqs, f"{sid}_bar2", sid, MARGIN, bar2_y, CONTENT_W, 16, line2)
        _style(reqs, f"{sid}_bar2", 0, len(line2), size=9, color=GRAY, font=FONT)
        _style(reqs, f"{sid}_bar2", 0, len(prio_label), bold=True, size=9, color=NAVY, font=FONT)
        for a, b, rgb in prio_chunks:
            _style(reqs, f"{sid}_bar2", a, b, bold=True, size=9, color=rgb, font=FONT)
        body_top = bar2_y + 18

    insights = (eng.get("insights") or {}).get("bug_health", [])
    insights_h = (len(insights) * 22 + 4) if insights else 0
    blocker_rows = min(len(blocker_crit), 6) if blocker_crit else 0
    # Per ticket: meta row + subject + 2 description lines + small gap
    _bug_ticket_h = 16 + 16 + 15 + 15 + 4
    blocker_section_h = 0
    if blocker_crit:
        blocker_section_h = 18 + 18 + blocker_rows * _bug_ticket_h + 8

    list_bottom_cap = BODY_BOTTOM - insights_h - blocker_section_h - 8
    left_x = MARGIN
    list_w = CONTENT_W
    desc_inner_w = float(CONTENT_W - 16)
    subj_max = max_chars_one_line_for_table_col(desc_inner_w, 9.0)
    desc_line_chars = max_chars_one_line_for_table_col(desc_inner_w, 8.0)

    left_y = body_top
    _box(reqs, f"{sid}_bl_h", sid, left_x, left_y, list_w, 16, "Open Bugs")
    _style(reqs, f"{sid}_bl_h", 0, 9, bold=True, size=11, color=NAVY, font=FONT)
    left_y += 18

    ticket_h = _bug_ticket_h
    for bug_index, bug in enumerate(open_bugs[:12]):
        if left_y + ticket_h > list_bottom_cap:
            break
        key = bug["key"]
        priority = bug["priority"]
        prio_short = priority.split(":")[0] if ":" in priority else priority
        assignee = bug.get("assignee") or ""
        first_name = assignee.split()[0] if assignee else "—"
        subject = _truncate_one_line(str(bug.get("summary") or ""), subj_max)
        d1, d2 = _first_two_description_lines(str(bug.get("description_text") or ""), desc_line_chars)

        key_line = f"{key}  [{prio_short}]  {first_name}"
        link = f"{jira_base}/browse/{key}" if jira_base else None
        _box(reqs, f"{sid}_bk{bug_index}", sid, left_x, left_y, list_w, 16, key_line)
        _style(
            reqs,
            f"{sid}_bk{bug_index}",
            0,
            len(key),
            bold=True,
            size=9,
            color=prio_color.get(prio_short, RED),
            font=MONO,
            link=link,
        )
        _style(reqs, f"{sid}_bk{bug_index}", len(key), len(key_line), size=9, color=GRAY, font=FONT)
        left_y += 16

        _box(reqs, f"{sid}_bsj{bug_index}", sid, left_x + 8, left_y, list_w - 8, 16, subject)
        _style(reqs, f"{sid}_bsj{bug_index}", 0, len(subject), size=9, color=NAVY, font=FONT, bold=True)
        left_y += 16
        if not d1 and not d2:
            d1 = "—"
        _box(reqs, f"{sid}_bsd1{bug_index}", sid, left_x + 8, left_y, list_w - 8, 15, d1)
        _style(reqs, f"{sid}_bsd1{bug_index}", 0, len(d1), size=8, color=GRAY, font=FONT)
        left_y += 15
        _box(reqs, f"{sid}_bsd2{bug_index}", sid, left_x + 8, left_y, list_w - 8, 15, d2)
        _style(reqs, f"{sid}_bsd2{bug_index}", 0, len(d2), size=8, color=GRAY, font=FONT)
        left_y += 15 + 4

    if blocker_crit:
        left_y += 6
        bh = "Blockers & Criticals"
        _box(reqs, f"{sid}_bch", sid, left_x, left_y, list_w, 16, bh)
        _style(reqs, f"{sid}_bch", 0, len(bh), bold=True, size=11, color=RED, font=FONT)
        left_y += 18
        for bug_index, bug in enumerate(blocker_crit[:6]):
            key = bug["key"]
            priority = bug.get("priority") or ""
            prio_short = priority.split(":")[0] if ":" in priority else (priority or "—")
            assignee = bug.get("assignee") or ""
            first_name = assignee.split()[0] if assignee else "—"
            link = f"{jira_base}/browse/{key}" if jira_base else None
            subject = _truncate_one_line(str(bug.get("summary") or ""), subj_max)
            d1, d2 = _first_two_description_lines(str(bug.get("description_text") or ""), desc_line_chars)
            key_line = f"{key}  [{prio_short}]  {first_name}"
            _box(reqs, f"{sid}_bc{bug_index}", sid, left_x, left_y, list_w, 16, key_line)
            _style(
                reqs,
                f"{sid}_bc{bug_index}",
                0,
                len(key),
                bold=True,
                size=9,
                color=RED,
                font=MONO,
                link=link,
            )
            _style(reqs, f"{sid}_bc{bug_index}", len(key), len(key_line), size=9, color=GRAY, font=FONT)
            left_y += 16
            _box(reqs, f"{sid}_bcsj{bug_index}", sid, left_x + 8, left_y, list_w - 8, 16, subject)
            _style(reqs, f"{sid}_bcsj{bug_index}", 0, len(subject), size=9, color=NAVY, font=FONT, bold=True)
            left_y += 16
            if not d1 and not d2:
                d1 = "—"
            _box(reqs, f"{sid}_bcsd1{bug_index}", sid, left_x + 8, left_y, list_w - 8, 15, d1)
            _style(reqs, f"{sid}_bcsd1{bug_index}", 0, len(d1), size=8, color=GRAY, font=FONT)
            left_y += 15
            _box(reqs, f"{sid}_bcsd2{bug_index}", sid, left_x + 8, left_y, list_w - 8, 15, d2)
            _style(reqs, f"{sid}_bcsd2{bug_index}", 0, len(d2), size=8, color=GRAY, font=FONT)
            left_y += 15 + 4

    if insights:
        eng_insight_bullets(reqs, sid, insights, MARGIN, left_y + 8, CONTENT_W)

    return idx + 1


def eng_velocity_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Velocity & throughput: combo chart, weekly table, pipeline bars, then insights (no overlap)."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    throughput = eng.get("throughput") or []
    closed_count = eng.get("closed_count", 0)
    in_flight = eng.get("in_flight_count", 0)

    recent_throughput = throughput[-4:] if throughput else []
    avg_closed = (
        sum(week.get("resolved", 0) for week in recent_throughput) / len(recent_throughput)
        if recent_throughput
        else 0
    )
    avg_created = (
        sum(week.get("created", 0) for week in recent_throughput) / len(recent_throughput)
        if recent_throughput
        else 0
    )
    net = avg_closed - avg_created
    if net > 2:
        title = f"Backlog Shrinking — {net:.0f} More Tickets Closed Than Created Per Week"
    elif net < -2:
        title = f"Backlog Growing — {abs(net):.0f} More Created Than Closed Per Week"
    else:
        title = f"Flow Balanced — Averaging {avg_closed:.0f} Tickets Closed Per Week"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = f"Open: {in_flight}   ·   Closed this period: {closed_count}   ·   Last 12 weeks"
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_bar", 0, len(context), size=9, color=GRAY, font=FONT)

    body_top = BODY_Y + 22
    col_gap = 20
    left_w = (CONTENT_W - col_gap) * 3 // 5
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    insights_list = (eng.get("insights") or {}).get("velocity", [])
    insight_bullets = insights_list[:3]
    ins_h = len(insight_bullets) * 22
    ins_gap = 12 if insight_bullets else 0
    content_ceiling = BODY_BOTTOM - ins_h - ins_gap - 6

    by_status = eng.get("by_status") or {}
    status_items_all = sorted(by_status.items(), key=lambda item: -item[1])
    status_items = status_items_all[:6]

    right_header_h = 20 + 16
    right_bottom_est = body_top + right_header_h + len(status_items) * 14

    recent_weeks = throughput[-12:] if len(throughput) >= 12 else throughput
    charts = report.get("_charts")

    CHART_GAP = 6
    TABLE_PRE = 4
    TH = 14
    ROW_H = 12

    chosen_chart_h = 100
    chosen_rows = 6
    if recent_weeks and charts:
        found = False
        for chart_h in range(168, 72, -12):
            for n_rows in range(8, 2, -1):
                left_bottom = body_top + chart_h + CHART_GAP + TABLE_PRE + TH + n_rows * ROW_H
                if max(left_bottom, right_bottom_est) <= content_ceiling:
                    chosen_chart_h = chart_h
                    chosen_rows = min(n_rows, len(recent_weeks))
                    found = True
                    break
            if found:
                break
        if not found:
            chosen_chart_h = 72
            chosen_rows = min(3, len(recent_weeks) or 1)
    elif recent_weeks:
        chosen_chart_h = 0
        chosen_rows = min(
            8,
            max(
                1,
                int((content_ceiling - body_top - TABLE_PRE - TH) // ROW_H),
            ),
        )

    left_y = body_top
    if recent_weeks and charts:
        try:
            from .charts import embed_chart

            ss_id, chart_id = charts.add_combo_chart(
                title="Weekly Throughput",
                labels=[week.get("label", "") for week in recent_weeks],
                bar_series={"Created": [week.get("created", 0) for week in recent_weeks]},
                line_series={"Closed": [week.get("resolved", 0) for week in recent_weeks]},
                show_title=False,
            )
            embed_chart(
                reqs, f"{sid}_chart", sid, ss_id, chart_id,
                left_x, left_y, left_w, chosen_chart_h, linked=False,
            )
            left_y += chosen_chart_h + CHART_GAP
        except Exception as exc:
            logger.warning("Throughput chart embed failed: %s", exc)

    if recent_weeks:
        left_y += TABLE_PRE
        header = "Week        Created  Closed"
        _box(reqs, f"{sid}_wt_h", sid, left_x, left_y, left_w, TH, header)
        _style(reqs, f"{sid}_wt_h", 0, len(header), bold=True, size=8, color=GRAY, font=MONO)
        left_y += TH
        tail = recent_weeks[-chosen_rows:] if chosen_rows > 0 else []
        for week in tail:
            row = f"{week['label']:<12}  {week.get('created', 0):>5}    {week.get('resolved', 0):>4}"
            wk = str(week.get("week", week.get("label", "w")))
            safe_wk = "".join(c if c.isalnum() else "_" for c in wk)[:24]
            _box(reqs, f"{sid}_wr{safe_wk}", sid, left_x, left_y, left_w, ROW_H, row)
            _style(reqs, f"{sid}_wr{safe_wk}", 0, len(row), size=8, color=NAVY, font=MONO)
            left_y += ROW_H

    right_y = body_top
    _box(reqs, f"{sid}_qlh", sid, right_x, right_y, right_w, 16, "Quarterly Goal Tracking")
    _style(reqs, f"{sid}_qlh", 0, len("Quarterly Goal Tracking"), bold=True, size=11, color=NAVY, font=FONT)
    right_y += 20

    _box(reqs, f"{sid}_sbh", sid, right_x, right_y, right_w, 14, "Pipeline Status")
    _style(reqs, f"{sid}_sbh", 0, len("Pipeline Status"), bold=True, size=10, color=NAVY, font=FONT)
    right_y += 16
    total_in_flight = sum(by_status.values()) or 1
    max_status = max(by_status.values()) if by_status else 1
    pct_col_w = 30
    bar_max_w = right_w - 76 - pct_col_w - 4
    for status, count in status_items:
        pct = int(count / total_in_flight * 100)
        bar_w = max(3, int(count / max_status * bar_max_w))
        safe_status = status.replace(" ", "_").replace("/", "_")[:10]
        is_active = status in ("In Progress", "In Review")
        bar_color = BLUE if is_active else {"red": 0.75, "green": 0.80, "blue": 0.90}
        label = f"{count}  {status}"
        _box(reqs, f"{sid}_sl_{safe_status}", sid, right_x, right_y, 70, 13, label)
        _style(
            reqs,
            f"{sid}_sl_{safe_status}",
            0,
            len(str(count)),
            bold=is_active,
            size=8,
            color=BLUE if is_active else NAVY,
            font=FONT,
        )
        _style(
            reqs,
            f"{sid}_sl_{safe_status}",
            len(str(count)) + 2,
            len(label),
            size=8,
            color=GRAY,
            font=FONT,
        )
        _box(reqs, f"{sid}_sb_{safe_status}", sid, right_x + 72, right_y + 3, bar_w, 8, "")
        reqs.append(
            {
                "updateShapeProperties": {
                    "objectId": f"{sid}_sb_{safe_status}",
                    "shapeProperties": {
                        "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": bar_color}}},
                        "outline": {
                            "outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                            "weight": {"magnitude": 0.75, "unit": "PT"},
                        },
                    },
                    "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
                }
            }
        )
        pct_label = f"{pct}%"
        pct_x = right_x + right_w - pct_col_w
        _box(reqs, f"{sid}_sp_{safe_status}", sid, pct_x, right_y, pct_col_w, 13, pct_label)
        _style(reqs, f"{sid}_sp_{safe_status}", 0, len(pct_label), size=8, color=GRAY, font=FONT)
        right_y += 14

    content_bottom = max(left_y, right_y)
    if insight_bullets:
        eng_insight_bullets(
            reqs, sid, insight_bullets, MARGIN, content_bottom + ins_gap, CONTENT_W,
        )

    return idx + 1


def eng_enhancements_open_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Open enhancement requests, paginated with all tickets shown up to the slide cap."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    enhancements = eng.get("enhancements") or {}
    open_tickets = enhancements.get("open", [])
    open_count = enhancements.get("open_count", 0)
    shipped_count = enhancements.get("shipped_count", 0)
    declined_count = enhancements.get("declined_count", 0)
    jira_base = eng.get("base_url", "")

    tickets_per_page = 2
    pages_all = [open_tickets[i : i + tickets_per_page] for i in range(0, max(1, len(open_tickets)), tickets_per_page)]
    pages = _cap_chunk_list(pages_all)
    num_pages = len(pages)
    omitted_tickets = sum(len(page) for page in pages_all[len(pages) :])

    for page_index, page_tickets in enumerate(pages):
        page_sid = f"{sid}_p{page_index}"
        if page_index == 0:
            title = (
                f"{open_count} Open Enhancement Request  ({page_index + 1} of {num_pages})"
                if num_pages > 1
                else (
                    "1 Open Enhancement Request in Backlog"
                    if open_count == 1
                    else f"{open_count} Open Enhancement Requests in Backlog"
                )
            )
        else:
            title = f"Enhancement Requests — Open  ({page_index + 1} of {num_pages})"

        _slide(reqs, page_sid, idx)
        _bg(reqs, page_sid, WHITE)
        _slide_title(reqs, page_sid, title)

        bar = f"Open backlog: {open_count}   |   Recently shipped: {shipped_count}   |   Declined: {declined_count}"
        _box(reqs, f"{page_sid}_bar", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, bar)
        _style(reqs, f"{page_sid}_bar", 0, len(bar), size=9, color=GRAY, font=FONT)

        inner_w = float(CONTENT_W - 16)
        er_subj_max = max_chars_one_line_for_table_col(inner_w, 9.0)
        er_desc_lc = max_chars_one_line_for_table_col(inner_w, 8.0)

        y = BODY_Y + 22
        for row_index, ticket in enumerate(page_tickets):
            key = ticket["key"]
            link = f"{jira_base}/browse/{key}" if jira_base else None
            status = ticket.get("status", "Open")

            raw_date = ticket.get("updated", "")
            try:
                updated = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%b %-d, %Y") if raw_date else ""
            except ValueError:
                updated = raw_date

            meta = f"{key}  [{status}]"
            if updated:
                meta += f"  ·  updated {updated}"
            _box(reqs, f"{page_sid}_k{row_index}", page_sid, MARGIN, y, CONTENT_W, 14, meta)
            _style(reqs, f"{page_sid}_k{row_index}", 0, len(key), bold=True, size=9, color=BLUE, font=MONO, link=link)
            _style(reqs, f"{page_sid}_k{row_index}", len(key), len(meta), size=9, color=GRAY, font=FONT)
            y += 14

            subject = _truncate_one_line(str(ticket.get("summary") or ""), er_subj_max)
            d1, d2 = _first_two_description_lines(str(ticket.get("description_text") or ""), er_desc_lc)
            if not d1 and not d2:
                d1 = "—"
            _box(reqs, f"{page_sid}_sj{row_index}", page_sid, MARGIN + 8, y, CONTENT_W - 8, 16, subject)
            _style(reqs, f"{page_sid}_sj{row_index}", 0, len(subject), size=9, color=NAVY, font=FONT, bold=True)
            y += 16
            _box(reqs, f"{page_sid}_d1{row_index}", page_sid, MARGIN + 8, y, CONTENT_W - 8, 15, d1)
            _style(reqs, f"{page_sid}_d1{row_index}", 0, len(d1), size=8, color=GRAY, font=FONT)
            y += 15
            _box(reqs, f"{page_sid}_d2{row_index}", page_sid, MARGIN + 8, y, CONTENT_W - 8, 15, d2)
            _style(reqs, f"{page_sid}_d2{row_index}", 0, len(d2), size=8, color=GRAY, font=FONT)
            y += 15 + 8

        idx += 1

    if omitted_tickets:
        omit_sid = f"{sid}_omit"
        _slide(reqs, omit_sid, idx)
        _bg(reqs, omit_sid, WHITE)
        _slide_title(reqs, omit_sid, "Enhancement Requests — Open (continued)")
        note = (
            f"{omitted_tickets} additional open enhancement requests not shown "
            f"(pagination cap {MAX_PAGINATED_SLIDE_PAGES} pages). "
            f"Full backlog: {open_count} open tickets. View in Jira for complete list."
        )
        _box(reqs, f"{omit_sid}_note", omit_sid, MARGIN, BODY_Y + 10, CONTENT_W, 40, note)
        _style(reqs, f"{omit_sid}_note", 0, len(note), size=11, color=GRAY, font=FONT)
        idx += 1

    return idx


def eng_enhancements_shipped_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Recently shipped enhancement requests."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    enhancements = eng.get("enhancements") or {}
    shipped_count = enhancements.get("shipped_count", 0)
    open_count = enhancements.get("open_count", 0)
    declined_count = enhancements.get("declined_count", 0)

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, f"{shipped_count} Enhancement Requests Recently Shipped")

    bar = f"Recently shipped: {shipped_count}   |   Open backlog: {open_count}   |   Declined: {declined_count}"
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 18, bar)
    _style(reqs, f"{sid}_bar", 0, len(bar), size=9, color=GRAY, font=FONT)

    jira_base = eng.get("base_url", "")
    inner_w = float(CONTENT_W - 16)
    er_subj_max = max_chars_one_line_for_table_col(inner_w, 9.0)
    er_desc_lc = max_chars_one_line_for_table_col(inner_w, 8.0)
    ticket_h = 14 + 16 + 15 + 15 + 10
    y = BODY_Y + 22

    shipped = enhancements.get("shipped") or []
    if not shipped:
        msg = (
            "No enhancement requests were marked as resolved in Jira in the last 12 months. "
            "This may indicate that shipped work isn't being closed out in the ER project — "
            "worth a quick audit of the Jira workflow."
        )
        _box(reqs, f"{sid}_empty", sid, MARGIN, y + 20, CONTENT_W, 60, msg)
        _style(reqs, f"{sid}_empty", 0, len(msg), size=11, color=GRAY, font=FONT)
        flag = "Action needed: update Jira ER tickets when shipping"
        _box(reqs, f"{sid}_flag", sid, MARGIN, y + 90, CONTENT_W, 20, flag)
        _style(reqs, f"{sid}_flag", 0, len(flag), bold=True, size=10, color=RED, font=FONT)
        return idx + 1

    for row_index, ticket in enumerate(shipped[:10]):
        if y + ticket_h > BODY_BOTTOM:
            break
        key = ticket["key"]
        link = f"{jira_base}/browse/{key}" if jira_base else None
        raw_date = ticket.get("updated", "")
        try:
            updated = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%b %-d, %Y") if raw_date else ""
        except ValueError:
            updated = raw_date

        meta = f"{key}  [Shipped]"
        if updated:
            meta += f"  ·  shipped {updated}"
        _box(reqs, f"{sid}_k{row_index}", sid, MARGIN, y, CONTENT_W, 14, meta)
        _style(reqs, f"{sid}_k{row_index}", 0, len(key), bold=True, size=9, color=GREEN, font=MONO, link=link)
        _style(reqs, f"{sid}_k{row_index}", len(key), len(meta), size=9, color=GRAY, font=FONT)
        y += 14

        subject = _truncate_one_line(str(ticket.get("summary") or ""), er_subj_max)
        d1, d2 = _first_two_description_lines(str(ticket.get("description_text") or ""), er_desc_lc)
        if not d1 and not d2:
            d1 = "—"
        _box(reqs, f"{sid}_sj{row_index}", sid, MARGIN + 8, y, CONTENT_W - 8, 16, subject)
        _style(reqs, f"{sid}_sj{row_index}", 0, len(subject), size=9, color=NAVY, font=FONT, bold=True)
        y += 16
        _box(reqs, f"{sid}_d1{row_index}", sid, MARGIN + 8, y, CONTENT_W - 8, 15, d1)
        _style(reqs, f"{sid}_d1{row_index}", 0, len(d1), size=8, color=GRAY, font=FONT)
        y += 15
        _box(reqs, f"{sid}_d2{row_index}", sid, MARGIN + 8, y, CONTENT_W - 8, 15, d2)
        _style(reqs, f"{sid}_d2{row_index}", 0, len(d2), size=8, color=GRAY, font=FONT)
        y += 15 + 10

    return idx + 1


def eng_support_pressure_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Cross-customer support pressure feeding into engineering."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    support_pressure = eng.get("support_pressure") or {}
    total = support_pressure.get("total", 0)
    open_count = support_pressure.get("open", 0)
    escalated = support_pressure.get("escalated_to_eng", 0)
    bugs = support_pressure.get("open_bugs", 0)
    days = eng.get("days", 30)

    if total == 1:
        title = "1 Escalation from Support"
    elif total:
        title = f"{total:,} Escalations from Support"
    else:
        title = "Support Pressure — No Ticket Data Available"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    end_d = date.today()
    start_d = end_d - timedelta(days=days)
    context = f"{start_d.strftime('%b %-d')} – {end_d.strftime('%b %-d, %Y')}  ({days}d)"
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=9, color=GRAY, font=FONT)

    body_top = BODY_Y + 18
    col_gap = 24
    left_w = (CONTENT_W - col_gap) * 3 // 5
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    by_priority = support_pressure.get("by_priority") or {}
    left_y = body_top
    priority_header = "Ticket Volume by Priority"
    _box(reqs, f"{sid}_ph", sid, left_x, left_y, left_w, 16, priority_header)
    _style(reqs, f"{sid}_ph", 0, len(priority_header), bold=True, size=12, color=NAVY, font=FONT)
    left_y += 22

    priority_order = ["Blocker", "Critical", "Major", "Minor", "Unknown"]
    priority_colors = {
        "Blocker": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "Critical": {"red": 0.9, "green": 0.4, "blue": 0.0},
        "Major": BLUE,
        "Minor": {"red": 0.48, "green": 0.77, "blue": 0.98},
        "Unknown": GRAY,
    }
    priority_items = [(priority, by_priority.get(priority, 0)) for priority in priority_order if by_priority.get(priority, 0) > 0]
    max_value = max(value for _, value in priority_items) if priority_items else 1
    bar_max_w = left_w - 100

    for priority_index, (priority, count) in enumerate(priority_items):
        bar_w = max(6, int(count / max_value * bar_max_w))
        is_critical = priority in ("Blocker", "Critical")
        _box(reqs, f"{sid}_pl{priority_index}", sid, left_x, left_y, 88, 26, priority)
        _style(
            reqs,
            f"{sid}_pl{priority_index}",
            0,
            len(priority),
            size=12,
            bold=is_critical,
            color=priority_colors.get(priority, NAVY),
            font=FONT,
        )
        _box(reqs, f"{sid}_pb{priority_index}", sid, left_x + 92, left_y + 6, bar_w, 14, "")
        reqs.append(
            {
                "updateShapeProperties": {
                    "objectId": f"{sid}_pb{priority_index}",
                    "shapeProperties": {
                        "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": priority_colors.get(priority, NAVY)}}},
                        "outline": {
                            "outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                            "weight": {"magnitude": 0.75, "unit": "PT"},
                        },
                    },
                    "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
                }
            }
        )
        count_label = str(count)
        _box(reqs, f"{sid}_pc{priority_index}", sid, left_x + 96 + bar_w, left_y + 4, 40, 18, count_label)
        _style(
            reqs,
            f"{sid}_pc{priority_index}",
            0,
            len(count_label),
            size=11,
            bold=is_critical,
            color=priority_colors.get(priority, NAVY),
            font=FONT,
        )
        left_y += 30

    kpi_h = 52
    kpi_gap = 6
    right_y = body_top
    kpi_cards = [
        ("Total", total, None),
        ("Open", open_count, None),
        ("Escalated to Eng", escalated, RED if escalated > 5 else BLUE),
        ("Open Bugs", bugs, RED if bugs > 3 else BLUE),
    ]
    for card_index, (label, value, color) in enumerate(kpi_cards):
        accent = color or BLUE
        _kpi_metric_card(
            reqs,
            f"{sid}_spk{card_index}",
            sid,
            right_x,
            right_y,
            right_w,
            kpi_h,
            label,
            str(value),
            accent=accent,
            value_pt=22,
        )
        right_y += kpi_h + kpi_gap

    insights = (eng.get("insights") or {}).get("support_pressure", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


def eng_jira_project_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Per-project Jira snapshot with status and assignee bar charts."""
    eng = report.get("eng_portfolio") or {}
    entry = report.get("_current_slide") or {}
    project_key = (entry.get("jira_project") or "HELP").strip().upper()
    snapshots = eng.get("project_snapshots") or {}
    snapshot = snapshots.get(project_key) or {}

    if snapshot.get("error") and "open_count" not in snapshot:
        return _missing_data_slide(
            reqs,
            sid,
            report,
            idx,
            f"Jira project data ({project_key}): {snapshot.get('error', 'unavailable')}",
        )

    title = entry.get("title") or f"{project_key} — {PROJECT_SLIDE_SUBTITLE.get(project_key, project_key)}"
    open_count = int(snapshot.get("open_count") or 0)
    by_status = snapshot.get("by_status_open") or {}
    median_open = snapshot.get("median_open_age_days")
    avg_cycle = snapshot.get("avg_resolved_cycle_days")
    resolved_six_months = int(snapshot.get("resolved_in_6mo_count") or 0)
    assignee_rows = snapshot.get("assignee_resolved_table") or []

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    open_label = (
        f"Median age of open tickets: {median_open} d"
        if median_open is not None
        else "Median age of open tickets: —"
    )
    cycle_label = (
        f"Avg open→resolved (6 mo): {avg_cycle} d"
        if avg_cycle is not None
        else "Avg open→resolved (6 mo): —"
    )
    meta = f"Total open: {open_count}   ·   {open_label}   ·   {cycle_label}   ·   Resolved (6 mo): {resolved_six_months}"
    _box(reqs, f"{sid}_meta", sid, MARGIN, BODY_Y, CONTENT_W, 30, meta)
    _style(reqs, f"{sid}_meta", 0, len(meta), size=10, color=GRAY, font=FONT)

    body_top = BODY_Y + 32
    col_gap = 24
    left_w = (CONTENT_W - col_gap) // 2
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap
    charts = report.get("_charts")

    left_y = body_top
    status_header = "Open tickets by status"
    _box(reqs, f"{sid}_hh", sid, left_x, left_y, left_w, 14, status_header)
    _style(reqs, f"{sid}_hh", 0, len(status_header), bold=True, size=10, color=NAVY, font=FONT)
    left_y += 22

    status_items = list(by_status.items())[:8]
    if status_items and charts:
        try:
            from .charts import embed_chart

            ss_id, chart_id = charts.add_bar_chart(
                title=f"{project_key} Open Tickets by Status",
                labels=[status for status, _ in status_items],
                series={"Open tickets": [count for _, count in status_items]},
                horizontal=False,
            )
            embed_chart(reqs, f"{sid}_status_chart", sid, ss_id, chart_id, left_x, left_y, left_w, 188, linked=False)
        except Exception as exc:
            logger.warning("Jira project status chart failed (%s): %s", project_key, exc)

    if not status_items:
        empty = "No open tickets"
        _box(reqs, f"{sid}_no_st", sid, left_x, left_y + 68, left_w, 14, empty)
        _style(reqs, f"{sid}_no_st", 0, len(empty), size=9, color=GRAY, font=FONT)

    right_y = body_top
    assignee_header = "Resolved tickets by assignee (6 mo)"
    _box(reqs, f"{sid}_th", sid, right_x, right_y, right_w, 14, assignee_header)
    _style(reqs, f"{sid}_th", 0, len(assignee_header), bold=True, size=10, color=NAVY, font=FONT)
    right_y += 22

    if assignee_rows and charts:
        try:
            from .charts import embed_chart

            assignee_items = assignee_rows[:8]
            ss_id, chart_id = charts.add_bar_chart(
                title=f"{project_key} Resolved Tickets by Assignee",
                labels=[(row.get("assignee") or "Unassigned")[:24] for row in assignee_items],
                series={"Resolved (6 mo)": [int(row.get("6m", 0)) for row in assignee_items]},
                horizontal=True,
            )
            embed_chart(reqs, f"{sid}_assignee_chart", sid, ss_id, chart_id, right_x, right_y, right_w, 188, linked=False)
        except Exception as exc:
            logger.warning("Jira project assignee chart failed (%s): %s", project_key, exc)

    if not assignee_rows:
        empty = "No resolved tickets in last 6 months"
        _box(reqs, f"{sid}_no_as", sid, right_x, right_y + 56, right_w, 14, empty)
        _style(reqs, f"{sid}_no_as", 0, len(empty), size=8, color=GRAY, font=FONT)

    note = "Assignee chart shows resolved tickets in the last 6 months."
    _box(reqs, f"{sid}_fn", sid, MARGIN, BODY_BOTTOM - 12, CONTENT_W, 10, note)
    _style(reqs, f"{sid}_fn", 0, len(note), size=6, color=GRAY, font=FONT)

    return idx + 1


def _render_project_volume_trends(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    *,
    trends: dict[str, Any],
    project: str,
    bg: dict[str, float],
) -> int:
    """Shared layout: monthly created vs resolved, escalated and non-escalated."""
    all_months = list(trends.get("all") or [])
    escalated_months = list(trends.get("escalated") or [])
    non_escalated_months = list(trends.get("non_escalated") or [])
    charts = report.get("_charts")

    if not all_months:
        return _missing_data_slide(
            reqs,
            sid,
            report,
            idx,
            f"{project} ticket volume trends — no monthly series (unexpected empty response)",
        )
    if not charts:
        return _missing_data_slide(reqs, sid, report, idx, f"{project} ticket volume trends — chart embedding unavailable")

    recent = all_months[-3:]
    recent_created = sum(month.get("created", 0) for month in recent)
    recent_resolved = sum(month.get("resolved", 0) for month in recent)
    net = recent_created - recent_resolved
    if net > 10:
        headline = f"Volume Rising - {net} more tickets created than resolved in last 3 full months"
    elif net < -10:
        headline = f"Volume Easing - {abs(net)} more tickets resolved than created in last 3 full months"
    else:
        headline = "Last 3 full months: created and resolved within 10 tickets of each other"

    entry = report.get("_current_slide") or {}
    configured_title = (entry.get("title") or "").strip()
    volume_title = configured_title if configured_title else f"{project} — Volume analysis"
    title_has_project = _support_title_includes_project(volume_title, project)

    _slide(reqs, sid, idx)
    _bg(reqs, sid, bg)
    _slide_title(reqs, sid, volume_title)

    _box(reqs, f"{sid}_headline", sid, MARGIN, BODY_Y, CONTENT_W, 34, headline)
    _style(reqs, f"{sid}_headline", 0, len(headline), bold=True, size=16, color=NAVY, font=FONT)

    legend_y = BODY_Y + 40
    _rect(reqs, f"{sid}_lg_created", sid, MARGIN, legend_y + 4, 20, 4, NAVY)
    created_label = "Created"
    _box(reqs, f"{sid}_lg_created_t", sid, MARGIN + 28, legend_y, 64, 14, created_label)
    _style(reqs, f"{sid}_lg_created_t", 0, len(created_label), bold=True, size=CHART_LEGEND_PT, color=NAVY, font=FONT)
    created_resolved = {"red": 0.90, "green": 0.40, "blue": 0.00}
    _rect(reqs, f"{sid}_lg_resolved", sid, MARGIN + 100, legend_y + 4, 20, 4, created_resolved)
    resolved_label = "Resolved"
    _box(reqs, f"{sid}_lg_resolved_t", sid, MARGIN + 128, legend_y, 64, 14, resolved_label)
    _style(reqs, f"{sid}_lg_resolved_t", 0, len(resolved_label), bold=True, size=CHART_LEGEND_PT, color=NAVY, font=FONT)

    from .charts import embed_chart

    top_y = legend_y + 16
    top_gap = 16
    top_chart_w = (CONTENT_W - top_gap) // 2
    top_chart_h = 100
    left_x = MARGIN
    right_x = MARGIN + top_chart_w + top_gap

    all_header = "All tickets" if title_has_project else f"All {project} tickets"
    _box(reqs, f"{sid}_all_h", sid, left_x, top_y, top_chart_w, 14, all_header)
    _style(reqs, f"{sid}_all_h", 0, len(all_header), bold=True, size=10, color=NAVY, font=FONT)
    top_chart_y = top_y + 18
    ss_id, chart_id = charts.add_line_chart(
        title="",
        labels=[month.get("label", "") for month in all_months],
        series={
            "Created": [month.get("created", 0) for month in all_months],
            "Resolved": [month.get("resolved", 0) for month in all_months],
        },
        series_colors=[NAVY, created_resolved],
        show_legend=False,
        axis_font_size=12,
        line_width=3,
        background=bg,
    )
    embed_chart(reqs, f"{sid}_all_chart", sid, ss_id, chart_id, left_x, top_chart_y, top_chart_w, top_chart_h, linked=False)

    escalated_header = "w/ jira_escalated" if title_has_project else f"{project} tickets with jira_escalated label"
    _box(reqs, f"{sid}_esc_h", sid, right_x, top_y, top_chart_w, 14, escalated_header)
    _style(reqs, f"{sid}_esc_h", 0, len(escalated_header), bold=True, size=10, color=NAVY, font=FONT)
    esc_chart_y = top_y + 18
    ss_id2, chart_id2 = charts.add_line_chart(
        title="",
        labels=[month.get("label", "") for month in escalated_months],
        series={
            "Created": [month.get("created", 0) for month in escalated_months],
            "Resolved": [month.get("resolved", 0) for month in escalated_months],
        },
        series_colors=[NAVY, created_resolved],
        show_legend=False,
        axis_font_size=12,
        line_width=3,
        background=bg,
    )
    embed_chart(reqs, f"{sid}_esc_chart", sid, ss_id2, chart_id2, right_x, esc_chart_y, top_chart_w, top_chart_h, linked=False)

    bottom_chart_w = 436
    bottom_chart_h = 100
    bottom_x = MARGIN + (CONTENT_W - bottom_chart_w) / 2
    bottom_y = top_chart_y + top_chart_h + 18
    non_header = "w/o jira_escalated" if title_has_project else f"{project} tickets excluding jira_escalated"
    _box(reqs, f"{sid}_non_h", sid, bottom_x, bottom_y, bottom_chart_w, 14, non_header)
    _style(reqs, f"{sid}_non_h", 0, len(non_header), bold=True, size=10, color=NAVY, font=FONT)
    non_chart_y = bottom_y + 18
    ss_id3, chart_id3 = charts.add_line_chart(
        title="",
        labels=[month.get("label", "") for month in non_escalated_months],
        series={
            "Created": [month.get("created", 0) for month in non_escalated_months],
            "Resolved": [month.get("resolved", 0) for month in non_escalated_months],
        },
        series_colors=[NAVY, created_resolved],
        show_legend=False,
        axis_font_size=12,
        line_width=3,
        background=bg,
    )
    embed_chart(reqs, f"{sid}_non_chart", sid, ss_id3, chart_id3, bottom_x, non_chart_y, bottom_chart_w, bottom_chart_h, linked=False)

    return idx + 1


def eng_help_volume_trends_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """HELP monthly created vs resolved trends for all, escalated, and non-escalated tickets."""
    jira = report.get("jira") or {}
    eng = report.get("eng_portfolio") or {}
    raw_trends = jira.get("help_ticket_volume_trends") or eng.get("help_ticket_trends")

    if raw_trends is None:
        try:
            from .jira_client import get_shared_jira_client

            raw_trends = get_shared_jira_client().get_help_ticket_volume_trends(
                report.get("customer")
            )
            eng["help_ticket_trends"] = raw_trends
            report.setdefault("eng_portfolio", eng)
            logger.debug("eng_help_volume_trends: fetched HELP trends on demand (no eng_portfolio)")
        except Exception as exc:
            logger.warning("eng_help_volume_trends: on-demand HELP trends fetch failed: %s", exc)
            raw_trends = {"error": str(exc)}

    trends = raw_trends if isinstance(raw_trends, dict) else {}
    error = trends.get("error")
    jql_block = trends.get("jql_queries") if isinstance(trends.get("jql_queries"), list) else []
    report["eng_help_volume_jql_trace"] = {"jql_queries": jql_block}
    if error:
        return _missing_data_slide(reqs, sid, report, idx, f"HELP ticket volume trends — Jira error: {error}")
    return _render_project_volume_trends(reqs, sid, report, idx, trends=trends, project="HELP", bg=_project_slide_bg("HELP"))


def customer_project_volume_trends_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """CUSTOMER project monthly created vs resolved trends."""
    jira = report.get("jira") or {}
    trends = jira.get("customer_project_volume_trends") or {}
    jql = trends.get("jql_queries") if isinstance(trends, dict) and isinstance(trends.get("jql_queries"), list) else []
    report["customer_project_volume_jql_trace"] = {"jql_queries": jql}
    if not isinstance(trends, dict):
        return _missing_data_slide(reqs, sid, report, idx, "CUSTOMER volume trends (not in report)")
    if trends.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"CUSTOMER ticket volume trends — Jira error: {trends.get('error')}")
    return _render_project_volume_trends(reqs, sid, report, idx, trends=trends, project="CUSTOMER", bg=_project_slide_bg("CUSTOMER"))


def lean_project_volume_trends_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """LEAN project monthly created vs resolved trends."""
    jira = report.get("jira") or {}
    trends = jira.get("lean_project_volume_trends") or {}
    jql = trends.get("jql_queries") if isinstance(trends, dict) and isinstance(trends.get("jql_queries"), list) else []
    report["lean_project_volume_jql_trace"] = {"jql_queries": jql}
    if not isinstance(trends, dict):
        return _missing_data_slide(reqs, sid, report, idx, "LEAN volume trends (not in report)")
    if trends.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"LEAN ticket volume trends — Jira error: {trends.get('error')}")
    return _render_project_volume_trends(reqs, sid, report, idx, trends=trends, project="LEAN", bg=_project_slide_bg("LEAN"))
