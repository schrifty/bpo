"""Engineering portfolio deck slide helpers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .config import logger
from .slide_primitives import background as _bg, missing_data_slide as _missing_data_slide, slide_title as _slide_title, style as _style
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


GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
RED = {"red": 0.85, "green": 0.15, "blue": 0.15}


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
    sprint_name = sprint.get("name", "")
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
    sprint_name = sprint.get("name", "Current Sprint")
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
        _style(reqs, f"{sid}_tln{row_index}", 0, len(label), size=8, color=NAVY, font=FONT)

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
        _style(reqs, f"{sid}_tcnt{row_index}", 0, len(counts), size=8, color=RED if bugs_n else GRAY, font=FONT)
        left_y += row_h

    charts = report.get("_charts")

    right_y = body_top
    if by_type:
        _box(reqs, f"{sid}_typ_h", sid, right_x, right_y, right_w, 14, "Type Mix")
        _style(reqs, f"{sid}_typ_h", 0, 8, bold=True, size=10, color=NAVY, font=FONT)
        right_y += 16
        if charts:
            from .charts import embed_chart

            type_items = list(by_type.items())[:6]
            ss_id, chart_id = charts.add_bar_chart(
                title="Type Mix",
                labels=[ticket_type for ticket_type, _ in type_items],
                series={"Open tickets": [count for _, count in type_items]},
                horizontal=False,
            )
            embed_chart(reqs, f"{sid}_type_mix", sid, ss_id, chart_id, right_x, right_y, right_w, 120, linked=False)
            right_y += 126

    by_assignee = eng.get("by_assignee", {})
    top_assignees = sorted(by_assignee.items(), key=lambda item: -item[1])[:7]
    if top_assignees:
        _box(reqs, f"{sid}_ass_h", sid, right_x, right_y, right_w, 14, "WIP by Engineer")
        _style(reqs, f"{sid}_ass_h", 0, 15, bold=True, size=10, color=NAVY, font=FONT)
        right_y += 16
        if charts:
            from .charts import embed_chart

            ss_id, chart_id = charts.add_bar_chart(
                title="WIP by Engineer",
                labels=[(name.split()[0] if name else "Unassigned") for name, _ in top_assignees],
                series={"Open tickets": [count for _, count in top_assignees]},
                horizontal=False,
            )
            embed_chart(reqs, f"{sid}_wip_eng", sid, ss_id, chart_id, right_x, right_y, right_w, 120, linked=False)
            right_y += 126

    insights = (eng.get("insights") or {}).get("sprint_snapshot", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


def eng_bug_health_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Bug health: open bugs by priority, blocker/critical callout, trend."""
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
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 18, bar)
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

    body_top = BODY_Y + 22
    col_gap = 20
    left_w = (CONTENT_W - col_gap) * 2 // 3
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    left_y = body_top
    _box(reqs, f"{sid}_bl_h", sid, left_x, left_y, left_w, 16, "Open Bugs")
    _style(reqs, f"{sid}_bl_h", 0, 9, bold=True, size=11, color=NAVY, font=FONT)
    left_y += 18

    prio_color = {
        "Blocker": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "Critical": {"red": 0.9, "green": 0.4, "blue": 0.0},
        "Major": NAVY,
        "Minor": GRAY,
    }
    ticket_h = 34
    for bug_index, bug in enumerate(open_bugs[:12]):
        if left_y + ticket_h > BODY_BOTTOM - 72:
            break
        key = bug["key"]
        priority = bug["priority"]
        prio_short = priority.split(":")[0] if ":" in priority else priority
        assignee = bug.get("assignee") or ""
        first_name = assignee.split()[0] if assignee else "—"
        raw_summary = bug["summary"]
        summary = raw_summary[:48] + "…" if len(raw_summary) > 48 else raw_summary

        key_line = f"{key}  [{prio_short}]  {first_name}"
        link = f"{jira_base}/browse/{key}" if jira_base else None
        _box(reqs, f"{sid}_bk{bug_index}", sid, left_x, left_y, left_w, 16, key_line)
        _style(
            reqs,
            f"{sid}_bk{bug_index}",
            0,
            len(key),
            bold=True,
            size=8,
            color=prio_color.get(prio_short, RED),
            font=MONO,
            link=link,
        )
        _style(reqs, f"{sid}_bk{bug_index}", len(key), len(key_line), size=8, color=GRAY, font=FONT)
        left_y += 16

        _box(reqs, f"{sid}_bs{bug_index}", sid, left_x + 8, left_y, left_w - 8, 16, summary)
        _style(reqs, f"{sid}_bs{bug_index}", 0, len(summary), size=8, color=NAVY, font=FONT)
        left_y += 18

    right_y = body_top
    by_priority: dict[str, int] = {}
    for bug in open_bugs:
        priority = bug["priority"]
        short = priority.split(":")[0] if ":" in priority else priority
        by_priority[short] = by_priority.get(short, 0) + 1

    if by_priority:
        _box(reqs, f"{sid}_ph", sid, right_x, right_y, right_w, 16, "By Priority")
        _style(reqs, f"{sid}_ph", 0, 11, bold=True, size=11, color=NAVY, font=FONT)
        right_y += 18
        prio_order = ["Blocker", "Critical", "Major", "Minor"]
        for prio_index, (priority, count) in enumerate(
            sorted(by_priority.items(), key=lambda item: prio_order.index(item[0]) if item[0] in prio_order else 99)
        ):
            line = f"{count:>4}  {priority}"
            _box(reqs, f"{sid}_pp{prio_index}", sid, right_x, right_y, right_w, 13, line)
            color = prio_color.get(priority, NAVY)
            _style(reqs, f"{sid}_pp{prio_index}", 0, len(f"{count:>4}"), bold=True, size=10, color=color, font=FONT)
            _style(reqs, f"{sid}_pp{prio_index}", len(f"{count:>4}"), len(line), size=10, color=NAVY, font=FONT)
            right_y += 14
        right_y += 10

    if blocker_crit:
        _box(reqs, f"{sid}_bch", sid, right_x, right_y, right_w, 16, "Blockers & Criticals")
        _style(reqs, f"{sid}_bch", 0, 20, bold=True, size=11, color=RED, font=FONT)
        right_y += 18
        for bug_index, bug in enumerate(blocker_crit[:6]):
            key = bug["key"]
            link = f"{jira_base}/browse/{key}" if jira_base else None
            raw_summary = bug["summary"]
            summary = raw_summary[:30] + "…" if len(raw_summary) > 30 else raw_summary
            line = f"{key}  {summary}"
            _box(reqs, f"{sid}_bc{bug_index}", sid, right_x, right_y, right_w, 16, line)
            _style(reqs, f"{sid}_bc{bug_index}", 0, len(key), bold=True, size=9, color=RED, font=MONO, link=link)
            _style(reqs, f"{sid}_bc{bug_index}", len(key), len(line), size=9, color=NAVY, font=FONT)
            right_y += 17

    insights = (eng.get("insights") or {}).get("bug_health", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


def eng_velocity_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Velocity & throughput: combo chart plus pipeline status."""
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

    left_y = body_top
    recent_weeks = throughput[-12:] if len(throughput) >= 12 else throughput
    charts = report.get("_charts")
    if recent_weeks and charts:
        try:
            from .charts import embed_chart

            ss_id, chart_id = charts.add_combo_chart(
                title="Weekly Throughput",
                labels=[week.get("label", "") for week in recent_weeks],
                bar_series={"Created": [week.get("created", 0) for week in recent_weeks]},
                line_series={"Closed": [week.get("resolved", 0) for week in recent_weeks]},
            )
            embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id, left_x, left_y, left_w, 170, linked=False)
            left_y += 176
        except Exception as exc:
            logger.warning("Throughput chart embed failed: %s", exc)

    if recent_weeks:
        left_y += 4
        header = "Week        Created  Closed"
        _box(reqs, f"{sid}_wt_h", sid, left_x, left_y, left_w, 14, header)
        _style(reqs, f"{sid}_wt_h", 0, len(header), bold=True, size=8, color=GRAY, font=MONO)
        left_y += 14
        for week in recent_weeks[-8:]:
            row = f"{week['label']:<12}  {week.get('created', 0):>5}    {week.get('resolved', 0):>4}"
            _box(reqs, f"{sid}_wr{week['week']}", sid, left_x, left_y, left_w, 12, row)
            _style(reqs, f"{sid}_wr{week['week']}", 0, len(row), size=8, color=NAVY, font=MONO)
            left_y += 12

    right_y = body_top
    _box(reqs, f"{sid}_qlh", sid, right_x, right_y, right_w, 16, "Quarterly Goal Tracking")
    _style(reqs, f"{sid}_qlh", 0, 24, bold=True, size=11, color=NAVY, font=FONT)
    right_y += 20

    by_status = eng.get("by_status") or {}
    status_items = sorted(by_status.items(), key=lambda item: -item[1])

    _box(reqs, f"{sid}_sbh", sid, right_x, right_y, right_w, 14, "Pipeline Status")
    _style(reqs, f"{sid}_sbh", 0, 15, bold=True, size=10, color=NAVY, font=FONT)
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

    insights = (eng.get("insights") or {}).get("velocity", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

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

    tickets_per_page = 3
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

        y = BODY_Y + 22
        for row_index, ticket in enumerate(page_tickets):
            key = ticket["key"]
            link = f"{jira_base}/browse/{key}" if jira_base else None
            raw_summary = ticket["summary"]
            summary = raw_summary[:87] + "…" if len(raw_summary) > 87 else raw_summary
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

            _box(reqs, f"{page_sid}_s{row_index}", page_sid, MARGIN + 8, y, CONTENT_W - 8, 36, summary)
            _style(reqs, f"{page_sid}_s{row_index}", 0, len(summary), size=9, color=NAVY, font=FONT)
            y += 36

            narrative = (ticket.get("narrative") or "").strip()
            if narrative and y + 40 <= BODY_BOTTOM:
                _box(reqs, f"{page_sid}_n{row_index}", page_sid, MARGIN + 8, y, CONTENT_W - 8, 40, narrative)
                _style(reqs, f"{page_sid}_n{row_index}", 0, len(narrative), size=8, color=GRAY, font=FONT)
                y += 42

            y += 4

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
    ticket_h = 96
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
        raw_summary = ticket["summary"]
        summary = raw_summary[:87] + "…" if len(raw_summary) > 87 else raw_summary
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

        _box(reqs, f"{sid}_s{row_index}", sid, MARGIN + 8, y, CONTENT_W - 8, 36, summary)
        _style(reqs, f"{sid}_s{row_index}", 0, len(summary), size=9, color=NAVY, font=FONT)
        y += 36

        narrative = (ticket.get("narrative") or "").strip()
        if narrative and y + 40 <= BODY_BOTTOM:
            _box(reqs, f"{sid}_n{row_index}", sid, MARGIN + 8, y, CONTENT_W - 8, 40, narrative)
            _style(reqs, f"{sid}_n{row_index}", 0, len(narrative), size=8, color=GRAY, font=FONT)
            y += 42

        y += 4

    return idx + 1
