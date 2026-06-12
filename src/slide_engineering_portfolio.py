"""Engineering portfolio deck slide helpers."""

from __future__ import annotations

import re
import urllib.parse
from datetime import date, datetime, timedelta
from typing import Any

from .config import logger
from .slide_primitives import (
    CHART_LEGEND_PT,
    background as _bg,
    clean_table as _clean_table,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    rect as _rect,
    slide_chart_legend as _slide_chart_legend,
    slide_title as _slide_title,
    style as _style,
    support_title_includes_project as _support_title_includes_project,
    table_cell_style as _table_cell_style,
    table_cell_text as _table_cell_text,
    table_column_widths as _table_column_widths,
)
from .eng_sprint_velocity import build_sprint_velocity_series
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slide_utils import (
    max_chars_one_line_for_table_col,
    slide_size as _sz,
    slide_transform as _tf,
)
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
from .charts import BRAND_SERIES_COLORS, CHART_AXIS_PT

GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
RED = {"red": 0.85, "green": 0.15, "blue": 0.15}
AMBER = {"red": 0.85, "green": 0.6, "blue": 0.13}

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

    entry = report.get("_current_slide") or {}
    title = (entry.get("cover_title") or "").strip() or "Engineering Review"
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


def _format_scorecard_days(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.1f}d"
    except (TypeError, ValueError):
        return "—"


# Scorecard table geometry. Column widths sum to CONTENT_W so the table justifies
# edge to edge; numeric columns are right-aligned for clean decimal/percent stacking.
# Delivery %/Story-pts columns were dropped: neither board commitment is trustworthy
# (LEAN punts incomplete work; the CUSTOMER board hoards a standing backlog in-sprint),
# so the honest, cross-team-comparable columns are throughput (Closed) and lead time.
_SCORECARD_COL_WIDTHS: tuple[float, ...] = (300.0, 150.0, 90.0, 84.0)
_SCORECARD_BODY_PT = 10.0
_SCORECARD_HEADER_PT = 9.0
_SCORECARD_ROW_H = 24.0


def eng_team_scorecard_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Per-team sprint delivery, story points, and cycle time on one native table."""
    eng = report.get("eng_portfolio") or {}
    scorecard = eng.get("team_scorecard") or {}
    teams = scorecard.get("teams") or []
    if not teams:
        detail = scorecard.get("error") or "Team scorecard (Jira boards 44, 36, 46)"
        return _missing_data_slide(reqs, sid, report, idx, detail)

    summary = scorecard.get("summary") or {}
    # Throughput (issues closed in the sprint) and lead time are the honest,
    # cross-team-comparable metrics. Delivery % was removed because no board's
    # "commitment" is trustworthy: LEAN punts incomplete work (reads ~100%) and the
    # CUSTOMER board parks a standing backlog in each sprint (reads ~23%).
    total_throughput = summary.get("total_throughput")
    avg_lead = summary.get("average_median_lead_days")

    # Lead with throughput — the one delivery number that is meaningful org-wide.
    if total_throughput:
        title = f"Team Scorecard — {total_throughput} Issues Closed Last Sprint"
    else:
        title = "Development Team Scorecard"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    # Business context: define throughput and lead time; note the two operating models.
    context = (
        "Closed = issues resolved in the latest sprint (throughput). Lead time = median "
        "days from created to resolved. The six LEAN squads run continuous flow; the "
        "CUSTOMER board runs weekly sprints. Sprint commit-vs-complete % is omitted — it "
        "is not comparable across these models."
    )
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 28, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=9.5, color=NAVY, font=FONT)

    # One-line portfolio rollup (replaces the old 3 KPI cards so every team row fits).
    summary_bits: list[str] = []
    if total_throughput:
        summary_bits.append(f"{total_throughput} closed last sprint")
    if avg_lead is not None:
        summary_bits.append(f"avg lead {_format_scorecard_days(avg_lead)}")
    # ── Native team table ────────────────────────────────────────────────────
    table_top = BODY_Y + 50
    col_widths = list(_SCORECARD_COL_WIDTHS)
    headers = ["Team", "Latest sprint", "Closed", "Lead time"]
    # Left-align text columns; right-align the two numeric columns.
    aligns = ["START", "START", "END", "END"]

    max_rows = max(1, int((_ENG_CONTENT_BOTTOM - table_top) // _SCORECARD_ROW_H) - 1)
    display_teams = teams[:max_rows]
    num_rows = 1 + len(display_teams)

    # Flag any teams that didn't fit so the rollup line never hides dropped rows.
    dropped = len(teams) - len(display_teams)
    if dropped > 0:
        summary_bits.append(f"+{dropped} more team{'s' if dropped != 1 else ''} not shown")
    summary_line = "   ·   ".join(summary_bits)
    if summary_line:
        _box(reqs, f"{sid}_sum", sid, MARGIN, BODY_Y + 30, CONTENT_W, 16, summary_line)
        _style(reqs, f"{sid}_sum", 0, len(summary_line), bold=True, size=10.5, color=NAVY, font=FONT)
    table_id = f"{sid}_tbl"
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * _SCORECARD_ROW_H),
                "transform": _tf(MARGIN, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })
    _clean_table(reqs, table_id, num_rows, len(headers))
    _table_column_widths(reqs, table_id, col_widths)

    for col_index, header in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, col_index, header)
        _table_cell_style(
            reqs,
            table_id,
            0,
            col_index,
            len(header),
            bold=True,
            color=GRAY,
            size=_SCORECARD_HEADER_PT,
            font=FONT,
            align=aligns[col_index],
        )

    team_chars = max_chars_one_line_for_table_col(col_widths[0], _SCORECARD_BODY_PT)
    sprint_chars = max_chars_one_line_for_table_col(col_widths[1], _SCORECARD_BODY_PT)
    for row_index, team in enumerate(display_teams, start=1):
        team_name = _truncate_one_line(str(team.get("team") or ""), team_chars)
        sprint_name = _format_sprint_name_for_display(str(team.get("sprint_name") or ""))
        sprint_name = _truncate_one_line(sprint_name, sprint_chars)
        cycle = _format_scorecard_days(team.get("median_lead_days"))
        throughput = team.get("throughput")
        if throughput is None:
            throughput = team.get("delivered")
        closed_text = "—" if throughput is None else f"{int(throughput)}"

        row_cells = [
            (team_name, NAVY, FONT, True),
            (sprint_name, GRAY, FONT, False),
            (closed_text, NAVY, MONO, True),
            (cycle, NAVY, MONO, False),
        ]
        for col_index, (text, color, font, bold) in enumerate(row_cells):
            _table_cell_text(reqs, table_id, row_index, col_index, text)
            _table_cell_style(
                reqs,
                table_id,
                row_index,
                col_index,
                len(text),
                bold=bold,
                color=color,
                size=_SCORECARD_BODY_PT,
                font=font,
                align=aligns[col_index],
            )

    _eng_takeaway_bar(reqs, sid, report, "team_scorecard")
    return idx + 1


# Team roster bar track (unfilled portion) — a light neutral rail behind each bar.
_ROSTER_TRACK_FILL = {"red": 0.90, "green": 0.93, "blue": 0.97}


def eng_team_roster_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Roster of engineering teams: headcount bar + member names (+ lead when known)."""
    eng = report.get("eng_portfolio") or {}
    roster = eng.get("team_roster") or {}
    teams = roster.get("teams") or []
    if not teams:
        detail = roster.get("error") or "Team roster (Jira LEAN 'Agile Team' field)"
        return _missing_data_slide(reqs, sid, report, idx, detail)

    total = int(roster.get("total_engineers") or sum(int(t.get("headcount") or 0) for t in teams))
    window_days = int(roster.get("window_days") or 90)

    title = f"Engineering Teams — {total} Engineers Across {len(teams)} Squads"
    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = (
        f"Each engineer is shown on the team where they did most of their work over the last "
        f"{window_days} days · bar length = team headcount · bold name = team lead (where known)."
    )
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=9.5, color=GRAY, font=FONT)

    top = BODY_Y + 24
    n = len(teams)
    row_h = min(46.0, max(30.0, (BODY_BOTTOM - top) / n))
    max_hc = max((int(t.get("headcount") or 0) for t in teams), default=1) or 1

    name_w = 184.0
    bar_x = MARGIN + name_w + 8
    count_w = 28.0
    max_bar = CONTENT_W - name_w - 8 - count_w - 6
    mem_indent = 12.0
    mem_w = CONTENT_W - mem_indent
    mem_chars = max_chars_one_line_for_table_col(mem_w, 8.5)

    for i, team in enumerate(teams):
        y0 = top + i * row_h
        name = str(team.get("team") or "")
        hc = int(team.get("headcount") or 0)
        lead = str(team.get("lead") or "").strip()
        members = [str(m) for m in (team.get("members") or [])]

        # Team name (bold).
        _box(reqs, f"{sid}_tn{i}", sid, MARGIN, y0, name_w, 16, name)
        _style(reqs, f"{sid}_tn{i}", 0, len(name), bold=True, size=11, color=NAVY, font=FONT)

        # Headcount bar: light track + blue fill, with the count just past the fill.
        bar_w = max(4.0, hc / max_hc * max_bar)
        _rect(reqs, f"{sid}_bt{i}", sid, bar_x, y0 + 2, max_bar, 11, _ROSTER_TRACK_FILL)
        _rect(reqs, f"{sid}_bf{i}", sid, bar_x, y0 + 2, bar_w, 11, BLUE)
        count_txt = str(hc)
        _box(reqs, f"{sid}_bc{i}", sid, bar_x + bar_w + 6, y0, count_w, 16, count_txt)
        _style(reqs, f"{sid}_bc{i}", 0, len(count_txt), bold=True, size=10.5, color=NAVY, font=MONO)

        # Members line, with optional bold "Lead: <name>" prefix.
        prefix = f"Lead: {lead} — " if lead else ""
        line = _truncate_one_line(prefix + ", ".join(members), mem_chars)
        if line:
            _box(reqs, f"{sid}_mm{i}", sid, MARGIN + mem_indent, y0 + 17, mem_w, 14, line)
            _style(reqs, f"{sid}_mm{i}", 0, len(line), size=8.5, color=GRAY, font=FONT)
            if prefix:
                dash = line.find("—")
                bold_end = dash if dash != -1 else min(len(line), len(prefix))
                if bold_end > 0:
                    _style(reqs, f"{sid}_mm{i}", 0, bold_end, bold=True, size=8.5, color=NAVY, font=FONT)

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


# ── Reorganized engineering team slides (current sprint / backlog / capacity) ──
# Shared geometry: bottom-anchored scope footer per SLIDE_DESIGN_STANDARDS.
_ENG_FOOTER_H = 22.0
_ENG_FOOTER_Y = float(SLIDE_H) - 10.0 - _ENG_FOOTER_H

# Bottom "what this means" takeaway band: a divider, a small label, and one
# LLM-written implication sentence. Replaces the old illegible gray scope footer on
# the slide face; data slides reserve space above it via ``_ENG_CONTENT_BOTTOM``.
_ENG_TAKEAWAY_H = 40.0
_ENG_TAKEAWAY_Y = float(SLIDE_H) - 4.0 - _ENG_TAKEAWAY_H
_ENG_CONTENT_BOTTOM = _ENG_TAKEAWAY_Y - 8.0
_ENG_TAKEAWAY_LABEL = "WHAT THIS MEANS"
_ENG_DIVIDER_FILL = {"red": 0.84, "green": 0.89, "blue": 0.96}


def _eng_scope_footer(reqs: list[dict[str, Any]], sid: str, text: str) -> None:
    _box(reqs, f"{sid}_scope", sid, MARGIN, _ENG_FOOTER_Y, CONTENT_W, _ENG_FOOTER_H, text)
    _style(reqs, f"{sid}_scope", 0, len(text), size=8, color=GRAY, font=FONT)


def _eng_takeaway_bar(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], key: str) -> None:
    """Render the bottom 'what this means' band for ``key`` from ``eng.takeaways``.

    No-ops when the takeaway is missing/empty so a slide never shows an orphan label.
    The sentence is kept short by the generator and wraps within a two-line box, so it
    is never truncated mid-word the way the old multi-bullet insights were.
    """
    eng = report.get("eng_portfolio") or {}
    text = ((eng.get("takeaways") or {}).get(key) or "").strip()
    if not text:
        return
    _rect(reqs, f"{sid}_tkdiv", sid, MARGIN, _ENG_TAKEAWAY_Y - 3.0, CONTENT_W, 1.2, _ENG_DIVIDER_FILL)
    _box(reqs, f"{sid}_tklbl", sid, MARGIN, _ENG_TAKEAWAY_Y, CONTENT_W, 11, _ENG_TAKEAWAY_LABEL)
    _style(reqs, f"{sid}_tklbl", 0, len(_ENG_TAKEAWAY_LABEL), bold=True, size=7.5, color=BLUE, font=FONT)
    _box(reqs, f"{sid}_tktxt", sid, MARGIN, _ENG_TAKEAWAY_Y + 12, CONTENT_W, 28, text)
    _style(reqs, f"{sid}_tktxt", 0, len(text), size=10, color=NAVY, font=FONT)


def _eng_kpi_row(
    reqs: list[dict[str, Any]],
    sid: str,
    cards: list[tuple[str, str]],
    *,
    y: float,
    h: float = 54.0,
    gap: float = 16.0,
    accent: dict[str, float] = BLUE,
) -> float:
    """Render a justified row of KPI cards across CONTENT_W. Returns y below the row."""
    n = max(1, len(cards))
    card_w = (CONTENT_W - (n - 1) * gap) / n
    for i, (label, value) in enumerate(cards):
        _kpi_metric_card(
            reqs, f"{sid}_kpi{i}", sid,
            MARGIN + i * (card_w + gap), y, card_w, h,
            label, value, accent=accent,
        )
    return y + h


def _fmt_days(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.0f} d"
    except (TypeError, ValueError):
        return "—"


def _eng_simple_bar_chart(
    reqs: list[dict[str, Any]],
    sid: str,
    charts: Any,
    *,
    oid: str,
    header: str,
    labels: list[str],
    values: list[float | int],
    x: float,
    y: float,
    w: float,
    chart_h: float,
    series_name: str = "Tickets",
    horizontal: bool = False,
) -> None:
    """Section header + embedded single-series bar chart."""
    _box(reqs, f"{sid}_{oid}_h", sid, x, y, w, 14, header)
    _style(reqs, f"{sid}_{oid}_h", 0, len(header), bold=True, size=10, color=NAVY, font=FONT)
    if not (labels and charts):
        empty = "No data"
        _box(reqs, f"{sid}_{oid}_e", sid, x, y + 20, w, 14, empty)
        _style(reqs, f"{sid}_{oid}_e", 0, len(empty), size=9, color=GRAY, font=FONT)
        return
    try:
        from .charts import embed_chart

        ss_id, chart_id = charts.add_bar_chart(
            title=header,
            labels=labels,
            series={series_name: list(values)},
            horizontal=horizontal,
            show_title=False,
        )
        embed_chart(reqs, f"{sid}_{oid}", sid, ss_id, chart_id, x, y + 18, w, chart_h, linked=False)
    except Exception as exc:
        logger.warning("Eng bar chart %s failed: %s", oid, exc)


def eng_current_sprint_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Current sprint health: commitment KPIs + what the team is working on, by theme."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    sprint = eng.get("sprint") or {}
    sprint_name = _format_sprint_name_for_display(str(sprint.get("name", "") or "Current Sprint"))

    in_flight = int(eng.get("in_flight_count", 0) or 0)
    closed = int(eng.get("closed_count", 0) or 0)
    by_status = eng.get("by_status", {}) or {}
    active = int(by_status.get("In Progress", 0) or 0) + int(by_status.get("In Review", 0) or 0)
    by_type = eng.get("by_type", {}) or {}
    bugs = int(by_type.get("Bug", 0) or 0)

    if bugs and in_flight:
        title = f"{sprint_name} — {active} Active, {bugs} Bug{'s' if bugs != 1 else ''} In Flight"
    elif in_flight:
        title = f"{sprint_name} — {active} of {in_flight} Items Active"
    else:
        title = f"{sprint_name} — No Open Work In Sprint"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = "What the LEAN engineering team is working on in the active sprint."
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=11, color=NAVY, font=FONT)

    legend = "Per theme: open items · active (in progress/review) · bugs (B, shown in red)."
    _box(reqs, f"{sid}_lgd", sid, MARGIN, BODY_Y + 13, CONTENT_W, 12, legend)
    _style(reqs, f"{sid}_lgd", 0, len(legend), size=8.5, color=GRAY, font=FONT)

    card_y = BODY_Y + 24
    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Open in sprint", str(in_flight)),
            ("Active (in progress/review)", str(active)),
            ("Bugs in flight", str(bugs)),
            ("Closed this period", str(closed)),
        ],
        y=card_y,
    )

    theme_top = cards_y + 18
    theme_bottom = _ENG_CONTENT_BOTTOM

    themes = [t for t in (eng.get("themes") or []) if int(t.get("total") or 0) > 0][:8]
    header = "Active work by theme"
    _box(reqs, f"{sid}_tht", sid, MARGIN, theme_top, CONTENT_W, 16, header)
    _style(reqs, f"{sid}_tht", 0, len(header), bold=True, size=11, color=NAVY, font=FONT)
    y = theme_top + 20

    if themes:
        max_total = max(int(t.get("total") or 0) for t in themes) or 1
        label_w = 150.0
        count_w = 70.0
        bar_x = MARGIN + label_w
        bar_max = CONTENT_W - label_w - count_w - 6
        avail_rows = max(1, int((theme_bottom - y) // 22))
        for ri, theme in enumerate(themes[:avail_rows]):
            total_n = int(theme.get("total") or 0)
            active_n = int(theme.get("in_progress") or 0)
            bugs_n = int(theme.get("bugs") or 0)
            name = _truncate_one_line(str(theme.get("theme") or "—"), 22)
            _box(reqs, f"{sid}_tl{ri}", sid, MARGIN, y, label_w, 18, name)
            _style(reqs, f"{sid}_tl{ri}", 0, len(name), size=10, color=NAVY, font=FONT)
            bar_w = max(4, int(total_n / max_total * bar_max))
            # Bar length encodes total volume; bug load is shown by the red bug count,
            # so keep every bar the same neutral blue (a red bar over-signals one bug).
            _box(reqs, f"{sid}_tb{ri}", sid, bar_x, y + 4, bar_w, 9, "")
            reqs.append({
                "updateShapeProperties": {
                    "objectId": f"{sid}_tb{ri}",
                    "shapeProperties": {
                        "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": BLUE}}},
                        "outline": {"outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}}, "weight": {"magnitude": 0.75, "unit": "PT"}},
                    },
                    "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
                }
            })
            # Color only the bug segment red so "red" unambiguously means bugs.
            base = f"{total_n}" + (f" · {active_n} act" if active_n else "")
            bug_seg = f" · {bugs_n}B" if bugs_n else ""
            counts = base + bug_seg
            _box(reqs, f"{sid}_tc{ri}", sid, bar_x + bar_w + 6, y, count_w + 40, 18, counts)
            _style(reqs, f"{sid}_tc{ri}", 0, len(counts), size=9, color=GRAY, font=FONT)
            if bug_seg:
                _style(reqs, f"{sid}_tc{ri}", len(base), len(counts), bold=True, size=9, color=RED, font=FONT)
            y += 22
    else:
        msg = "No themed work in flight (LEAN summaries need a [Theme] prefix)."
        _box(reqs, f"{sid}_tnone", sid, MARGIN, y, CONTENT_W, 16, msg)
        _style(reqs, f"{sid}_tnone", 0, len(msg), size=10, color=GRAY, font=FONT)

    _eng_takeaway_bar(reqs, sid, report, "current_sprint")
    return idx + 1


def eng_backlog_health_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Escalation backlog health: queue size, aging, and status composition for LEAN."""
    eng = report.get("eng_portfolio") or {}
    snapshot = (eng.get("project_snapshots") or {}).get("LEAN") or {}
    if not snapshot or (snapshot.get("error") and "open_count" not in snapshot):
        detail = snapshot.get("error") if snapshot else "LEAN project snapshot"
        return _missing_data_slide(reqs, sid, report, idx, f"Escalation backlog (LEAN): {detail or 'unavailable'}")

    open_count = int(snapshot.get("open_count") or 0)
    median_age = snapshot.get("median_open_age_days")
    avg_cycle = snapshot.get("avg_resolved_cycle_days")
    resolved_6mo = int(snapshot.get("resolved_in_6mo_count") or 0)
    resolved_capped = bool(snapshot.get("resolved_in_6mo_capped"))
    resolved_label = f"{resolved_6mo:,}+" if resolved_capped else f"{resolved_6mo:,}"
    over_90 = int(snapshot.get("open_over_90_count") or 0)
    by_status = snapshot.get("by_status_open") or {}
    age_buckets = snapshot.get("open_age_buckets") or {}

    if over_90 > 0 and median_age is not None:
        title = f"Escalation Backlog Aging — Median {median_age:.0f}d, {over_90} Over 90d"
    elif median_age is not None:
        title = f"Escalation Backlog Healthy — Median {median_age:.0f}d, None Over 90d"
    else:
        title = f"Escalation Backlog — {open_count} Open"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = "Health of the LEAN engineering escalation queue: how much is open and how old it is."
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=11, color=NAVY, font=FONT)

    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Open tickets", str(open_count)),
            ("Median open age", _fmt_days(median_age)),
            ("Avg resolve cycle", _fmt_days(avg_cycle)),
            ("Resolved (6 mo)", resolved_label),
        ],
        y=BODY_Y + 22,
    )

    charts = report.get("_charts")
    body_top = cards_y + 18
    col_gap = 24
    left_w = (CONTENT_W - col_gap) // 2
    right_w = CONTENT_W - left_w - col_gap
    right_x = MARGIN + left_w + col_gap
    chart_h = _ENG_CONTENT_BOTTOM - body_top - 20

    status_items = list(by_status.items())[:8]
    _eng_simple_bar_chart(
        reqs, sid, charts, oid="status",
        header="Open tickets by status",
        labels=[s for s, _ in status_items],
        values=[c for _, c in status_items],
        x=MARGIN, y=body_top, w=left_w, chart_h=chart_h,
        series_name="Open",
    )
    bucket_order = ["0-7d", "8-30d", "31-90d", "90d+"]
    bucket_labels = [b for b in bucket_order if b in age_buckets]
    _eng_simple_bar_chart(
        reqs, sid, charts, oid="aging",
        header="Open tickets by age",
        labels=bucket_labels,
        values=[int(age_buckets.get(b, 0)) for b in bucket_labels],
        x=right_x, y=body_top, w=right_w, chart_h=chart_h,
        series_name="Open",
    )

    _eng_takeaway_bar(reqs, sid, report, "backlog_health")
    return idx + 1


def eng_capacity_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Engineering capacity & load: open WIP now vs resolved throughput per engineer."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    by_assignee = eng.get("by_assignee", {}) or {}
    snapshot = (eng.get("project_snapshots") or {}).get("LEAN") or {}
    resolved_table = snapshot.get("assignee_resolved_table") or []
    resolved_by_name = {str(r.get("assignee") or ""): r for r in resolved_table}

    names = {n for n in by_assignee if n and n != "Unassigned"}
    names.update(n for n in resolved_by_name if n and n != "Unassigned")

    rows: list[dict[str, Any]] = []
    for name in names:
        wip = int(by_assignee.get(name, 0) or 0)
        r = resolved_by_name.get(name, {})
        rows.append({
            "name": name,
            "wip": wip,
            "r30": int(r.get("1m", 0) or 0),
            "r90": int(r.get("3m", 0) or 0),
        })
    rows.sort(key=lambda x: (-x["wip"], -x["r90"]))

    if not rows:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering capacity (no assignee data on LEAN board)")

    total_wip = sum(r["wip"] for r in rows)
    top3_wip = sum(r["wip"] for r in rows[:3])
    top3_share = int(round(top3_wip / total_wip * 100)) if total_wip else 0
    engineers_active = sum(1 for r in rows if r["wip"] > 0)

    if total_wip and top3_share >= 60:
        title = f"Capacity Concentrated — Top 3 Engineers Hold {top3_share}% of Assigned WIP"
    elif total_wip:
        title = f"Engineering Load — {total_wip} Assigned In-Flight Items Across {engineers_active} Engineers"
    else:
        title = "Engineering Capacity — No Assigned WIP On LEAN Board"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    # Reconcile with the Teams slide: this counts anyone with assigned LEAN work, which
    # is broader than the engineers mapped to squads (cross-team helpers, leads, etc.).
    roster = eng.get("team_roster") or {}
    roster_total = roster.get("total_engineers")
    squad_n = len(roster.get("teams") or [])
    reconcile = ""
    if roster_total and squad_n and int(roster_total) != engineers_active:
        reconcile = (
            f" Counts anyone with assigned LEAN work ({engineers_active}) — broader than the "
            f"{int(roster_total)} engineers mapped to the {squad_n} squads on the Teams slide."
        )
    context = (
        "LEAN Engineering board: per-engineer in-flight WIP (assigned, active sprint statuses) "
        "versus recent throughput — not the full escalation backlog." + reconcile
    )
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 26, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=9.5, color=NAVY, font=FONT)

    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Engineers with WIP", str(engineers_active)),
            ("Assigned in-flight WIP", str(total_wip)),
            ("Top 3 share of WIP", f"{top3_share}%" if total_wip else "—"),
        ],
        y=BODY_Y + 22,
    )

    # Native table: Engineer | WIP now | Resolved 30d | Resolved 90d.
    table_top = cards_y + 18
    col_widths = [288.0, 112.0, 112.0, 112.0]
    headers = ["Engineer", "WIP now", "Resolved 30d", "Resolved 90d"]
    aligns = ["START", "END", "END", "END"]
    max_rows = max(1, int((_ENG_CONTENT_BOTTOM - table_top) // 24) - 1)
    display = rows[:max_rows]
    num_rows = 1 + len(display)
    table_id = f"{sid}_tbl"
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * 24.0),
                "transform": _tf(MARGIN, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })
    _clean_table(reqs, table_id, num_rows, len(headers))
    _table_column_widths(reqs, table_id, col_widths)
    for ci, h in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, ci, h)
        _table_cell_style(reqs, table_id, 0, ci, len(h), bold=True, color=GRAY, size=9, font=FONT, align=aligns[ci])
    name_chars = max_chars_one_line_for_table_col(col_widths[0], 10.0)
    for ri, row in enumerate(display, start=1):
        cells = [
            (_truncate_one_line(row["name"], name_chars), NAVY, FONT),
            (str(row["wip"]), BLUE if row["wip"] else GRAY, MONO),
            (str(row["r30"]), NAVY, MONO),
            (str(row["r90"]), NAVY, MONO),
        ]
        for ci, (text, color, font) in enumerate(cells):
            _table_cell_text(reqs, table_id, ri, ci, text)
            _table_cell_style(
                reqs, table_id, ri, ci, len(text),
                bold=(ci == 0), color=color, size=10, font=font, align=aligns[ci],
            )

    _eng_takeaway_bar(reqs, sid, report, "capacity")
    return idx + 1


# ── VP-level synthesis slides (executive summary / flow / planned-vs-unplanned) ──

def _eng_callout_column(
    reqs: list[dict[str, Any]],
    sid: str,
    oid: str,
    x: float,
    y: float,
    w: float,
    heading: str,
    items: list[tuple[str, dict[str, float]]],
    *,
    max_items: int = 6,
    row_h: float = 30.0,
) -> float:
    """Titled column of color-bulleted, prescriptive lines. Returns y below the list."""
    _box(reqs, f"{sid}_{oid}_h", sid, x, y, w, 16, heading)
    _style(reqs, f"{sid}_{oid}_h", 0, len(heading), bold=True, size=12, color=NAVY, font=FONT)
    cy = y + 22
    if not items:
        empty = "Nothing flagged."
        _box(reqs, f"{sid}_{oid}_e", sid, x, cy, w, 16, empty)
        _style(reqs, f"{sid}_{oid}_e", 0, len(empty), size=10, color=GRAY, font=FONT)
        return cy + 20
    for i, (text, color) in enumerate(items[:max_items]):
        bullet = f"\u25cf  {text}"
        _box(reqs, f"{sid}_{oid}_b{i}", sid, x, cy, w, row_h, bullet)
        _style(reqs, f"{sid}_{oid}_b{i}", 0, 1, bold=True, size=10, color=color, font=FONT)
        _style(reqs, f"{sid}_{oid}_b{i}", 1, len(bullet), size=10, color=NAVY, font=FONT)
        cy += row_h
    return cy


def _trend_arrow(delta: float | None, *, good_is_down: bool = False) -> tuple[str, dict[str, float]]:
    """Return (glyph, color) for a delta. ``good_is_down`` flips the semantic color."""
    if delta is None or abs(delta) < 1e-9:
        return "\u2192", GRAY
    rising = delta > 0
    glyph = "\u25b2" if rising else "\u25bc"
    favorable = (not rising) if good_is_down else rising
    return glyph, (GREEN if favorable else RED)


def eng_exec_summary_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Bottom-line-up-front: headline KPIs, a risk watch-list, and decisions needed."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    sprint = eng.get("sprint") or {}
    sprint_name = _format_sprint_name_for_display(str(sprint.get("name", "") or "Current Sprint"))

    summary = (eng.get("team_scorecard") or {}).get("summary") or {}
    sprint_throughput = summary.get("total_throughput")
    lean = (eng.get("project_snapshots") or {}).get("LEAN") or {}
    open_esc = int(lean.get("open_count") or 0)
    over_90 = int(lean.get("open_over_90_count") or 0)
    oldest = lean.get("oldest_open_age_days")
    flow = eng.get("flow") or {}
    status_flow = flow.get("status_flow") or {}
    flow_changelog_on = (
        status_flow.get("source") == "changelog" and int(status_flow.get("enriched_count") or 0) > 0
    )
    stale5 = int(flow.get("stale_gt5") or 0)
    stale10 = int(flow.get("stale_gt10") or 0)
    blocked = int(flow.get("blocked_count") or status_flow.get("blocked_count") or 0)
    cycle_delta = flow.get("cycle_delta_days")
    split = eng.get("work_split") or {}
    reactive_wip_pct = int(split.get("reactive_wip_pct") or 0)
    blockers = eng.get("blocker_critical") or []
    by_assignee = eng.get("by_assignee") or {}

    series = build_sprint_velocity_series(eng.get("sprint_velocity"))
    sp_total = series.get("sp_total") or []
    vel_now = float(sp_total[-1]) if sp_total else None
    # Trend vs a multi-sprint baseline (not just the prior sprint), with the same ±5%
    # band the velocity slide uses — otherwise a one-sprint bounce off a low point reads
    # as "up" here while the velocity slide says "down".
    _vel_prior = [float(v) for v in sp_total[:-1] if v]
    vel_baseline = (sum(_vel_prior) / len(_vel_prior)) if _vel_prior else None
    vel_delta = None
    if vel_now is not None and vel_baseline:
        if vel_now >= vel_baseline * 1.05 or vel_now <= vel_baseline * 0.95:
            vel_delta = vel_now - vel_baseline
    vel_arrow, _ = _trend_arrow(vel_delta)

    # Capacity concentration (key-person risk).
    wip_values = sorted((int(v) for v in by_assignee.values()), reverse=True)
    total_wip = sum(wip_values)
    top3_share = int(round(sum(wip_values[:3]) / total_wip * 100)) if total_wip else 0

    # ── Build risk watch-list and paired actions (deterministic, threshold-driven) ──
    risks: list[tuple[str, dict[str, float]]] = []
    actions: list[tuple[str, dict[str, float]]] = []
    if blockers:
        n = len(blockers)
        risks.append((f"{n} blocker/critical item{'s' if n != 1 else ''} in flight", RED))
        actions.append((f"Assign owners and clear the {n} blocker/critical item{'s' if n != 1 else ''} this sprint", RED))
    if blocked:
        risks.append((f"{blocked} active item{'s' if blocked != 1 else ''} flagged blocked in Jira", RED))
        actions.append((f"Clear the {blocked} flagged blocker{'s' if blocked != 1 else ''} or escalate ownership", RED))
    # Sprint hygiene, NOT a delivery miss: the CUSTOMER/Data Integration boards park a
    # large standing backlog inside each weekly sprint, so most of it carries over. This
    # is a scoping problem to fix, not "commitments slipping" (see Team Scorecard).
    _committed = summary.get("total_committed")
    _delivered = summary.get("total_delivered")
    carryover = (
        int(_committed) - int(_delivered)
        if _committed is not None and _delivered is not None
        else None
    )
    if carryover is not None and carryover >= 25:
        risks.append((f"CUSTOMER/Data Integration sprints carry ~{carryover} unfinished issues week to week — sprint scope inflated", AMBER))
        actions.append((f"Trim CUSTOMER/Data Integration sprint scope to a realistic weekly commitment (clear the ~{carryover}-issue standing backlog)", AMBER))
    if over_90:
        oldest_txt = f", oldest {float(oldest):.0f}d" if oldest is not None else ""
        risks.append((f"{over_90} escalation{'s' if over_90 != 1 else ''} open >90 days{oldest_txt}", AMBER))
        actions.append((f"Run a backlog scrub on the {over_90} escalation{'s' if over_90 != 1 else ''} aging past 90 days", AMBER))
    stall_n = stale10 if flow_changelog_on else stale5
    if stall_n:
        stall_word = "stalled >10 days in stage" if flow_changelog_on else "idle >5 days"
        risks.append((f"{stall_n} active item{'s' if stall_n != 1 else ''} {stall_word} — flow stalling", AMBER))
        actions.append((f"Unblock or re-assign the {stall_n} stalled item{'s' if stall_n != 1 else ''}", AMBER))
    if reactive_wip_pct >= 40:
        risks.append((f"{reactive_wip_pct}% of WIP is unplanned/reactive work", AMBER))
        actions.append((f"Protect roadmap capacity — {reactive_wip_pct}% is going to reactive work", AMBER))
    if total_wip and top3_share >= 60:
        risks.append((f"Top 3 engineers hold {top3_share}% of open WIP — key-person risk", AMBER))
        actions.append(("Rebalance WIP off the top 3 engineers to reduce key-person risk", AMBER))
    if cycle_delta is not None and cycle_delta > 1:
        risks.append((f"Cycle time up {cycle_delta:.0f}d over recent weeks", AMBER))
    if not risks:
        risks.append(("No critical risks flagged this period", GREEN))
    if not actions:
        actions.append(("Maintain current cadence; no urgent interventions needed", GREEN))

    # Title counts must match the visible bullets: distinguish critical (red) from watch
    # (amber) so the headline never reads "2 items" over a list of five.
    red_count = sum(1 for _, c in risks if c is RED)
    amber_count = sum(1 for _, c in risks if c is AMBER)
    if red_count and amber_count:
        title = f"Engineering Review — {red_count} Critical, {amber_count} to Watch"
    elif red_count:
        verb = "Needs" if red_count == 1 else "Need"
        title = f"Engineering Review — {red_count} Critical Item{'' if red_count == 1 else 's'} {verb} Attention"
    elif amber_count:
        title = f"Engineering Review — {amber_count} Watch Item{'' if amber_count == 1 else 's'}, No Blockers"
    else:
        title = "Engineering Review — On Track"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = f"Bottom line for {sprint_name}: where things stand, what to watch, and what to decide."
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=11, color=NAVY, font=FONT)

    vel_value = "—" if vel_now is None else f"{float(vel_now):.0f} SP {vel_arrow}"
    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Closed last sprint", "—" if sprint_throughput is None else str(int(sprint_throughput))),
            ("Story-pt velocity", vel_value),
            ("Open escalations", str(open_esc)),
            ("Reactive load", f"{reactive_wip_pct}%"),
        ],
        y=BODY_Y + 22,
    )

    col_top = cards_y + 18
    col_gap = 28
    col_w = (CONTENT_W - col_gap) / 2
    right_x = MARGIN + col_w + col_gap
    _eng_callout_column(reqs, sid, "risk", MARGIN, col_top, col_w, "What to worry about", risks)
    _eng_callout_column(reqs, sid, "act", right_x, col_top, col_w, "Decisions needed", actions)

    _eng_scope_footer(
        reqs, sid,
        f"Synthesis of this deck  ·  {sprint_name}  ·  \u25b2/\u25bc vs prior sprint where data allows  ·  Source: Jira",
    )
    return idx + 1


def eng_flow_bottlenecks_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Where work stalls: WIP, the review chokepoint, and a ranked needs-attention list.

    Attention rows lead with carried-over items (in ≥2 sprints — a strong stall
    signal) then idle items, each with owner, priority, story points, and age so a
    VP can act without opening Jira.
    """
    eng = report.get("eng_portfolio") or {}
    flow = eng.get("flow") or {}
    if not eng or not flow:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering flow data (Jira LEAN in-flight tickets)")

    active = int(flow.get("active_count") or 0)
    in_review = int(flow.get("in_review") or 0)
    stale10 = int(flow.get("stale_gt10") or 0)
    carry = int(flow.get("carryover_count") or 0)
    carry_pts = float(flow.get("carryover_points") or 0.0)
    attention_items = flow.get("attention_items") or flow.get("stale_items") or []
    jira_base = (eng.get("base_url") or "").rstrip("/")

    # Tier 2: changelog-derived time-in-status + flagged signals (when available).
    status_flow = flow.get("status_flow") or {}
    changelog_on = status_flow.get("source") == "changelog" and int(status_flow.get("enriched_count") or 0) > 0
    blocked = int(flow.get("blocked_count") or status_flow.get("blocked_count") or 0)
    by_status_median = status_flow.get("by_status_median_days") or {}

    if blocked:
        title = f"Flow Risk — {blocked} Active Item{'s' if blocked != 1 else ''} Flagged Blocked"
    elif carry:
        title = f"Flow Risk — {carry} Active Item{'s' if carry != 1 else ''} Carried Across Sprints"
    elif stale10:
        stalled_suffix = "Stalled >10 Days In Stage" if changelog_on else "Stalled >10 Days"
        title = f"Flow Bottleneck — {stale10} Active Item{'s' if stale10 != 1 else ''} {stalled_suffix}"
    elif active:
        title = f"Flow Healthy — {active} Active Item{'s' if active != 1 else ''} Moving"
    else:
        title = "Flow & Bottlenecks — No Active Work"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = (
        "LEAN Engineering board: where active work is piling up or stalling, and which "
        "items need attention first."
    )
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=11, color=NAVY, font=FONT)

    carry_value = str(carry) + (f"  ·  {carry_pts:.0f} SP" if carry_pts else "")
    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Active WIP", str(active)),
            ("In review (chokepoint)", str(in_review)),
            ("Blocked (flagged)", str(blocked)),
            ("Carried over (≥2 sprints)", carry_value),
        ],
        y=BODY_Y + 22,
    )

    # ── Median time in current stage (changelog) — reveals the real chokepoint ──
    stage_y = cards_y + 10
    if changelog_on and by_status_median:
        ordered = [
            (s, by_status_median[s])
            for s in ("In Progress", "In Review")
            if by_status_median.get(s) is not None
        ]
        if ordered:
            worst = max(ordered, key=lambda kv: kv[1])[0]
            label = "Median time in current stage:  "
            line = label + "   ".join(f"{s} {v:.0f}d" for s, v in ordered)
            _box(reqs, f"{sid}_stage", sid, MARGIN, stage_y, CONTENT_W, 14, line)
            _style(reqs, f"{sid}_stage", 0, len(line), size=10, color=GRAY, font=FONT)
            _style(reqs, f"{sid}_stage", 0, len(label), bold=True, size=10, color=NAVY, font=FONT)
            # Bold + red the chokepoint stage segment.
            seg = f"{worst} {dict(ordered)[worst]:.0f}d"
            at = line.find(seg)
            if at >= 0:
                _style(reqs, f"{sid}_stage", at, at + len(seg), bold=True, size=10, color=RED, font=FONT)
            stage_y += 16

    # ── Full-width "needs attention" table ───────────────────────────────────
    header = "Needs attention — flagged, carried-over & stalled active items"
    table_hdr_y = stage_y + 8
    _box(reqs, f"{sid}_att_h", sid, MARGIN, table_hdr_y, CONTENT_W, 14, header)
    _style(reqs, f"{sid}_att_h", 0, len(header), bold=True, size=11, color=NAVY, font=FONT)
    table_top = table_hdr_y + 20

    if not attention_items:
        msg = "No carried-over or stalled active items — flow is healthy this sprint."
        _box(reqs, f"{sid}_att_e", sid, MARGIN, table_top, CONTENT_W, 16, msg)
        _style(reqs, f"{sid}_att_e", 0, len(msg), size=10, color=GREEN, font=FONT)
        _eng_takeaway_bar(reqs, sid, report, "flow_bottlenecks")
        return idx + 1

    # Key | Summary | Owner | Pri | In stage | Spr | SP  (sums to CONTENT_W = 624;
    # every column ≥ 32pt per Google Slides' minimum table-column width).
    col_widths = [84.0, 264.0, 92.0, 44.0, 56.0, 40.0, 44.0]
    headers = ["Key", "Summary", "Owner", "Pri", "In stage", "Spr", "SP"]
    aligns = ["START", "START", "START", "START", "END", "END", "END"]
    row_h = 22.0
    max_rows = max(1, int((_ENG_CONTENT_BOTTOM - table_top) // row_h) - 1)
    display = attention_items[:max_rows]
    num_rows = 1 + len(display)
    table_id = f"{sid}_atbl"
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * row_h),
                "transform": _tf(MARGIN, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })
    _clean_table(reqs, table_id, num_rows, len(headers))
    _table_column_widths(reqs, table_id, col_widths)
    for ci, h in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, ci, h)
        _table_cell_style(reqs, table_id, 0, ci, len(h), bold=True, color=GRAY, size=8, font=FONT, align=aligns[ci])

    summary_chars = max_chars_one_line_for_table_col(col_widths[1], 9.0)
    owner_chars = max_chars_one_line_for_table_col(col_widths[2], 9.0)
    for ri, item in enumerate(display, start=1):
        # Prefer precise changelog days-in-current-status; fall back to the proxy.
        stage_days = item.get("days_in_status")
        if stage_days is None:
            stage_days = item.get("idle_days")
        spr = int(item.get("sprint_count") or 0)
        is_carry = bool(item.get("carryover"))
        is_flagged = bool(item.get("flagged"))
        sp = item.get("story_points")
        prio = str(item.get("priority") or "")
        prio_short = prio.split(":")[0] if ":" in prio else prio
        owner = str(item.get("assignee") or "")
        owner_first = owner.split()[0] if owner and owner != "Unassigned" else (owner or "—")
        stage_color = RED if (stage_days or 0) > 10 else (AMBER if (stage_days or 0) > 5 else GRAY)
        summary_text = _truncate_one_line(str(item.get("summary") or ""), summary_chars)
        if is_flagged:
            summary_text = _truncate_one_line("\u2691 " + str(item.get("summary") or ""), summary_chars)
        key = str(item.get("key", ""))
        link = f"{jira_base}/browse/{key}" if jira_base and key else None
        cells = [
            (key, RED if is_flagged else BLUE, MONO, "START", link),
            (summary_text, RED if is_flagged else NAVY, FONT, "START", None),
            (_truncate_one_line(owner_first, owner_chars), GRAY, FONT, "START", None),
            (prio_short or "—", NAVY, FONT, "START", None),
            ("—" if stage_days is None else f"{int(round(stage_days))}d", stage_color, MONO, "END", None),
            (str(spr), RED if is_carry else GRAY, MONO, "END", None),
            ("—" if sp is None else f"{float(sp):.0f}", NAVY, MONO, "END", None),
        ]
        for ci, (text, color, font, align, cell_link) in enumerate(cells):
            _table_cell_text(reqs, table_id, ri, ci, text)
            _table_cell_style(
                reqs, table_id, ri, ci, len(text),
                bold=(ci == 0), color=color, size=9, font=font, align=align, link=cell_link,
            )

    _eng_takeaway_bar(reqs, sid, report, "flow_bottlenecks")
    return idx + 1


def eng_work_split_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Planned (roadmap) vs unplanned (reactive) engineering load — where capacity goes."""
    eng = report.get("eng_portfolio") or {}
    split = eng.get("work_split") or {}
    if not eng or not split:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering work-split data (Jira LEAN tickets)")

    wip = split.get("wip") or {}
    closed = split.get("closed") or {}
    reactive_wip_pct = int(split.get("reactive_wip_pct") or 0)
    reactive_closed_pct = int(split.get("reactive_closed_pct") or 0)
    breakdown = split.get("unplanned_breakdown") or {}
    planned_wip = int(wip.get("planned") or 0)
    unplanned_wip = int(wip.get("unplanned") or 0)

    if reactive_wip_pct >= 40:
        title = f"Reactive Work Dominating — {reactive_wip_pct}% of WIP Is Unplanned"
    elif (planned_wip + unplanned_wip) > 0:
        title = f"Roadmap-Focused — {100 - reactive_wip_pct}% of WIP Is Planned"
    else:
        title = "Planned vs. Unplanned — No Open Work"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = "LEAN Engineering board: how much capacity goes to roadmap work versus reactive bugs and escalations."
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=11, color=NAVY, font=FONT)

    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Planned WIP", str(planned_wip)),
            ("Unplanned WIP", str(unplanned_wip)),
            ("Reactive share (WIP)", f"{reactive_wip_pct}%"),
            ("Reactive share (closed)", f"{reactive_closed_pct}%"),
        ],
        y=BODY_Y + 22,
    )

    charts = report.get("_charts")
    body_top = cards_y + 18
    col_gap = 24
    left_w = (CONTENT_W - col_gap) // 2
    right_w = CONTENT_W - left_w - col_gap
    right_x = MARGIN + left_w + col_gap
    chart_h = _ENG_CONTENT_BOTTOM - body_top - 20

    _eng_simple_bar_chart(
        reqs, sid, charts, oid="split",
        header="Open WIP: planned vs unplanned",
        labels=["Planned", "Unplanned"],
        values=[planned_wip, unplanned_wip],
        x=MARGIN, y=body_top, w=left_w, chart_h=chart_h,
        series_name="Tickets",
    )
    bd_items = [(k, int(v)) for k, v in breakdown.items() if int(v or 0) > 0]
    _eng_simple_bar_chart(
        reqs, sid, charts, oid="rx",
        header="Unplanned work breakdown",
        labels=[k for k, _ in bd_items],
        values=[v for _, v in bd_items],
        x=right_x, y=body_top, w=right_w, chart_h=chart_h,
        series_name="Tickets",
    )

    _eng_takeaway_bar(reqs, sid, report, "work_split")
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

    blocker_rows = min(len(blocker_crit), 6) if blocker_crit else 0
    # Per ticket: meta row + subject + 2 description lines + small gap
    _bug_ticket_h = 16 + 16 + 15 + 15 + 4
    blocker_section_h = 0
    if blocker_crit:
        blocker_section_h = 18 + 18 + blocker_rows * _bug_ticket_h + 8

    list_bottom_cap = _ENG_CONTENT_BOTTOM - blocker_section_h - 8
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

    _eng_takeaway_bar(reqs, sid, report, "bug_health")
    return idx + 1


def _short_team_label(team: str) -> str:
    """Compact board label for the velocity chart legend."""
    t = (team or "").strip()
    upper = t.upper()
    if upper.startswith("LEAN"):
        return "LEAN"
    if "DATA INTEG" in upper:
        return "Data Integ."
    if upper.startswith("CUSTOMER"):
        return "CUSTOMER"
    return _truncate_one_line(t, 12)


_VELOCITY_TABLE_HEADER_H = 14
_VELOCITY_TABLE_ROW_H = 12
_VELOCITY_LEGEND_H = 18


def _render_sp_velocity(
    reqs: list[dict[str, Any]],
    sid: str,
    velocity: dict[str, Any],
    charts: Any,
    *,
    x: float,
    y: float,
    w: float,
    ceiling: float,
) -> float:
    """Story-points-per-sprint combo (bars per board + tickets line), legend, and a per-sprint table."""
    labels = velocity.get("labels") or []
    teams = velocity.get("teams") or []
    sp_by_team = velocity.get("sp_by_team") or {}
    tickets_total = velocity.get("tickets_total") or []
    sp_total = velocity.get("sp_total") or []

    table_rows = min(5, len(labels))
    table_h = 4 + _VELOCITY_TABLE_HEADER_H + table_rows * _VELOCITY_TABLE_ROW_H
    legend_h = _VELOCITY_LEGEND_H if charts else 0
    avail = ceiling - y
    chart_h = 0
    if charts:
        chart_h = int(max(90, min(160, avail - legend_h - table_h - 8)))

    cur_y = y
    if charts and chart_h > 0:
        try:
            from .charts import embed_chart

            axis_labels = [_truncate_one_line(_format_sprint_name_for_display(s), 12) for s in labels]
            bar_series = {team: list(sp_by_team.get(team) or []) for team in teams}
            ss_id, chart_id = charts.add_combo_chart(
                title="Sprint Velocity",
                labels=axis_labels,
                bar_series=bar_series,
                line_series={"Tickets delivered": list(tickets_total)},
                show_title=False,
                suppress_legend=True,
            )
            embed_chart(reqs, f"{sid}_spchart", sid, ss_id, chart_id, x, cur_y, w, chart_h, linked=False)
            cur_y += chart_h + 4

            entries: list[tuple[str, dict[str, float]]] = []
            for i, team in enumerate(teams):
                entries.append((_short_team_label(team), BRAND_SERIES_COLORS[i % len(BRAND_SERIES_COLORS)]))
            entries.append(("Tickets", BRAND_SERIES_COLORS[len(teams) % len(BRAND_SERIES_COLORS)]))
            cur_y = _slide_chart_legend(
                reqs, sid, f"{sid}_splgd", x, cur_y, entries,
                font_pt=9, swatch_size=9, entry_gap=14,
            )
        except Exception as exc:
            logger.warning("Sprint velocity chart embed failed: %s", exc)

    cur_y += 4
    header = f"{'Sprint':<14}{'SP':>5}{'Tix':>6}"
    _box(reqs, f"{sid}_spt_h", sid, x, cur_y, w, _VELOCITY_TABLE_HEADER_H, header)
    _style(reqs, f"{sid}_spt_h", 0, len(header), bold=True, size=8, color=GRAY, font=MONO)
    cur_y += _VELOCITY_TABLE_HEADER_H
    start = max(0, len(labels) - table_rows)
    for slot in range(start, len(labels)):
        name = _truncate_one_line(_format_sprint_name_for_display(labels[slot]), 13)
        sp = float(sp_total[slot]) if slot < len(sp_total) else 0.0
        tix = int(tickets_total[slot]) if slot < len(tickets_total) else 0
        row = f"{name:<14}{sp:>5.0f}{tix:>6}"
        _box(reqs, f"{sid}_spt{slot}", sid, x, cur_y, w, _VELOCITY_TABLE_ROW_H, row)
        _style(reqs, f"{sid}_spt{slot}", 0, len(row), size=8, color=NAVY, font=MONO)
        cur_y += _VELOCITY_TABLE_ROW_H
    return cur_y


def _render_weekly_throughput_fallback(
    reqs: list[dict[str, Any]],
    sid: str,
    throughput: list[dict[str, Any]],
    charts: Any,
    *,
    x: float,
    y: float,
    w: float,
    ceiling: float,
) -> float:
    """Legacy weekly created-vs-closed combo + table, used when sprint velocity is unavailable."""
    recent_weeks = throughput[-12:] if len(throughput) >= 12 else throughput
    if not recent_weeks:
        return y
    TH = _VELOCITY_TABLE_HEADER_H
    ROW_H = _VELOCITY_TABLE_ROW_H
    table_rows = min(6, len(recent_weeks))
    table_h = 4 + TH + table_rows * ROW_H
    chart_h = int(max(0, min(150, (ceiling - y) - table_h - 8))) if charts else 0

    cur_y = y
    if charts and chart_h > 0:
        try:
            from .charts import embed_chart

            ss_id, chart_id = charts.add_combo_chart(
                title="Weekly Throughput",
                labels=[week.get("label", "") for week in recent_weeks],
                bar_series={"Created": [week.get("created", 0) for week in recent_weeks]},
                line_series={"Closed": [week.get("resolved", 0) for week in recent_weeks]},
                show_title=False,
            )
            embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id, x, cur_y, w, chart_h, linked=False)
            cur_y += chart_h + 6
        except Exception as exc:
            logger.warning("Throughput chart embed failed: %s", exc)

    cur_y += 4
    header = "Week        Created  Closed"
    _box(reqs, f"{sid}_wt_h", sid, x, cur_y, w, TH, header)
    _style(reqs, f"{sid}_wt_h", 0, len(header), bold=True, size=8, color=GRAY, font=MONO)
    cur_y += TH
    for week in recent_weeks[-table_rows:]:
        row = f"{week['label']:<12}  {week.get('created', 0):>5}    {week.get('resolved', 0):>4}"
        wk = str(week.get("week", week.get("label", "w")))
        safe_wk = "".join(c if c.isalnum() else "_" for c in wk)[:24]
        _box(reqs, f"{sid}_wr{safe_wk}", sid, x, cur_y, w, ROW_H, row)
        _style(reqs, f"{sid}_wr{safe_wk}", 0, len(row), size=8, color=NAVY, font=MONO)
        cur_y += ROW_H
    return cur_y


def eng_bug_flow_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Bug inflow vs. outflow: weekly created-vs-resolved bugs and the backlog trend."""
    eng = report.get("eng_portfolio") or {}
    bf = eng.get("bug_flow") or {}
    weeks = bf.get("weeks") or []
    if not weeks:
        detail = bf.get("error") or "Bug flow (LEAN bugs created vs resolved)"
        return _missing_data_slide(reqs, sid, report, idx, detail)

    created_total = int(bf.get("created_total") or 0)
    resolved_total = int(bf.get("resolved_total") or 0)
    net = int(bf.get("net_total") or 0)
    open_now = bf.get("open_now")
    wk_n = int(bf.get("weeks_count") or len(weeks))
    trend = bf.get("trend") or "flat"

    if trend == "growing":
        title = f"Bug Backlog Growing — Net +{net} Over {wk_n} Weeks"
    elif trend == "shrinking":
        title = f"Bug Backlog Shrinking — Net {abs(net)} Resolved Over {wk_n} Weeks"
    else:
        title = f"Bug Inflow ≈ Outflow — {created_total} In / {resolved_total} Out Over {wk_n} Weeks"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = (
        f"LEAN bugs created vs. resolved per week (last {wk_n} weeks). Net = created − resolved; "
        "a backlog only shrinks when the resolved line sits above the created bars."
    )
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=9.5, color=NAVY, font=FONT)

    net_txt = f"+{net}" if net > 0 else str(net)
    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Open bugs now", "—" if open_now is None else str(int(open_now))),
            (f"Created ({wk_n}w)", str(created_total)),
            (f"Resolved ({wk_n}w)", str(resolved_total)),
            ("Net change", net_txt),
        ],
        y=BODY_Y + 22,
    )

    charts = report.get("_charts")
    chart_top = cards_y + 16
    labels = [_truncate_one_line(str(w.get("label") or ""), 8) for w in weeks]
    created = [int(w.get("created") or 0) for w in weeks]
    resolved = [int(w.get("resolved") or 0) for w in weeks]
    net_series = [int(w.get("net") or 0) for w in weeks]

    if charts:
        try:
            from .charts import embed_chart, BRAND_SERIES_COLORS
            from .charts import LINE_SERIES_COLORS

            chart_h = int(max(120, _ENG_CONTENT_BOTTOM - chart_top - _ENG_TAKEAWAY_H))
            ss_id, chart_id = charts.add_combo_chart(
                title="Bug flow",
                labels=labels,
                bar_series={"Created": created, "Resolved": resolved},
                line_series={"Net (created−resolved)": net_series},
                show_title=False,
                suppress_legend=True,
            )
            embed_chart(reqs, f"{sid}_bfchart", sid, ss_id, chart_id, MARGIN, chart_top, CONTENT_W, chart_h, linked=False)
            entries = [
                ("Created", BRAND_SERIES_COLORS[0]),
                ("Resolved", BRAND_SERIES_COLORS[1]),
                ("Net (created−resolved)", LINE_SERIES_COLORS[0]),
            ]
            _slide_chart_legend(
                reqs, sid, f"{sid}_bflgd", MARGIN, chart_top + chart_h + 2, entries,
                font_pt=9, swatch_size=9, entry_gap=16,
            )
        except Exception as exc:
            logger.warning("Bug flow chart embed failed: %s", exc)

    _eng_takeaway_bar(reqs, sid, report, "bug_flow")
    return idx + 1


# Epic progress bar track (unfilled rail behind each completion bar).
_EPIC_TRACK_FILL = {"red": 0.90, "green": 0.93, "blue": 0.97}


def eng_epic_progress_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Active initiative (epic) progress: completion bar + done/total per big rock."""
    eng = report.get("eng_portfolio") or {}
    ep = eng.get("epic_progress") or {}
    epics = ep.get("epics") or []
    if not epics:
        detail = ep.get("error") or "Epic progress (LEAN active epics)"
        return _missing_data_slide(reqs, sid, report, idx, detail)

    median_pct = ep.get("median_pct")
    total_remaining = int(ep.get("total_remaining") or 0)
    jira_base = eng.get("base_url", "")

    if median_pct is not None:
        title = f"Initiative Progress — {len(epics)} Active Epics, {int(median_pct)}% Median Complete"
    else:
        title = f"Initiative Progress — {len(epics)} Active Epics"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    context = (
        f"Largest in-flight LEAN epics by size · bar = % of child issues done · "
        f"{total_remaining} child issues still open across these initiatives."
        + ("" if ep.get("has_due_dates") else " (Epics have no due dates set in Jira, so target dates aren't shown.)")
    )
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 26, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=9.5, color=NAVY, font=FONT)

    top = BODY_Y + 30
    n = len(epics)
    row_h = min(44.0, max(30.0, (_ENG_CONTENT_BOTTOM - top) / n))

    name_w = 250.0
    bar_x = MARGIN + name_w + 8
    pct_w = 96.0
    max_bar = CONTENT_W - name_w - 8 - pct_w - 6

    for i, epic in enumerate(epics):
        y0 = top + i * row_h
        key = str(epic.get("key") or "")
        summary = str(epic.get("summary") or "")
        status = str(epic.get("status") or "")
        pct = int(epic.get("pct") or 0)
        done = int(epic.get("done") or 0)
        total = int(epic.get("total") or 0)
        overdue = bool(epic.get("overdue"))
        stale = bool(epic.get("stale"))

        name_chars = max_chars_one_line_for_table_col(name_w, 10.5)
        name = _truncate_one_line(f"{key}  {summary}", name_chars)
        _box(reqs, f"{sid}_en{i}", sid, MARGIN, y0, name_w, 16, name)
        _style(reqs, f"{sid}_en{i}", 0, len(name), size=10.5, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_en{i}", 0, len(key), bold=True, size=10.5, color=BLUE, font=FONT)

        # Completion bar: light track + fill (green when near done, blue otherwise).
        fill = GREEN if pct >= 80 else BLUE
        bar_w = max(3.0, pct / 100.0 * max_bar)
        _rect(reqs, f"{sid}_et{i}", sid, bar_x, y0 + 2, max_bar, 11, _EPIC_TRACK_FILL)
        _rect(reqs, f"{sid}_ef{i}", sid, bar_x, y0 + 2, bar_w, 11, fill)
        pct_txt = f"{pct}%  ({done}/{total})"
        _box(reqs, f"{sid}_ep{i}", sid, bar_x + max_bar + 6, y0, pct_w, 16, pct_txt)
        _style(reqs, f"{sid}_ep{i}", 0, len(pct_txt), bold=True, size=9.5, color=NAVY, font=MONO)

        # Status / risk line under the name.
        flags = []
        if overdue:
            flags.append("overdue")
        if stale:
            flags.append("no recent update")
        meta = status + (f" · {' · '.join(flags)}" if flags else "")
        if meta:
            _box(reqs, f"{sid}_es{i}", sid, MARGIN + 12, y0 + 16, name_w, 13, meta)
            risk = bool(overdue or stale)
            _style(reqs, f"{sid}_es{i}", 0, len(meta), size=8.5, color=(RED if risk else GRAY), font=FONT)

    _eng_takeaway_bar(reqs, sid, report, "epic_progress")
    return idx + 1


def eng_velocity_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Sprint velocity in story points (per board) with ticket throughput as a secondary line."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    throughput = eng.get("throughput") or []
    closed_count = eng.get("closed_count", 0)
    in_flight = eng.get("in_flight_count", 0)

    velocity = build_sprint_velocity_series(eng.get("sprint_velocity"), slots=6)
    sp_labels = velocity.get("labels") or []
    has_sp = bool(sp_labels)

    if has_sp:
        sp_total = velocity["sp_total"]
        latest = float(sp_total[-1])
        # Trend off the whole series, not just last-vs-prior: a one-sprint bounce off a
        # low point (e.g. 678→580→488→336→436) is a downtrend, not "velocity up".
        prior = [float(v) for v in sp_total[:-1] if v]
        baseline = (sum(prior) / len(prior)) if prior else None
        if baseline and latest >= baseline * 1.05:
            title = f"Velocity Up — {latest:.0f} SP Last Sprint, Above {baseline:.0f} Recent Avg"
        elif baseline and latest <= baseline * 0.95:
            title = f"Velocity Down — {latest:.0f} SP Last Sprint, Below {baseline:.0f} Recent Avg"
        else:
            title = f"Sprint Velocity — {latest:.0f} SP Last Sprint (~{baseline:.0f} Avg)" if baseline \
                else f"Sprint Velocity — {latest:.0f} SP Delivered Last Sprint"
        sp_team_names = ", ".join(velocity.get("teams") or []) or "scrum boards"
        zero_sp = velocity.get("zero_sp_teams") or []
        no_sp_note = (
            f" · {', '.join(zero_sp)} run on ticket throughput (no story points)"
            if zero_sp else ""
        )
        # Surface the ticket-throughput trend too — it can fall even when SP looks flat,
        # and tickets/sprint is the more reliable signal across estimating styles.
        tix = [int(v) for v in (velocity.get("tickets_total") or [])]
        tix_note = ""
        if len(tix) >= 2:
            tix_now = tix[-1]
            tix_prior = [v for v in tix[:-1] if v]
            tix_base = (sum(tix_prior) / len(tix_prior)) if tix_prior else None
            if tix_base and tix_now <= tix_base * 0.9:
                tix_note = f" · tickets/sprint DOWN to {tix_now} (from ~{tix_base:.0f} avg)"
            elif tix_base and tix_now >= tix_base * 1.1:
                tix_note = f" · tickets/sprint UP to {tix_now} (from ~{tix_base:.0f} avg)"
            else:
                tix_note = f" · ~{tix_now} tickets/sprint"
        context = (
            f"Bars = story points per closed sprint (per SP-estimating board: {sp_team_names}); "
            f"line = total tickets delivered{no_sp_note}{tix_note}. Aligned by recency."
        )
    else:
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
        context = f"Open: {in_flight}   ·   Closed this period: {closed_count}   ·   Last 12 weeks"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 24, context)
    _style(reqs, f"{sid}_bar", 0, len(context), size=9, color=GRAY, font=FONT)

    body_top = BODY_Y + 30
    col_gap = 20
    left_w = (CONTENT_W - col_gap) * 3 // 5
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    content_ceiling = _ENG_CONTENT_BOTTOM - 6

    by_status = eng.get("by_status") or {}
    status_items_all = sorted(by_status.items(), key=lambda item: -item[1])
    status_items = status_items_all[:6]

    charts = report.get("_charts")
    left_y = body_top
    if has_sp:
        left_y = _render_sp_velocity(
            reqs, sid, velocity, charts,
            x=left_x, y=body_top, w=left_w, ceiling=content_ceiling,
        )
    else:
        left_y = _render_weekly_throughput_fallback(
            reqs, sid, throughput, charts,
            x=left_x, y=body_top, w=left_w, ceiling=content_ceiling,
        )

    right_y = body_top
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

    _eng_takeaway_bar(reqs, sid, report, "velocity")
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
    jira_base = (eng.get("base_url") or "").rstrip("/")
    jql_by_short = support_pressure.get("jql_by_priority_short")
    if not isinstance(jql_by_short, dict):
        jql_by_short = {}

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
        prio_jql = jql_by_short.get(priority)
        prio_link = (
            f"{jira_base}/issues/?jql={urllib.parse.quote(str(prio_jql), safe='')}"
            if jira_base and isinstance(prio_jql, str) and prio_jql.strip()
            else None
        )
        label_color = priority_colors.get(priority, NAVY)
        _style(
            reqs,
            f"{sid}_pl{priority_index}",
            0,
            len(priority),
            size=12,
            bold=is_critical,
            color=label_color,
            font=FONT,
            link=prio_link,
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

    _eng_takeaway_bar(reqs, sid, report, "support_pressure")
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
