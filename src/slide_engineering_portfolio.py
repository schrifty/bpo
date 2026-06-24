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
    bar_rect as _bar_rect,
    clean_table as _clean_table,
    internal_footer as _internal_footer,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    rect as _rect,
    slide_chart_legend as _slide_chart_legend,
    slide_chart_legend_vertical as _slide_chart_legend_vertical,
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
    FONT_SERIF,
    GRAY,
    MARGIN,
    MAX_PAGINATED_SLIDE_PAGES,
    MONO,
    CURSOR_BG,
    GITHUB_BG,
    NAVY,
    SLIDE_H,
    SLIDE_W,
    TITLE_Y,
    WHITE,
    _cap_chunk_list,
    _table_rows_fit_span,
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

# User-facing labels for internal Jira board/project codes (never show raw keys on slides).
_PROJECT_DISPLAY = {
    "HELP": "Support",
    "CUSTOMER": "Implementation",
    "LEAN": "Engineering",
}


def _display_project_code(project: str) -> str:
    code = (project or "").strip().upper()
    return _PROJECT_DISPLAY.get(code, (project or "").strip() or "—")


def _display_team_name(team: str) -> str:
    """Compact, executive-friendly squad label."""
    t = (team or "").strip()
    if not t:
        return t
    upper = t.upper()
    if "DATA INTEG" in upper:
        return "Data Integration"
    if upper.startswith("CUSTOMER"):
        return "Implementation"
    if upper.startswith("LEAN"):
        rest = t[4:].strip(" -")
        return f"Engineering — {rest}" if rest else "Engineering"
    return t


def _truncate_callout(text: str, *, max_chars: int = 92) -> str:
    return _truncate_one_line(" ".join((text or "").split()), max_chars)

# Embedded column charts on small slide bands: axis/category text one step above CHART_AXIS_PT (12).
_SPRINT_SNAPSHOT_CHART_AXIS_PT = CHART_AXIS_PT + 2

# KPI band placement (see docs/PRESENTATION/SLIDE_DESIGN_STANDARDS.md).
_ENG_KPI_AFTER_TITLE_Y = BODY_Y + 4
_ENG_KPI_AFTER_CONTEXT_Y = BODY_Y + 20


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


def _eng_title(
    reqs: list[dict[str, Any]],
    sid: str,
    title: str,
    subtitle: str | None = None,
) -> None:
    """High-impact short title + a story subtitle.

    The title is a stable, short section label (TOC-friendly); the subtitle carries
    the dynamic one-line story that used to be crammed into the title. This keeps the
    headline scannable while preserving the per-period narrative.
    """
    tid = f"{sid}_ttl"
    _box(reqs, tid, sid, MARGIN, TITLE_Y - 4, CONTENT_W, 28, title)
    _style(reqs, tid, 0, len(title), bold=True, size=22, color=NAVY, font=FONT_SERIF)
    if subtitle:
        sub = " ".join(subtitle.split()).strip()
        sub = _truncate_one_line(sub, 130)
        sub_id = f"{sid}_sub"
        _box(reqs, sub_id, sid, MARGIN, TITLE_Y + 26, CONTENT_W, 18, sub)
        _style(reqs, sub_id, 0, len(sub), size=11.5, color=GRAY, font=FONT)
    ul_y = TITLE_Y + 48 if subtitle else TITLE_Y + 26
    _rect(reqs, f"{sid}_ul", sid, MARGIN, ul_y, 56, 2.5, BLUE)
    _internal_footer(reqs, sid)


def eng_toc_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Agenda — section list for the engineering portfolio deck."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Agenda", "Sections in this review")

    sections = [
        "Executive Summary",
        "Team & Org",
        "Outcomes",
        "Operational Health",
        "Quality",
        "Backlog & Support",
        "Engineering Output",
        "AI Tooling",
        "Productivity",
        "Appendix",
    ]

    row_h = 28.0
    y = BODY_Y + 20
    for i, name in enumerate(sections):
        line = f"{i + 1}.  {name}"
        _box(reqs, f"{sid}_s{i}", sid, MARGIN, y, CONTENT_W, 18, line)
        _style(reqs, f"{sid}_s{i}", 0, len(line), size=13, color=NAVY, font=FONT)
        y += row_h

    return idx + 1


def eng_divider_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Section divider for engineering portfolio chapters."""
    entry = report.get("_current_slide") or {}
    section_title = str(entry.get("title") or "").strip()
    title_key = section_title.casefold()
    if "ai tooling" in title_key or ("cursor" in title_key and "productivity" not in title_key):
        bg, title_color = CURSOR_BG, NAVY
    elif "engineering output" in title_key or "github" in title_key:
        bg, title_color = GITHUB_BG, NAVY
    elif "productivity" in title_key:
        bg, title_color = WHITE, NAVY
    else:
        bg, title_color = NAVY, WHITE

    _slide(reqs, sid, idx)
    _bg(reqs, sid, bg)
    if section_title:
        _box(reqs, f"{sid}_sec", sid, MARGIN, SLIDE_H * 0.38, CONTENT_W, 56, section_title)
        _style(
            reqs, f"{sid}_sec", 0, len(section_title),
            bold=True, size=32, color=title_color, font=FONT_SERIF,
        )
    _internal_footer(reqs, sid)
    return idx + 1


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

    subtitle_y = 160.0
    subtitle_color = {"red": 0.6, "green": 0.8, "blue": 1.0}
    if sprint_label:
        sub = f"Sprint: {sprint_label}"
        _box(reqs, f"{sid}_sp", sid, MARGIN, subtitle_y, CONTENT_W, 24, sub)
        _style(reqs, f"{sid}_sp", 0, len(sub), size=14, color=subtitle_color, font=FONT)
        subtitle_y += 28.0

    cu = report.get("cursor_usage") or {}
    if cu.get("configured"):
        run_rate = (cu.get("totals") or {}).get("charged_cents_window")
        cursor_sub = f"Cursor AI 30d run rate: {_fmt_cents(run_rate)}"
        _box(reqs, f"{sid}_cu", sid, MARGIN, subtitle_y, CONTENT_W, 24, cursor_sub)
        _style(reqs, f"{sid}_cu", 0, len(cursor_sub), size=14, color=subtitle_color, font=FONT)

    generated = datetime.now().strftime("%B %-d, %Y at %-I:%M %p")
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
        subtitle = f"{total_throughput} issues closed last sprint across {len(teams)} squads"
    else:
        subtitle = f"{len(teams)} development squads"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Team Scorecard", subtitle)

    context = (
        "Closed = issues resolved in the latest sprint; lead time = median days created→resolved. "
        "Engineering squads use continuous flow; Implementation uses weekly sprints."
    )
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=10, color=GRAY, font=FONT)

    total_sp = summary.get("total_story_points_delivered")
    kpi_cards: list[tuple[str, str]] = []
    if total_throughput is not None:
        kpi_cards.append(("Closed (sprint)", str(int(total_throughput))))
    if total_sp:
        kpi_cards.append(("Story points", f"{float(total_sp):.0f}"))
    if avg_lead is not None:
        kpi_cards.append(("Avg lead time", _format_scorecard_days(avg_lead)))
    kpi_cards.append(("Squads", str(len(teams))))

    cards_y = _eng_kpi_row(reqs, sid, kpi_cards[:4], y=_ENG_KPI_AFTER_CONTEXT_Y, h=50)

    # ── Native team table ────────────────────────────────────────────────────
    table_top = cards_y + 12
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
        note = f"+{dropped} more team{'s' if dropped != 1 else ''} not shown"
        _box(reqs, f"{sid}_drop", sid, MARGIN, table_top, CONTENT_W, 12, note)
        _style(reqs, f"{sid}_drop", 0, len(note), size=9, color=GRAY, font=FONT)
        table_top += 14
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
        team_name = _truncate_one_line(_display_team_name(str(team.get("team") or "")), team_chars)
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

    subtitle = f"{total} engineers across {len(teams)} squads"
    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Engineering Teams", subtitle)

    if roster.get("source") == "atlassian_teams":
        context = (
            "Membership and headcount from Atlassian Teams (the Dev squads) · bar length = team "
            "headcount · bold name = team lead · an engineer may belong to more than one squad."
        )
    else:
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

        # Members line, with optional bold "Lead: <name>" prefix. The lead is shown in
        # the prefix, so drop them from the member list to avoid repeating the name.
        if lead:
            members = [m for m in members if m.strip().casefold() != lead.casefold()]
        prefix = f"Lead: {lead} — " if lead else ""
        line = _truncate_one_line(prefix + ", ".join(members), mem_chars)
        if line:
            _box(reqs, f"{sid}_mm{i}", sid, MARGIN + mem_indent, y0 + 17, mem_w, 14, line)
            _style(reqs, f"{sid}_mm{i}", 0, len(line), size=9, color=GRAY, font=FONT)
            if prefix:
                dash = line.find("—")
                bold_end = dash if dash != -1 else min(len(line), len(prefix))
                if bold_end > 0:
                    _style(reqs, f"{sid}_mm{i}", 0, bold_end, bold=True, size=9, color=NAVY, font=FONT)

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

    subtitle = f"{in_flight} open, {active} active, {bugs_in_flight} bugs in flight"
    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, sprint_name, subtitle)

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
    _render_takeaway_band(reqs, sid, text)


def _render_takeaway_band(reqs: list[dict[str, Any]], sid: str, text: str) -> None:
    """Render the bottom 'what this means' band for a single implication sentence.

    No-ops when *text* is empty so a slide never shows an orphan label.
    """
    text = _clamp_eng_takeaway(text)
    if not text:
        return
    _rect(reqs, f"{sid}_tkdiv", sid, MARGIN, _ENG_TAKEAWAY_Y - 3.0, CONTENT_W, 1.2, _ENG_DIVIDER_FILL)
    _box(reqs, f"{sid}_tklbl", sid, MARGIN, _ENG_TAKEAWAY_Y, CONTENT_W, 11, _ENG_TAKEAWAY_LABEL)
    _style(reqs, f"{sid}_tklbl", 0, len(_ENG_TAKEAWAY_LABEL), bold=True, size=7.5, color=BLUE, font=FONT)
    _box(reqs, f"{sid}_tktxt", sid, MARGIN, _ENG_TAKEAWAY_Y + 12, CONTENT_W, 28, text)
    _style(reqs, f"{sid}_tktxt", 0, len(text), size=10, color=NAVY, font=FONT)


def _clamp_eng_takeaway(text: str, *, max_lines: int = 2, font_pt: float = 10.0) -> str:
    """Keep takeaway copy within the fixed band so it does not overflow or wrap badly."""
    text = " ".join((text or "").split()).strip()
    if not text:
        return ""
    # Strip a lone stray quote the LLM sometimes appends.
    if text.endswith('"') and not text.endswith('""'):
        text = text[:-1].rstrip()
    if text.startswith('"') and text.count('"') == 1:
        text = text[1:].strip()
    chars_per_line = max(40, int(CONTENT_W / (font_pt * 0.52)))
    max_chars = chars_per_line * max_lines
    if len(text) <= max_chars:
        return text
    cut = text[: max_chars - 1].rsplit(" ", 1)[0]
    return (cut or text[: max_chars - 1]).rstrip(",.;:") + "…"


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
        subtitle = f"{sprint_name}: {active} active, {bugs} bug{'s' if bugs != 1 else ''} in flight"
    elif in_flight:
        subtitle = f"{sprint_name}: {active} of {in_flight} items active"
    else:
        subtitle = f"{sprint_name}: no open work in sprint"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Current Sprint", subtitle)

    card_y = BODY_Y + 4
    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Open items", str(in_flight)),
            ("Active now", str(active)),
            ("Bugs in flight", str(bugs)),
            ("Closed this period", str(closed)),
        ],
        y=card_y,
    )

    legend = "Per theme: open items · active (in progress/review) · bugs (B, shown in red)."
    legend_y = cards_y + 10
    _box(reqs, f"{sid}_lgd", sid, MARGIN, legend_y, CONTENT_W, 12, legend)
    _style(reqs, f"{sid}_lgd", 0, len(legend), size=8.5, color=GRAY, font=FONT)
    theme_top = legend_y + 16

    # Backlog hygiene callout: most "open" items are abandoned, not active work.
    staleness = eng.get("backlog_staleness") or {}
    abandoned_open = int(staleness.get("abandoned_open") or 0)
    abandoned_pct = int(staleness.get("abandoned_pct") or 0)
    abandoned_days = int(staleness.get("abandoned_days") or 180)
    if abandoned_open:
        note = (
            f"\u26a0  {abandoned_open} of {in_flight} open items ({abandoned_pct}%) untouched in "
            f">{abandoned_days}d — backlog needs triage; only {active} are actively in progress/review."
        )
        _box(reqs, f"{sid}_stale", sid, MARGIN, theme_top, CONTENT_W, 14, note)
        _style(reqs, f"{sid}_stale", 0, len(note), size=9.5, color=AMBER, font=FONT)
        theme_top += 18
    theme_bottom = _ENG_CONTENT_BOTTOM

    # Surface real areas of work; "Untagged" (no [theme] prefix or epic link) would
    # otherwise dominate the bars and hide them, so report it as a hygiene caption.
    themes_all = [t for t in (eng.get("themes") or []) if int(t.get("total") or 0) > 0]
    untagged = next((t for t in themes_all if t.get("theme") == "Untagged"), None)
    themes = [t for t in themes_all if t.get("theme") != "Untagged"][:8]
    untagged_n = int(untagged.get("total") or 0) if untagged else 0
    hdr_extra = ""
    if untagged_n:
        u_pct = round(100 * untagged_n / in_flight) if in_flight else 0
        hdr_extra = f"   (+{untagged_n} untagged, {u_pct}% — need a [theme] prefix or epic link)"
    header = "Active work by theme"
    _box(reqs, f"{sid}_tht", sid, MARGIN, theme_top, CONTENT_W, 16, header + hdr_extra)
    _style(reqs, f"{sid}_tht", 0, len(header) + len(hdr_extra), bold=True, size=11, color=NAVY, font=FONT)
    if hdr_extra:
        _style(reqs, f"{sid}_tht", len(header), len(header) + len(hdr_extra), bold=False, size=9, color=GRAY, font=FONT)
    y = theme_top + 20

    if themes:
        max_total = max(int(t.get("total") or 0) for t in themes) or 1
        label_w = 172.0
        count_w = 70.0
        bar_x = MARGIN + label_w
        bar_max = CONTENT_W - label_w - count_w - 6
        theme_label_chars = max_chars_one_line_for_table_col(label_w - 4, 10.0)
        avail_rows = max(1, int((theme_bottom - y) // 22))
        for ri, theme in enumerate(themes[:avail_rows]):
            if y + 22 > theme_bottom:
                break
            total_n = int(theme.get("total") or 0)
            active_n = int(theme.get("in_progress") or 0)
            bugs_n = int(theme.get("bugs") or 0)
            name = _truncate_one_line(str(theme.get("theme") or "—"), theme_label_chars)
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
        subtitle = f"Aging — median {median_age:.0f}d, {over_90} open over 90d"
    elif median_age is not None:
        subtitle = f"Healthy — median {median_age:.0f}d, none over 90d"
    else:
        subtitle = f"{open_count} open"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Escalation Backlog", subtitle)

    context = "Health of the LEAN engineering escalation queue: how much is open and how old it is."
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, context)
    _style(reqs, f"{sid}_ctx", 0, len(context), size=11, color=NAVY, font=FONT)

    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Open escalations", str(open_count)),
            ("Median open age", _fmt_days(median_age)),
            ("Avg resolve cycle", _fmt_days(avg_cycle)),
            ("Resolved (6 mo)", resolved_label),
        ],
        y=_ENG_KPI_AFTER_CONTEXT_Y,
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
    active_by = eng.get("by_assignee_active", {}) or {}
    stale_by = eng.get("by_assignee_stale", {}) or {}
    staleness = eng.get("backlog_staleness") or {}
    abandoned_days = int(staleness.get("abandoned_days") or 180)
    active_days = int(staleness.get("active_days") or 30)
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
            "active": int(active_by.get(name, 0) or 0),
            "stale": int(stale_by.get(name, 0) or 0),
            "r30": int(r.get("1m", 0) or 0),
            "r90": int(r.get("3m", 0) or 0),
        })
    # Rank by *active* WIP (real load) so stale-assignment hoarders sink, not surface.
    rows.sort(key=lambda x: (-x["active"], -x["r90"], -x["wip"]))

    if not rows:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering capacity (no assignee data on LEAN board)")

    total_active = sum(r["active"] for r in rows)
    total_wip = sum(r["wip"] for r in rows)
    # Keep every number on this slide assigned-scoped (it is a per-engineer view), so
    # stale is a subset of assigned WIP — never larger than it. The all-open stale count
    # (which also includes unassigned items) lives in the Current Sprint backlog-hygiene
    # callout, with the matching open-backlog denominator.
    total_stale = sum(r["stale"] for r in rows)
    all_open_stale = int(staleness.get("abandoned_open") or 0)
    unassigned_stale = max(0, all_open_stale - total_stale)
    top3_active = sum(r["active"] for r in rows[:3])
    top3_share = int(round(top3_active / total_active * 100)) if total_active else 0
    engineers_active = sum(1 for r in rows if r["active"] > 0)

    if total_active and top3_share >= 60:
        subtitle = f"Top 3 engineers hold {top3_share}% of active WIP"
    elif total_active:
        subtitle = f"{total_active} active items across {engineers_active} engineers"
    else:
        subtitle = "No active WIP on the engineering board"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Engineering Load", subtitle)

    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Engineers (active)", str(engineers_active)),
            (f"Active WIP (\u2264{active_days}d)", str(total_active)),
            (f"Stale WIP (>{abandoned_days}d)", str(total_stale)),
            ("Top 3 share", f"{top3_share}%" if total_active else "—"),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    # Native table: Engineer | Active WIP | Total WIP | Resolved 30d | Resolved 90d.
    table_top = cards_y + 12
    col_widths = [216.0, 102.0, 102.0, 102.0, 102.0]
    headers = ["Engineer", f"Active (\u2264{active_days}d)", "Total WIP", "Resolved 30d", "Resolved 90d"]
    aligns = ["START", "END", "END", "END", "END"]
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
        # Stale assignment: meaningful assigned WIP but nothing active and no throughput.
        stale_assignment = row["wip"] >= 15 and row["active"] <= 1 and row["r90"] == 0
        name_color = AMBER if stale_assignment else NAVY
        cells = [
            (_truncate_one_line(row["name"], name_chars), name_color, FONT),
            (str(row["active"]), BLUE if row["active"] else GRAY, MONO),
            (str(row["wip"]), GRAY, MONO),
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
        bullet = f"\u25cf  {_truncate_callout(text)}"
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
    stale_recent = int(flow.get("stale_recent") or 0)
    abandoned = int(flow.get("abandoned_in_stage") or 0)
    abandoned_days = int(flow.get("abandoned_days") or 180)
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
    # Initiative delivery risk: epics with open work but no recent movement (from the
    # Initiative Progress slide) — the "big rocks" that have quietly stopped.
    epic_progress = eng.get("epic_progress") or {}
    epics_at_risk = int(epic_progress.get("at_risk_count") or 0)
    if epics_at_risk:
        n = epics_at_risk
        risks.append((f"{n} initiative{'s' if n != 1 else ''} stalled — no child activity in 30d", AMBER))
        actions.append((f"Re-engage or re-scope the {n} stalled initiative{'s' if n != 1 else ''}", AMBER))
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
        risks.append((f"Implementation sprints carry ~{carryover} open items week to week — scope inflated", AMBER))
        actions.append((f"Right-size Implementation sprint scope (~{carryover} standing backlog)", AMBER))
    if over_90:
        oldest_txt = f", oldest {float(oldest):.0f}d" if oldest is not None else ""
        risks.append((f"{over_90} escalation{'s' if over_90 != 1 else ''} open >90 days{oldest_txt}", AMBER))
        actions.append((f"Run a backlog scrub on the {over_90} escalation{'s' if over_90 != 1 else ''} aging past 90 days", AMBER))
    if stale_recent:
        risks.append((f"{stale_recent} active item{'s' if stale_recent != 1 else ''} stalled 10–{abandoned_days}d in stage — flow stalling", AMBER))
        actions.append((f"Unblock or re-assign the {stale_recent} stalled item{'s' if stale_recent != 1 else ''}", AMBER))
    if abandoned:
        risks.append((f"{abandoned} active item{'s' if abandoned != 1 else ''} abandoned in stage >{abandoned_days}d — backlog hygiene", AMBER))
        actions.append((f"Triage the {abandoned} abandoned in-progress item{'s' if abandoned != 1 else ''} — close or re-engage", AMBER))
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
        verdict = f"{red_count} critical, {amber_count} to watch"
    elif red_count:
        verb = "needs" if red_count == 1 else "need"
        verdict = f"{red_count} critical item{'' if red_count == 1 else 's'} {verb} attention"
    elif amber_count:
        verdict = f"{amber_count} watch item{'' if amber_count == 1 else 's'}, no blockers"
    else:
        verdict = "on track"

    subtitle = f"{sprint_name}: {verdict}"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Executive Summary", subtitle)

    vel_value = "—" if vel_now is None else f"{float(vel_now):.0f} SP {vel_arrow}"
    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Closed (all teams)", "—" if sprint_throughput is None else str(int(sprint_throughput))),
            ("Sprint velocity", vel_value),
            ("Open escalations", str(open_esc)),
            ("Reactive load", f"{reactive_wip_pct}%"),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y + 4,
    )

    col_top = cards_y + 14
    col_gap = 28
    col_w = (CONTENT_W - col_gap) / 2
    right_x = MARGIN + col_w + col_gap
    _eng_callout_column(reqs, sid, "risk", MARGIN, col_top, col_w, "Watch list", risks)
    _eng_callout_column(reqs, sid, "act", right_x, col_top, col_w, "Decisions", actions)
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
    stale_recent = int(flow.get("stale_recent") or 0)
    abandoned = int(flow.get("abandoned_in_stage") or 0)
    abandoned_days = int(flow.get("abandoned_days") or 180)
    carry = int(flow.get("carryover_count") or 0)
    attention_items = flow.get("attention_items") or flow.get("stale_items") or []
    jira_base = (eng.get("base_url") or "").rstrip("/")

    # Tier 2: changelog-derived time-in-status + flagged signals (when available).
    status_flow = flow.get("status_flow") or {}
    changelog_on = status_flow.get("source") == "changelog" and int(status_flow.get("enriched_count") or 0) > 0
    blocked = int(flow.get("blocked_count") or status_flow.get("blocked_count") or 0)
    # Prefer stage medians computed on non-abandoned items — the all-items median is
    # dragged to years by zombies parked in-stage and misrepresents the real chokepoint.
    by_status_median = flow.get("by_status_median_active") or status_flow.get("by_status_median_days") or {}

    if blocked:
        subtitle = f"{blocked} active item{'s' if blocked != 1 else ''} flagged blocked"
    elif carry:
        subtitle = f"{carry} active item{'s' if carry != 1 else ''} carried across sprints"
    elif stale_recent:
        subtitle = f"{stale_recent} item{'s' if stale_recent != 1 else ''} stalled 10–{abandoned_days}d in stage"
    elif abandoned:
        subtitle = f"{abandoned} item{'s' if abandoned != 1 else ''} abandoned in stage >{abandoned_days}d"
    elif active:
        subtitle = f"{active} active item{'s' if active != 1 else ''} in flight"
    else:
        subtitle = "No active work"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Flow & Bottlenecks", subtitle)

    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Active WIP", str(active)),
            ("In review", str(in_review)),
            (f"Stalled 10–{abandoned_days}d", str(stale_recent)),
            (f"Abandoned >{abandoned_days}d", str(abandoned)),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    # ── Median time in current stage (changelog) — reveals the real chokepoint ──
    stage_y = cards_y + 8
    if changelog_on and by_status_median:
        ordered = [
            (s, by_status_median[s])
            for s in ("In Progress", "In Review")
            if by_status_median.get(s) is not None
        ]
        if ordered:
            worst = max(ordered, key=lambda kv: kv[1])[0]
            label = "Median time in stage (active, excl. abandoned):  "
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
    header = (
        f"Needs attention — recent stalls & carry-overs (excludes {abandoned} abandoned >{abandoned_days}d)"
        if abandoned else "Needs attention — flagged, carried-over & stalled active items"
    )
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
        _table_cell_style(reqs, table_id, 0, ci, len(h), bold=True, color=GRAY, size=9.5, font=FONT, align=aligns[ci])

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
        subtitle = f"{reactive_wip_pct}% of WIP is unplanned reactive work"
    elif (planned_wip + unplanned_wip) > 0:
        subtitle = f"{100 - reactive_wip_pct}% of WIP is planned roadmap work"
    else:
        subtitle = "No open work"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Planned vs. Unplanned", subtitle)

    cards_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Planned WIP", str(planned_wip)),
            ("Unplanned WIP", str(unplanned_wip)),
            ("Reactive share (WIP)", f"{reactive_wip_pct}%"),
            ("Reactive share (closed)", f"{reactive_closed_pct}%"),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    charts = report.get("_charts")
    body_top = cards_y + 12
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
        subtitle = f"{len(open_bugs)} open bugs — {len(blocker_crit)} blocker/critical need attention"
    elif open_bugs:
        subtitle = f"{len(open_bugs)} open bugs — no blockers currently active"
    else:
        subtitle = "Backlog clear — no open bugs"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Bug Health", subtitle)

    jira_base = eng.get("base_url", "")
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
    body_top = BODY_Y + 4
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
    blocker_start_y: float | None = None
    if blocker_crit:
        blocker_section_h = 18 + 18 + blocker_rows * _bug_ticket_h + 8
        blocker_start_y = _ENG_CONTENT_BOTTOM - blocker_section_h

    list_bottom_cap = (blocker_start_y - 8) if blocker_start_y is not None else _ENG_CONTENT_BOTTOM
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

    if blocker_crit and blocker_start_y is not None:
        left_y = blocker_start_y
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
    return _truncate_one_line(_display_team_name(team), 12)


_VELOCITY_PIPELINE_LABEL_W = 86.0
_VELOCITY_PIPELINE_ROW_H = 16.0
_PIPELINE_STATUS_SHORT = {
    "In Progress": "In Prog.",
    "In Review": "Review",
    "Selected for Development": "Selected",
    "To Do": "To Do",
}


def _short_pipeline_status(status: str) -> str:
    return _PIPELINE_STATUS_SHORT.get(status, _truncate_one_line(status, 14))


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
        subtitle = f"Backlog growing — net +{net} over {wk_n} weeks"
    elif trend == "shrinking":
        subtitle = f"Backlog shrinking — net {abs(net)} resolved over {wk_n} weeks"
    else:
        subtitle = f"Inflow \u2248 outflow — {created_total} in / {resolved_total} out over {wk_n} weeks"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Bug Inflow vs. Outflow", subtitle)

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
        y=_ENG_KPI_AFTER_CONTEXT_Y,
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

    total_remaining = int(ep.get("total_remaining") or 0)
    early_stage = int(ep.get("early_stage_count") or 0)
    at_risk = int(ep.get("at_risk_count") or 0)

    subtitle = f"{len(epics)} in-flight epics, {total_remaining} issues remaining"
    if at_risk:
        subtitle += f", {at_risk} at risk"
    elif early_stage:
        subtitle += f", {early_stage} early-stage"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Initiative Progress", subtitle)

    top = BODY_Y + 8
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
        owner = str(epic.get("owner") or "")
        project = str(epic.get("project") or "")
        pct = int(epic.get("pct") or 0)
        done = int(epic.get("done") or 0)
        total = int(epic.get("total") or 0)
        active = int(epic.get("active_30d") or 0)
        overdue = bool(epic.get("overdue"))
        stalled = bool(epic.get("stalled"))

        name_chars = max_chars_one_line_for_table_col(name_w, 10.5)
        name = _truncate_one_line(f"{key}  {summary}", name_chars)
        _box(reqs, f"{sid}_en{i}", sid, MARGIN, y0, name_w, 16, name)
        _style(reqs, f"{sid}_en{i}", 0, len(name), size=10.5, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_en{i}", 0, len(key), bold=True, size=10.5, color=BLUE, font=FONT)

        # Completion bar: light track + fill. Amber when at risk, green near done, else blue.
        risk = bool(overdue or stalled)
        fill = AMBER if risk else (GREEN if pct >= 80 else BLUE)
        bar_w = max(3.0, pct / 100.0 * max_bar)
        _rect(reqs, f"{sid}_et{i}", sid, bar_x, y0 + 2, max_bar, 11, _EPIC_TRACK_FILL)
        _rect(reqs, f"{sid}_ef{i}", sid, bar_x, y0 + 2, bar_w, 11, fill)
        pct_txt = f"{pct}%  ({done}/{total})"
        _box(reqs, f"{sid}_ep{i}", sid, bar_x + max_bar + 6, y0, pct_w, 16, pct_txt)
        _style(reqs, f"{sid}_ep{i}", 0, len(pct_txt), bold=True, size=9.5, color=NAVY, font=MONO)

        # Meta line under the name: project · owner · status · activity · risk flags.
        parts: list[str] = []
        if project:
            parts.append(_display_project_code(project))
        if owner:
            parts.append(owner.split()[0] if owner else owner)
        if status:
            parts.append(status)
        parts.append(f"{active} updated/30d")
        flags: list[str] = []
        if overdue:
            flags.append("overdue")
        if stalled:
            flags.append("stalled")
        meta = " · ".join(parts) + (f"  ·  {' · '.join(flags)}" if flags else "")
        _box(reqs, f"{sid}_es{i}", sid, MARGIN + 12, y0 + 16, name_w + max_bar, 13, meta)
        _style(reqs, f"{sid}_es{i}", 0, len(meta), size=8.5, color=GRAY, font=FONT)
        if project:
            disp = _display_project_code(project)
            _style(reqs, f"{sid}_es{i}", 0, len(disp), bold=True, size=8.5, color=NAVY, font=FONT)
        if flags:
            fstart = meta.index(flags[0])
            _style(reqs, f"{sid}_es{i}", fstart, len(meta), bold=True, size=8.5, color=RED, font=FONT)

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
            subtitle = f"{latest:.0f} SP last sprint, above {baseline:.0f} recent average"
        elif baseline and latest <= baseline * 0.95:
            subtitle = f"{latest:.0f} SP last sprint, below {baseline:.0f} recent average"
        else:
            subtitle = f"{latest:.0f} SP last sprint (~{baseline:.0f} avg)" if baseline \
                else f"{latest:.0f} SP delivered last sprint"
        sp_team_names = ", ".join(_display_team_name(t) for t in (velocity.get("teams") or [])) or "scrum boards"
        zero_sp = velocity.get("zero_sp_teams") or []
        no_sp_note = (
            f" · {', '.join(_display_team_name(t) for t in zero_sp)} use ticket throughput"
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
            f"Story points by sprint ({sp_team_names}); line = tickets closed{no_sp_note}."
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
            subtitle = f"Backlog shrinking by ~{net:.0f} tickets per week"
        elif net < -2:
            subtitle = f"Backlog growing by ~{abs(net):.0f} tickets per week"
        else:
            subtitle = f"~{avg_closed:.0f} tickets closed per week on average"
        context = ""

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Velocity", subtitle)

    body_top = BODY_Y + 8 if not has_sp else BODY_Y + 22
    if has_sp:
        _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y + 2, CONTENT_W, 14, context)
        _style(reqs, f"{sid}_bar", 0, len(context), size=9, color=GRAY, font=FONT)
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
    _pipeline_hdr = "Engineering pipeline (by status)"
    _box(reqs, f"{sid}_sbh", sid, right_x, right_y, right_w, 14, _pipeline_hdr)
    _style(reqs, f"{sid}_sbh", 0, len(_pipeline_hdr), bold=True, size=10, color=NAVY, font=FONT)
    right_y += 16
    total_in_flight = sum(by_status.values()) or 1
    max_status = max(by_status.values()) if by_status else 1
    pct_col_w = 30
    label_w = _VELOCITY_PIPELINE_LABEL_W
    bar_x = right_x + label_w + 4
    bar_max_w = max(20.0, right_w - label_w - pct_col_w - 8)
    for status, count in status_items:
        pct = int(count / total_in_flight * 100)
        bar_w = max(3, int(count / max_status * bar_max_w))
        safe_status = status.replace(" ", "_").replace("/", "_")[:10]
        is_active = status in ("In Progress", "In Review")
        bar_color = BLUE if is_active else {"red": 0.75, "green": 0.80, "blue": 0.90}
        status_label = _short_pipeline_status(status)
        label = f"{count}  {status_label}"
        _box(reqs, f"{sid}_sl_{safe_status}", sid, right_x, right_y, label_w, 13, label)
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
        _box(reqs, f"{sid}_sb_{safe_status}", sid, bar_x, right_y + 3, bar_w, 8, "")
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
        right_y += _VELOCITY_PIPELINE_ROW_H

    _eng_takeaway_bar(reqs, sid, report, "velocity")
    return idx + 1


def _fmt_tokens(value: Any) -> str:
    """Compact token count: 3,200,000 → '3.2M', 12,400 → '12K'."""
    try:
        n = int(value or 0)
    except (TypeError, ValueError):
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


def _fmt_cents(value: Any) -> str:
    """Dollar amount from cents (whole dollars): 184029 → '$1,840'."""
    if value is None:
        return "—"
    try:
        return f"${float(value) / 100:,.0f}"
    except (TypeError, ValueError):
        return "—"


def _fmt_cost_per_1k_tokens(cents: Any, tokens: Any) -> str:
    """Model cost in cents per 1K tokens (compact ¢ label for sub-dollar rates)."""
    try:
        c = float(cents or 0)
        toks = int(tokens or 0)
    except (TypeError, ValueError):
        return "—"
    if toks <= 0:
        return "—"
    per_1k = c / toks * 1000.0
    if per_1k < 100:
        return f"{per_1k:.2f}¢"
    return _fmt_cents(per_1k)


def _short_email(email: str, max_chars: int = 22) -> str:
    """Local-part only (drop the domain) for a consistent, compact user column."""
    e = (email or "").strip()
    local = e.split("@", 1)[0] if "@" in e else e
    return _truncate_one_line(local, max_chars)


def _render_cursor_volume_user_list(
    reqs: list[dict[str, Any]],
    sid: str,
    *,
    users: list[dict[str, Any]],
    x: float,
    y: float,
    w: float,
    content_ceiling: float,
    oid_prefix: str,
    limit: int = 4,
    show_models: bool = False,
    row_color: str = NAVY,
) -> float:
    """Compact name + tokens list for the users slide. Returns y below the last row."""
    cur_y = y
    if not users:
        empty = "No usage in window"
        _box(reqs, f"{sid}_{oid_prefix}e", sid, x, cur_y, w, 13, empty)
        _style(reqs, f"{sid}_{oid_prefix}e", 0, len(empty), size=8, color=GRAY, font=FONT)
        return cur_y + 13

    for i, u in enumerate(users[:limit]):
        models = u.get("models") or []
        block_h = 13 + (11 if show_models and models else 0)
        if cur_y + block_h > content_ceiling:
            break
        name = _short_email(str(u.get("email") or ""), 11)
        tok = _fmt_tokens(u.get("tokens"))
        row = f"{name:<11}{tok:>6}"
        _box(reqs, f"{sid}_{oid_prefix}{i}", sid, x, cur_y, w, 13, row)
        _style(reqs, f"{sid}_{oid_prefix}{i}", 0, len(row), size=8, color=row_color, font=MONO)
        cur_y += 13
        if show_models and models:
            top_model = _truncate_one_line(str(models[0].get("model") or ""), 16)
            sub = f"  └ {top_model}"
            _box(reqs, f"{sid}_{oid_prefix}m{i}", sid, x, cur_y, w, 11, sub)
            _style(reqs, f"{sid}_{oid_prefix}m{i}", 0, len(sub), size=7.5, color=GRAY, font=MONO)
            cur_y += 11
    return cur_y


def _eng_share_bar(
    reqs: list[dict[str, Any]],
    sid: str,
    oid: str,
    *,
    label: str,
    value_label: str,
    fraction: float,
    x: float,
    y: float,
    w: float,
    bar_max_w: float,
    color: dict[str, float],
    value_w: float = 34.0,
    value_font_pt: float = 8.0,
    value_color: dict[str, float] | None = None,
) -> None:
    """One labelled horizontal share bar (model mix row).

    *value_w* widens the right-hand value column so multi-digit dollar amounts
    (e.g. ``$2,135``) render on a single line instead of wrapping.
    """
    # Reserve room for the bar and the value column so the label never collides.
    label_w = max(20.0, w - bar_max_w - value_w - 10)
    _box(reqs, f"{sid}_{oid}_l", sid, x, y, label_w, 13, label)
    _style(reqs, f"{sid}_{oid}_l", 0, len(label), size=8.5, color=NAVY, font=FONT)
    bar_w = max(3.0, float(fraction) * bar_max_w)
    bar_x = x + (w - bar_max_w - value_w - 2)
    _box(reqs, f"{sid}_{oid}_b", sid, bar_x, y + 3, bar_w, 8, "")
    reqs.append(
        {
            "updateShapeProperties": {
                "objectId": f"{sid}_{oid}_b",
                "shapeProperties": {
                    "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}},
                    "outline": {
                        "outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                        "weight": {"magnitude": 0.75, "unit": "PT"},
                    },
                },
                "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
            }
        }
    )
    _box(reqs, f"{sid}_{oid}_v", sid, bar_x + bar_max_w + 4, y, value_w, 13, value_label)
    _style(
        reqs, f"{sid}_{oid}_v", 0, len(value_label),
        size=value_font_pt, color=value_color or GRAY, font=FONT,
    )


def _cursor_blob(report: dict[str, Any]) -> dict[str, Any]:
    return report.get("cursor_usage") or {}


def _cursor_bg(reqs: list[dict[str, Any]], sid: str) -> None:
    """Light-blue background shared by all Cursor AI slides."""
    _bg(reqs, sid, CURSOR_BG)


def _github_bg(reqs: list[dict[str, Any]], sid: str) -> None:
    """Light-green background shared by GitHub productivity slides."""
    _bg(reqs, sid, GITHUB_BG)


_GITHUB_REPO_ROW_STEP = 11.0


def _cursor_takeaway(cu: dict[str, Any], focus: str) -> str:
    """Per-slide takeaway, falling back to the legacy single ``takeaway`` field."""
    takeaways = cu.get("takeaways") or {}
    return str(takeaways.get(focus) or cu.get("takeaway") or "").strip()


def _cursor_section_header(reqs: list[dict[str, Any]], sid: str, oid: str, x: float, y: float, w: float, text: str) -> float:
    _box(reqs, f"{sid}_{oid}", sid, x, y, w, 14, text)
    _style(reqs, f"{sid}_{oid}", 0, len(text), bold=True, size=10, color=NAVY, font=FONT)
    return y + 16


_CURSOR_CHART_PAD = 6.0
_CURSOR_CHART_LEGEND_GAP = 4.0
_CURSOR_CHART_TITLE_H = 14.0
_CURSOR_CHART_TITLE_GAP = 2.0
_CURSOR_CHART_LEGEND_ROW_H = 12.0


def _cursor_embed_chart_panel(
    reqs: list[dict[str, Any]],
    *,
    sid: str,
    oid: str,
    x: float,
    y: float,
    w: float,
    chart_h: float,
    spreadsheet_id: str,
    chart_id: int,
    title: str | None = None,
    legend: list[tuple[str, dict[str, float]]] | None = None,
    legend_vertical: bool = False,
    legend_above: bool = False,
    legend_font_pt: float = 9,
    legend_swatch: float = 9,
    legend_entry_gap: float = 14,
) -> float:
    """Embed a Sheets chart in a bordered panel with a slide-level title and legend.

    Chart titles render as native slide text above the plot (``show_title=False`` on the
    Sheets chart) so they stay readable when the embed is scaled — in-chart titles were
    clipped or illegible at presentation size.
    """
    from .charts import embed_chart

    title_block = (_CURSOR_CHART_TITLE_H + _CURSOR_CHART_TITLE_GAP) if title else 0.0
    if legend:
        if legend_vertical:
            legend_h = len(legend) * _CURSOR_CHART_LEGEND_ROW_H
        else:
            legend_h = 16.0
    else:
        legend_h = 0.0
    panel_h = (
        _CURSOR_CHART_PAD * 2 + title_block + chart_h
        + (_CURSOR_CHART_LEGEND_GAP + legend_h if legend else 0)
    )
    _bar_rect(reqs, f"{sid}_{oid}_pnl", sid, x, y, w, panel_h, WHITE, outline=GRAY)
    inner_x = x + _CURSOR_CHART_PAD
    inner_y = y + _CURSOR_CHART_PAD
    inner_w = w - 2 * _CURSOR_CHART_PAD
    if title:
        display = _truncate_one_line(title, max(24, int(inner_w / 5.5)))
        _box(reqs, f"{sid}_{oid}_ttl", sid, inner_x, inner_y, inner_w, _CURSOR_CHART_TITLE_H, display)
        _style(reqs, f"{sid}_{oid}_ttl", 0, len(display), bold=True, size=10, color=NAVY, font=FONT)
        inner_y += title_block
    if legend and legend_above and not legend_vertical:
        _slide_chart_legend(
            reqs, sid, f"{sid}_{oid}lgd", inner_x, inner_y,
            legend, font_pt=legend_font_pt, swatch_size=legend_swatch, entry_gap=legend_entry_gap,
        )
        inner_y += legend_h + _CURSOR_CHART_LEGEND_GAP
    embed_chart(
        reqs, f"{sid}_{oid}", sid, spreadsheet_id, chart_id,
        inner_x, inner_y, inner_w, chart_h, linked=False,
    )
    if legend and not (legend_above and not legend_vertical):
        legend_y = inner_y + chart_h + _CURSOR_CHART_LEGEND_GAP
        if legend_vertical:
            _slide_chart_legend_vertical(
                reqs, sid, f"{sid}_{oid}lgd", inner_x, legend_y, inner_w, legend,
                font_pt=legend_font_pt, swatch_size=legend_swatch, row_h=_CURSOR_CHART_LEGEND_ROW_H,
                max_label_chars=max(18, int(inner_w / 7)),
            )
        else:
            _slide_chart_legend(
                reqs, sid, f"{sid}_{oid}lgd", inner_x, legend_y,
                legend, font_pt=legend_font_pt, swatch_size=legend_swatch, entry_gap=legend_entry_gap,
            )
    return y + panel_h + 8


def _cursor_chart_panel_reserve(
    *,
    content_ceiling: float,
    start_y: float,
    title: bool = True,
    legend_rows: int = 0,
    legend_vertical: bool = False,
) -> float:
    """Compute chart plot height that fits title, legend, and panel padding under *content_ceiling*."""
    title_block = (_CURSOR_CHART_TITLE_H + _CURSOR_CHART_TITLE_GAP) if title else 0.0
    legend_h = (legend_rows * _CURSOR_CHART_LEGEND_ROW_H if legend_vertical else 16.0) if legend_rows else 0.0
    overhead = _CURSOR_CHART_PAD * 2 + title_block + (legend_h + _CURSOR_CHART_LEGEND_GAP if legend_rows else 0) + 14
    return max(100.0, content_ceiling - start_y - overhead)


def cursor_cost_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """AI coding-assistant COST for a VP of Engineering.

    KPI cards: org-wide 30-day run rate and billing-cycle included usage; engineer-scoped
    cost per active engineer and active engineer count. Charts/tables stay dev-* scoped.
    """
    cu = _cursor_blob(report)
    if not cu.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Cursor usage (set CURSOR_ADMIN_API_KEY)")

    cost = cu.get("cost_engineers") or {}
    if not cost.get("configured"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "Engineer-scoped cost (set ATLASSIAN_ORG_ID and dev-* Atlassian teams)",
        )

    org_totals = cu.get("totals") or {}
    eng_totals = cost.get("totals") or {}
    daily = cost.get("daily") or []
    window_days = int(cu.get("window_days") or 30)

    # Org-wide headline KPIs; engineer-scoped denominator for cost/active and active count.
    run_rate_cents = org_totals.get("charged_cents_window")
    included_cents = org_totals.get("included_spend_cents_cycle")
    active = int(cost.get("active_window") or 0)
    eng_window_cents = eng_totals.get("charged_cents_window")
    cost_per_active = (
        (float(eng_window_cents) / active) if (eng_window_cents is not None and active) else None
    )

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, "Cursor AI Coding Spend")

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            (f"30d run rate", _fmt_cents(run_rate_cents)),
            ("Cost / active eng", _fmt_cents(cost_per_active)),
            ("Active engineers", str(active)),
            ("Total included usage", _fmt_cents(included_cents)),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    body_top = kpi_y + 12
    content_x = MARGIN
    content_w = CONTENT_W
    content_ceiling = _ENG_CONTENT_BOTTOM - 6
    charts = report.get("_charts")

    chart_y = body_top
    if daily and charts:
        try:
            chart_title = f"Cost over time (daily $, dev-* engineers, {window_days}d)"
            labels = [str(d.get("label") or d.get("date") or "") for d in daily]
            cost_series = [round(float(d.get("cents") or 0) / 100.0, 2) for d in daily]
            users_series = [int(d.get("active_users") or 0) for d in daily]
            chart_h = int(_cursor_chart_panel_reserve(
                content_ceiling=content_ceiling, start_y=chart_y, legend_rows=2,
            ))
            chart_inner_w = content_w - 2 * _CURSOR_CHART_PAD
            ss_id, chart_id = charts.add_combo_chart(
                title=chart_title,
                labels=labels,
                bar_series={"Daily cost ($)": cost_series},
                line_series={"Active users": users_series},
                show_title=False,
                suppress_legend=True,
                width_pixels=int(chart_inner_w * 2),
                height_pixels=int(chart_h * 2),
            )
            _cursor_embed_chart_panel(
                reqs, sid=sid, oid="cchart", x=content_x, y=chart_y, w=content_w, chart_h=chart_h,
                spreadsheet_id=ss_id, chart_id=chart_id,
                title=chart_title,
                legend=[("Daily cost ($)", BRAND_SERIES_COLORS[0]), ("Active users", BRAND_SERIES_COLORS[1])],
                legend_above=True,
            )
        except Exception as exc:
            logger.warning("Cursor cost chart embed failed: %s", exc)
    else:
        empty = "No usage-based cost in window"
        _box(reqs, f"{sid}_ce", sid, content_x, chart_y, content_w, 14, empty)
        _style(reqs, f"{sid}_ce", 0, len(empty), size=9, color=GRAY, font=FONT)

    _render_takeaway_band(reqs, sid, _cursor_takeaway(cu, "cost"))
    return idx + 1


def cursor_cost_models_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Engineer-scoped Cursor spend ranked by model (native table)."""
    cu = _cursor_blob(report)
    if not cu.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Cursor usage (set CURSOR_ADMIN_API_KEY)")

    cost = cu.get("cost_engineers") or {}
    if not cost.get("configured"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "Engineer-scoped cost (set ATLASSIAN_ORG_ID and dev-* Atlassian teams)",
        )

    model_mix = cost.get("model_mix") or []
    eng_totals = cost.get("totals") or {}
    window_days = int(cu.get("window_days") or 30)
    model_cost = sorted(
        (m for m in model_mix if (m.get("tokens") or 0) > 0 or (m.get("cents") or 0) > 0),
        key=lambda m: float(m.get("tokens") or 0),
        reverse=True,
    )
    kpi_total_cents = float(eng_totals.get("charged_cents_window") or 0)
    mix_total_cents = sum(float(m.get("cents") or 0) for m in model_cost)
    total_cents = kpi_total_cents if kpi_total_cents > 0 else mix_total_cents

    if not model_cost or total_cents <= 0:
        return _missing_data_slide(reqs, sid, report, idx, "Engineer-scoped model cost (no spend in window)")

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, "Cursor AI Spend by Model")

    col_widths_base = _COST_MODEL_TABLE_COL_WIDTHS
    headers = ("Model", "Tokens", f"Spend (last {window_days}d)", "Cost / 1K tok", "% of spend")
    aligns = _COST_MODEL_TABLE_ALIGNS
    row_h = _COST_MODEL_TABLE_ROW_H
    panel_x = MARGIN
    panel_y = BODY_Y
    panel_w = CONTENT_W
    panel_pad = _COST_MODEL_PANEL_PAD
    inner_w = panel_w - 2 * panel_pad
    col_widths = _scale_col_widths(col_widths_base, inner_w)
    table_x = panel_x + panel_pad
    table_top = panel_y + panel_pad
    disclaimer_reserve = _COST_MODEL_DISCLAIMER_H + _COST_MODEL_DISCLAIMER_GAP
    table_bottom = _ENG_CONTENT_BOTTOM - panel_pad - disclaimer_reserve
    max_body_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=table_bottom,
        row_height_pt=row_h,
        reserved_table_rows=1,
        max_rows_cap=20,
    )
    body_rows = _cursor_cost_model_rows(model_cost, total_cents, max_body_rows)
    num_rows = 1 + len(body_rows)
    panel_h = 2 * panel_pad + num_rows * row_h
    _bar_rect(reqs, f"{sid}_cpnl", sid, panel_x, panel_y, panel_w, panel_h, WHITE, outline=GRAY)
    table_id = f"{sid}_ctbl"
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(inner_w, num_rows * row_h),
                "transform": _tf(table_x, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })
    _clean_table(reqs, table_id, num_rows, len(headers))
    _table_column_widths(reqs, table_id, col_widths)

    for ci, head in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, ci, head)
        _table_cell_style(
            reqs, table_id, 0, ci, len(head),
            bold=True, color=GRAY, size=_COST_MODEL_TABLE_HEADER_PT, font=FONT, align=aligns[ci],
        )

    model_chars = max_chars_one_line_for_table_col(col_widths[0], _COST_MODEL_TABLE_BODY_PT)
    for ri, cells in enumerate(body_rows, start=1):
        cells = list(cells)
        cells[0] = _truncate_one_line(cells[0], model_chars)
        for ci, text in enumerate(cells):
            if not text:
                continue
            _table_cell_text(reqs, table_id, ri, ci, text)
            color = NAVY if ci == 0 else NAVY
            font = FONT if ci == 0 else MONO
            _table_cell_style(
                reqs, table_id, ri, ci, len(text),
                bold=False, color=color, size=_COST_MODEL_TABLE_BODY_PT, font=font, align=aligns[ci],
            )

    disclaimer_y = panel_y + panel_h + _COST_MODEL_DISCLAIMER_GAP
    _box(reqs, f"{sid}_cmdisc", sid, MARGIN, disclaimer_y, CONTENT_W, _COST_MODEL_DISCLAIMER_H, _COST_MODEL_DISCLAIMER)
    _style(
        reqs, f"{sid}_cmdisc", 0, len(_COST_MODEL_DISCLAIMER),
        size=11, color=GRAY, font=FONT,
    )

    top_parts = [
        f"{_truncate_one_line(str(m.get('model') or 'unknown'), 24)} {_fmt_tokens(m.get('tokens'))}"
        for m in model_cost[:3]
    ]
    fallback = f"Top by tokens: {'; '.join(top_parts)}." if top_parts else ""
    _render_takeaway_band(reqs, sid, _cursor_takeaway(cu, "cost_models") or fallback)
    return idx + 1


def _cursor_usage_scope(cu: dict[str, Any], audience: str) -> dict[str, Any]:
    if audience == "engineers":
        return cu.get("usage_engineers") or {}
    return cu.get("usage_non_engineers") or {}


def _render_cursor_usage_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    *,
    audience: str,
) -> int:
    cu = _cursor_blob(report)
    if not cu.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Cursor usage (set CURSOR_ADMIN_API_KEY)")

    scope = _cursor_usage_scope(cu, audience)
    if not scope.get("configured"):
        label = "Engineer-scoped" if audience == "engineers" else "Non-engineer"
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"{label} usage (set ATLASSIAN_ORG_ID and dev-* Atlassian teams)",
        )

    totals = scope.get("totals") or {}
    daily = scope.get("daily") or []
    window_days = int(cu.get("window_days") or 30)
    total_tokens = int(totals.get("total_tokens") or 0)
    input_tokens = int(totals.get("input_tokens") or 0)
    output_tokens = int(totals.get("output_tokens") or 0)
    events = int(totals.get("event_count") or 0)

    is_engineers = audience == "engineers"
    title = "Cursor AI Token Usage - Engineering" if is_engineers else "Cursor AI Token Usage — Non-Engineering"
    takeaway_focus = "usage" if is_engineers else "usage_non_engineers"
    scope_label = "dev-* engineers" if is_engineers else "non-engineering users"

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, title)

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            (f"Tokens ({window_days}d)", _fmt_tokens(total_tokens)),
            ("Input tokens", _fmt_tokens(input_tokens)),
            ("Output tokens", _fmt_tokens(output_tokens)),
            ("Requests", _fmt_tokens(events)),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    body_top = kpi_y + 12
    content_x = MARGIN
    content_w = CONTENT_W
    content_ceiling = _ENG_CONTENT_BOTTOM - 6
    charts = report.get("_charts")

    # Single full-width column: the daily tokens-over-time chart now owns the body.
    # The per-model mix moved to the dedicated Cursor Model Usage slide (both audiences).
    chart_y = body_top
    if daily and charts:
        try:
            chart_title = f"Tokens over time ({scope_label}, daily input + output, {window_days}d)"
            labels = [str(d.get("label") or d.get("date") or "") for d in daily]
            in_series = [int(d.get("input_tokens") or 0) for d in daily]
            out_series = [int(d.get("output_tokens") or 0) for d in daily]
            chart_h = int(_cursor_chart_panel_reserve(
                content_ceiling=content_ceiling, start_y=chart_y, legend_rows=2,
            ))
            ss_id, chart_id = charts.add_bar_chart(
                title=chart_title,
                labels=labels,
                series={"Input": in_series, "Output": out_series},
                stacked=True,
                show_title=False,
                suppress_legend=True,
                width_pixels=int(content_w * 2),
                height_pixels=int(chart_h * 2),
            )
            chart_y = _cursor_embed_chart_panel(
                reqs, sid=sid, oid="tchart", x=content_x, y=chart_y, w=content_w, chart_h=chart_h,
                spreadsheet_id=ss_id, chart_id=chart_id,
                title=chart_title,
                legend=[("Input", BRAND_SERIES_COLORS[0]), ("Output", BRAND_SERIES_COLORS[1])],
            )
        except Exception as exc:
            logger.warning("Cursor tokens chart embed failed: %s", exc)
    else:
        empty = "No token usage in window"
        _box(reqs, f"{sid}_te", sid, content_x, chart_y, content_w, 14, empty)
        _style(reqs, f"{sid}_te", 0, len(empty), size=9, color=GRAY, font=FONT)

    _render_takeaway_band(reqs, sid, _cursor_takeaway(cu, takeaway_focus))
    return idx + 1


def cursor_usage_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """AI Token Usage for dev-* Atlassian team members."""
    return _render_cursor_usage_slide(reqs, sid, report, idx, audience="engineers")


def cursor_usage_non_engineers_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """AI Token Usage for non-engineering Cursor users."""
    return _render_cursor_usage_slide(reqs, sid, report, idx, audience="non_engineers")


# Native model-usage table geometry. Columns (sum to CONTENT_W = 624):
# Audience · Model · Tokens · % of tokens · Requests · % of volume. The Model column is wide
# so full model names (e.g. "claude-opus-4-8-thinking-xhigh") are not truncated.
_MODEL_TABLE_COL_WIDTHS: tuple[float, ...] = (92.0, 206.0, 84.0, 80.0, 82.0, 80.0)
_MODEL_TABLE_HEADERS = ("Audience", "Model", "Tokens", "% of tokens", "Requests", "% of volume")
_MODEL_TABLE_ALIGNS = ("START", "START", "END", "END", "END", "END")
_MODEL_TABLE_HEADER_PT = 9.0
_MODEL_TABLE_BODY_PT = 9.5
# Google Slides enforces a minimum table row height (~31pt at this font), so size rows to
# match what the renderer actually produces — otherwise the table grows past its box and
# the last rows fall off the slide.
_MODEL_TABLE_ROW_H = 31.0

# Spend-by-model table (engineer-scoped cost mix). Spend column header is built per slide
# from ``window_days``; base column widths are scaled to the bordered panel inner width.
# Row height matches rendered Slides rows at 8pt (~22pt with cell padding) so we can pack
# more models without the table growing past the disclaimer band.
_COST_MODEL_PANEL_PAD = 2.0
_COST_MODEL_TABLE_ROW_H = 22.0
_COST_MODEL_TABLE_HEADER_PT = 8.0
_COST_MODEL_TABLE_BODY_PT = 8.0
_COST_MODEL_TABLE_COL_WIDTHS: tuple[float, ...] = (210.0, 76.0, 110.0, 84.0, 144.0)
_COST_MODEL_TABLE_ALIGNS = ("START", "END", "END", "END", "END")
_COST_MODEL_DISCLAIMER = (
    "Model spend cannot be correlated with productivity because usage is not attributed on commits."
)
_COST_MODEL_DISCLAIMER_H = 16.0
_COST_MODEL_DISCLAIMER_GAP = 4.0


def _scale_col_widths(base: tuple[float, ...], total: float) -> tuple[float, ...]:
    """Scale column width proportions to *total* pt, fixing rounding on the last column."""
    denom = sum(base) or 1.0
    scaled = [w / denom * total for w in base]
    if len(scaled) > 1:
        scaled[-1] = max(1.0, total - sum(scaled[:-1]))
    return tuple(scaled)


def _cursor_cost_model_rows(
    model_cost: list[dict[str, Any]],
    total_cents: float,
    cap: int,
) -> list[list[str]]:
    """Table rows for spend-by-model, reserving one row for an Other aggregate when truncated."""
    cap = max(1, cap)
    show_other = len(model_cost) > cap
    display_cap = cap - 1 if show_other else cap
    rows: list[list[str]] = []
    for m in model_cost[:display_cap]:
        cents = float(m.get("cents") or 0)
        tokens = m.get("tokens")
        rows.append([
            str(m.get("model") or "unknown"),
            _fmt_tokens(tokens),
            _fmt_cents(cents),
            _fmt_cost_per_1k_tokens(cents, tokens),
            _pct_label(cents, total_cents),
        ])
    if show_other:
        other_models = model_cost[display_cap:]
        other_cents = sum(float(m.get("cents") or 0) for m in other_models)
        other_tokens = sum(int(m.get("tokens") or 0) for m in other_models)
        n = len(other_models)
        label = f"Other ({n} models)" if n != 1 else "Other (1 model)"
        rows.append([
            label,
            _fmt_tokens(other_tokens),
            _fmt_cents(other_cents),
            _fmt_cost_per_1k_tokens(other_cents, other_tokens),
            _pct_label(other_cents, total_cents),
        ])
    return rows


def _pct_label(part: float, whole: float) -> str:
    """Whole-percent share, guarding divide-by-zero (renders '—' when undefined)."""
    if not whole:
        return "—"
    return f"{part / whole * 100:.0f}%"


def _cursor_model_rows(scope: dict[str, Any], audience_label: str, cap: int) -> list[list[str]]:
    """Display rows (one per model) for one audience, sorted by tokens desc.

    Each row is ``[audience, model, tokens, % of tokens, requests, % of volume]`` with the
    audience label only on the first row so the segment reads as a group. Percentages use the
    audience-wide scope totals (model events / total events for volume; tokens / total tokens).
    Returns ``[]`` when the scope has no model data.
    """
    model_mix = scope.get("model_mix") or []
    if not (scope.get("configured") and model_mix):
        return []
    totals = scope.get("totals") or {}
    total_tokens = float(totals.get("total_tokens") or 0) or sum(float(m.get("tokens") or 0) for m in model_mix)
    total_events = float(totals.get("event_count") or 0) or sum(float(m.get("events") or 0) for m in model_mix)
    ranked = sorted(model_mix, key=lambda m: float(m.get("tokens") or 0), reverse=True)[: max(1, cap)]
    rows: list[list[str]] = []
    for i, m in enumerate(ranked):
        tokens = float(m.get("tokens") or 0)
        evs = float(m.get("events") or 0)
        rows.append([
            audience_label if i == 0 else "",
            str(m.get("model") or "unknown"),
            _fmt_tokens(tokens),
            _pct_label(tokens, total_tokens),
            _fmt_tokens(evs),
            _pct_label(evs, total_events),
        ])
    return rows


def cursor_model_usage_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Per-model token & request mix for engineering vs non-engineering, as one native table.

    A single full-width Google Slides table groups models by audience (engineering dev-* teams,
    then everyone else), each model ranked by tokens with both a token share and a request-volume
    share — so a VP can see which models carry the spend and whether request volume tracks token
    volume per audience.
    """
    cu = _cursor_blob(report)
    if not cu.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Cursor usage (set CURSOR_ADMIN_API_KEY)")

    eng = _cursor_usage_scope(cu, "engineers")
    non = _cursor_usage_scope(cu, "non_engineers")
    if not (eng.get("configured") or non.get("configured")):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "Audience-scoped model usage (set ATLASSIAN_ORG_ID and dev-* Atlassian teams)",
        )

    window_days = int(cu.get("window_days") or 30)

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, "Cursor AI Model Usage")

    section_y = _cursor_section_header(
        reqs, sid, "msh", MARGIN, BODY_Y, CONTENT_W,
        f"Model mix by audience ({window_days}d)",
    )

    # One full-width table; no takeaway band, so it uses the full body height. Cap models per
    # audience so header + both groups fit within the real (min-height) row count.
    col_widths = list(_MODEL_TABLE_COL_WIDTHS)
    headers = list(_MODEL_TABLE_HEADERS)
    aligns = list(_MODEL_TABLE_ALIGNS)
    row_h = _MODEL_TABLE_ROW_H
    table_top = section_y + 4
    content_bottom = float(SLIDE_H) - 14.0
    max_rows = max(3, int((content_bottom - table_top) / row_h))

    have_eng = bool(_cursor_model_rows(eng, "Engineering", 1))
    have_non = bool(_cursor_model_rows(non, "Non-engineering", 1))
    groups = (have_eng + have_non) or 1
    per_audience = max(1, min(5, (max_rows - 1) // groups))

    body_rows: list[list[str]] = []
    body_rows += _cursor_model_rows(eng, "Engineering", per_audience)
    body_rows += _cursor_model_rows(non, "Non-engineering", per_audience)
    if not body_rows:
        return _missing_data_slide(
            reqs, sid, report, idx, "Audience-scoped model usage (no model data in window)",
        )
    eng_count = len(_cursor_model_rows(eng, "Engineering", per_audience))

    num_rows = 1 + len(body_rows)
    table_id = f"{sid}_mtbl"
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

    for ci, head in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, ci, head)
        _table_cell_style(
            reqs, table_id, 0, ci, len(head),
            bold=True, color=GRAY, size=_MODEL_TABLE_HEADER_PT, font=FONT, align=aligns[ci],
        )

    model_chars = max_chars_one_line_for_table_col(col_widths[1], _MODEL_TABLE_BODY_PT)
    for ri, cells in enumerate(body_rows, start=1):
        cells = list(cells)
        cells[1] = _truncate_one_line(cells[1], model_chars)
        for ci, text in enumerate(cells):
            # Skip empty cells (e.g. the blank audience column on a group's later rows):
            # styling/aligning a cell with no text is rejected by the Slides API.
            if not text:
                continue
            _table_cell_text(reqs, table_id, ri, ci, text)
            if ci == 0:
                color, font, bold = BLUE, FONT, True
            elif ci == 1:
                color, font, bold = NAVY, FONT, False
            else:
                color, font, bold = NAVY, MONO, False
            _table_cell_style(
                reqs, table_id, ri, ci, len(text),
                bold=bold, color=color, size=_MODEL_TABLE_BODY_PT, font=font, align=aligns[ci],
            )

    # Hairline divider between the engineering and non-engineering groups.
    if eng_count and eng_count < len(body_rows):
        div_y = table_top + (1 + eng_count) * row_h
        _rect(reqs, f"{sid}_mdiv", sid, MARGIN, div_y, sum(col_widths), 1.0, _ENG_DIVIDER_FILL)

    return idx + 1


def _fmt_cents_per_line(cents: Any) -> str:
    """Format a per-accepted-line cost. Sub-dollar values read better in cents."""
    if not isinstance(cents, (int, float)):
        return "—"
    c = float(cents)
    return f"{c:.2f}\u00a2" if c < 100 else _fmt_cents(c)


def cursor_efficiency_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Output vs. cost combo chart for engineering Cursor efficiency."""
    return _render_cursor_efficiency_output_slide(reqs, sid, report, idx)


def cursor_efficiency_engineers_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Most-efficient-engineers ranking for Cursor efficiency."""
    return _render_cursor_efficiency_engineers_slide(reqs, sid, report, idx)


def _render_cursor_efficiency_output_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int,
) -> int:
    """Accepted lines vs. usage cost over time (engineering org)."""
    cu = _cursor_blob(report)
    if not cu.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Cursor usage (set CURSOR_ADMIN_API_KEY)")

    eff = cu.get("efficiency") or {}
    window_days = int(cu.get("window_days") or 30)

    accepted = int(eff.get("accepted_lines") or 0)
    lines_kept = eff.get("lines_kept")
    per_1k = eff.get("accepted_lines_per_1k_tokens")
    cents_per_line = eff.get("cost_per_accepted_line_cents")
    daily = eff.get("daily") or []

    kept_pct = f"{int(round(float(lines_kept) * 100))}%" if lines_kept is not None else "—"
    per_1k_str = f"{per_1k:g}" if isinstance(per_1k, (int, float)) else "—"

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, "Cursor AI Coding Efficiency - Engineering")

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            (f"Lines accepted ({window_days}d)", _fmt_tokens(accepted)),
            ("Lines kept", kept_pct),
            ("Lines / 1K tokens", per_1k_str),
            ("Cost / accepted line", _fmt_cents_per_line(cents_per_line)),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    body_top = kpi_y + 12
    content_x = MARGIN
    content_w = CONTENT_W
    content_ceiling = _ENG_CONTENT_BOTTOM - 6
    charts = report.get("_charts")

    chart_y = body_top
    if daily and charts:
        try:
            chart_title = f"Output vs. cost over time ({window_days}d)"
            labels = [str(d.get("label") or d.get("date") or "") for d in daily]
            lines_series = [int(d.get("accepted_lines") or 0) for d in daily]
            cost_series = [round(float(d.get("cents") or 0) / 100.0, 2) for d in daily]
            chart_h = int(_cursor_chart_panel_reserve(
                content_ceiling=content_ceiling, start_y=chart_y, legend_rows=2,
            ))
            chart_inner_w = content_w - 2 * _CURSOR_CHART_PAD
            ss_id, chart_id = charts.add_combo_chart(
                title=chart_title,
                labels=labels,
                bar_series={"Accepted lines": lines_series},
                line_series={"Cost ($)": cost_series},
                show_title=False,
                suppress_legend=True,
                width_pixels=int(chart_inner_w * 2),
                height_pixels=int(chart_h * 2),
            )
            _cursor_embed_chart_panel(
                reqs, sid=sid, oid="echart", x=content_x, y=chart_y, w=content_w, chart_h=chart_h,
                spreadsheet_id=ss_id, chart_id=chart_id,
                title=chart_title,
                legend=[("Accepted lines", BRAND_SERIES_COLORS[0]), ("Cost ($)", BRAND_SERIES_COLORS[1])],
                legend_above=True,
            )
        except Exception as exc:
            logger.warning("Cursor efficiency chart embed failed: %s", exc)
    else:
        empty = "No accepted-line data in window"
        _box(reqs, f"{sid}_ee", sid, content_x, chart_y, content_w, 14, empty)
        _style(reqs, f"{sid}_ee", 0, len(empty), size=9, color=GRAY, font=FONT)

    _render_takeaway_band(reqs, sid, _cursor_takeaway(cu, "efficiency"))
    return idx + 1


def _cursor_efficiency_engineers_footnote(report: dict[str, Any]) -> str:
    """Scope footnote for the most-efficient-engineers ranking slide."""
    note = "Tab + agent accepted lines vs. model-API cost"
    ai = _ai_productivity_blob(report)
    if ai.get("configured"):
        co = ai.get("company") or {}
        corr = co.get("token_commit_correlation")
        corr_txt = f"{corr:.2f}" if isinstance(corr, (int, float)) else "n/a"
        note += (
            f" · GitHub: {int(co.get('commits') or 0)} commits; "
            f"token↔commit r={corr_txt} (accepted lines ≠ git commits)"
        )
    return note


_EFF_ENGINEERS_ROW_STEP = 13.0
_EFF_ENGINEERS_FOOTNOTE_H = 14.0
_EFF_ENGINEERS_FOOTNOTE_ABOVE_TAKEAWAY = 6.0


def _render_cursor_efficiency_engineers_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int,
) -> int:
    """Rank engineers by accepted lines per 1K tokens."""
    cu = _cursor_blob(report)
    if not cu.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Cursor usage (set CURSOR_ADMIN_API_KEY)")

    eff = cu.get("efficiency") or {}
    top_eff = eff.get("top_efficiency") or []

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, "Cursor - Most Efficient Engineers")

    section_y = _cursor_section_header(
        reqs, sid, "meh", MARGIN, BODY_Y, CONTENT_W,
        "Most efficient engineers (lines / 1K tokens)",
    )
    right_x = MARGIN
    right_w = CONTENT_W
    right_y = section_y + 4
    footnote_y = (
        _ENG_TAKEAWAY_Y - 3.0 - _EFF_ENGINEERS_FOOTNOTE_ABOVE_TAKEAWAY - _EFF_ENGINEERS_FOOTNOTE_H
    )
    bar_ceiling = footnote_y - 4.0

    if top_eff:
        max_ratio = max(float(u.get("lines_per_1k_tokens") or 0) for u in top_eff) or 1.0
        bar_max_w = min(420.0, right_w - 120.0)
        for i, u in enumerate(top_eff):
            if right_y + _EFF_ENGINEERS_ROW_STEP > bar_ceiling:
                break
            ratio = float(u.get("lines_per_1k_tokens") or 0)
            label = _short_email(str(u.get("email") or ""), 22)
            _eng_share_bar(
                reqs, sid, f"ef{i}",
                label=label, value_label=f"{ratio:g}",
                fraction=ratio / max_ratio, x=right_x, y=right_y, w=right_w,
                bar_max_w=bar_max_w, color=BRAND_SERIES_COLORS[i % len(BRAND_SERIES_COLORS)],
                value_w=48.0,
                value_font_pt=9.0,
                value_color=NAVY,
            )
            right_y += _EFF_ENGINEERS_ROW_STEP
        note = _cursor_efficiency_engineers_footnote(report)
        _box(reqs, f"{sid}_efn", sid, right_x, footnote_y, right_w, _EFF_ENGINEERS_FOOTNOTE_H, note)
        _style(reqs, f"{sid}_efn", 0, len(note), size=7.5, color=GRAY, font=FONT)
    else:
        empty = "No per-engineer efficiency data"
        _box(reqs, f"{sid}_efe", sid, right_x, right_y, right_w, 13, empty)
        _style(reqs, f"{sid}_efe", 0, len(empty), size=9, color=GRAY, font=FONT)

    _render_takeaway_band(reqs, sid, _cursor_takeaway(cu, "efficiency_engineers"))
    return idx + 1


def _cursor_users_scope(cu: dict[str, Any], audience: str) -> dict[str, Any]:
    if audience == "engineers":
        return cu.get("users_engineers") or {}
    return cu.get("users_non_engineers") or {}


def _matrix_from_volume_users(
    users: list[dict[str, Any]],
    *,
    user_limit: int = 6,
    model_limit: int = 3,
) -> dict[str, Any]:
    """Stacked-bar matrix from volume-user rows (email + models[])."""
    picked = users[:user_limit]
    emails = [str(u.get("email") or "") for u in picked if u.get("email")]
    if not emails:
        return {"users": [], "models": [], "series": {}}

    user_model_tokens: dict[str, dict[str, int]] = {}
    model_totals: dict[str, int] = {}
    for u in picked:
        email = str(u.get("email") or "")
        if not email:
            continue
        mt = {
            str(m.get("model") or ""): int(m.get("tokens") or 0)
            for m in (u.get("models") or [])
            if m.get("model")
        }
        user_model_tokens[email] = mt
        for model, toks in mt.items():
            model_totals[model] = model_totals.get(model, 0) + toks

    top_models = [
        m for m, _ in sorted(model_totals.items(), key=lambda kv: kv[1], reverse=True)[:model_limit]
    ]
    has_other = len(model_totals) > len(top_models)
    series: dict[str, list[int]] = {m: [] for m in top_models}
    if has_other:
        series["Other"] = []
    for email in emails:
        mt = user_model_tokens.get(email, {})
        for m in top_models:
            series[m].append(int(mt.get(m, 0)))
        if has_other:
            other = sum(t for mm, t in mt.items() if mm not in top_models)
            series["Other"].append(int(other))
    models = top_models + (["Other"] if has_other else [])
    return {"users": emails, "models": models, "series": series}


def _render_cursor_user_model_chart(
    reqs: list[dict[str, Any]],
    sid: str,
    *,
    matrix: dict[str, Any],
    charts: Any,
    x: float,
    y: float,
    w: float,
    content_ceiling: float,
    chart_title: str,
    oid: str,
) -> float:
    """Embed a per-user model stacked bar chart; returns y below the panel."""
    m_users = matrix.get("users") or []
    m_models = matrix.get("models") or []
    m_series = matrix.get("series") or {}
    if not (m_users and m_models and charts):
        empty = "No per-user model data in window"
        _box(reqs, f"{sid}_{oid}e", sid, x, y, w, 14, empty)
        _style(reqs, f"{sid}_{oid}e", 0, len(empty), size=9, color=GRAY, font=FONT)
        return y + 14

    try:
        labels = [_short_email(u, 14) for u in m_users]
        series = {model: [int(v) for v in (m_series.get(model) or [])] for model in m_models}
        legend_entries = [
            (str(m), BRAND_SERIES_COLORS[i % len(BRAND_SERIES_COLORS)])
            for i, m in enumerate(m_models)
        ]
        chart_h = int(_cursor_chart_panel_reserve(
            content_ceiling=content_ceiling, start_y=y,
            legend_rows=len(legend_entries), legend_vertical=True,
        ))
        ss_id, chart_id = charts.add_bar_chart(
            title=chart_title,
            labels=labels,
            series=series,
            stacked=True,
            show_title=False,
            suppress_legend=True,
        )
        return _cursor_embed_chart_panel(
            reqs, sid=sid, oid=oid, x=x, y=y, w=w, chart_h=chart_h,
            spreadsheet_id=ss_id, chart_id=chart_id,
            title=chart_title,
            legend=legend_entries, legend_vertical=True,
            legend_font_pt=8, legend_swatch=8,
        )
    except Exception as exc:
        logger.warning("Cursor user-model chart embed failed: %s", exc)
        empty = "Chart unavailable"
        _box(reqs, f"{sid}_{oid}e", sid, x, y, w, 14, empty)
        _style(reqs, f"{sid}_{oid}e", 0, len(empty), size=9, color=GRAY, font=FONT)
        return y + 14


def _render_cursor_users_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    *,
    audience: str,
    layout: str,
) -> int:
    cu = _cursor_blob(report)
    if not cu.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Cursor usage (set CURSOR_ADMIN_API_KEY)")

    scope = _cursor_users_scope(cu, audience)
    if not scope.get("configured"):
        label = "Engineer-scoped" if audience == "engineers" else "Non-engineer"
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"{label} power users (set ATLASSIAN_ORG_ID and dev-* Atlassian teams)",
        )

    totals = scope.get("totals") or {}
    top_users = scope.get("top_users") or []
    bottom_users = scope.get("bottom_users") or []
    power_matrix = scope.get("user_model_matrix") or {}
    window_days = int(cu.get("window_days") or 30)

    # Audience-scoped slide: seat count / adoption / idle seats are not meaningful (the
    # denominator is a partial, email-matched subset), so focus on who the active users
    # are and how concentrated usage is among them.
    active = int(scope.get("active_window") or 0)
    total_tokens = int(totals.get("total_tokens") or 0)
    top_share = (
        int(round(int(top_users[0].get("tokens") or 0) / total_tokens * 100))
        if top_users and total_tokens else 0
    )
    top3_tokens = sum(int(u.get("tokens") or 0) for u in top_users[:3])
    top3_share = int(round(top3_tokens / total_tokens * 100)) if total_tokens else 0

    is_engineers = audience == "engineers"
    suffix = "" if is_engineers else " — Non-Engineering"
    titles = {
        "volume_lists": f"Cursor AI User Volume{suffix}",
        "power_chart": f"Cursor AI Power Users{suffix}",
        "light_chart": f"Cursor AI Light Usage{suffix}",
    }
    title = titles.get(layout, titles["power_chart"])
    takeaway_focus = "users" if is_engineers else "users_non_engineers"
    active_kpi = "Active engineers" if is_engineers else "Active users"
    scope_label = "dev-* engineers" if is_engineers else "non-engineering users"

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, title)

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            (active_kpi, str(active)),
            ("Tokens (window)", _fmt_tokens(total_tokens)),
            ("Top-user share", f"{top_share}%" if top_share else "—"),
            ("Top-3 share", f"{top3_share}%" if top3_share else "—"),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    body_top = kpi_y + 12
    content_x = MARGIN
    content_w = CONTENT_W
    content_ceiling = _ENG_CONTENT_BOTTOM - 6
    charts = report.get("_charts")
    list_limit = 4

    if layout == "volume_lists":
        list_gap = 20.0
        list_w = (content_w - list_gap) / 2.0
        high_x = content_x
        low_x = content_x + list_w + list_gap
        row_y = _cursor_section_header(
            reqs, sid, "th", high_x, body_top, list_w, f"Highest volume ({window_days}d)",
        )
        _cursor_section_header(reqs, sid, "tl", low_x, body_top, list_w, f"Lowest volume ({window_days}d)")
        _render_cursor_volume_user_list(
            reqs, sid,
            users=top_users, x=high_x, y=row_y, w=list_w, content_ceiling=content_ceiling,
            oid_prefix="tu", limit=list_limit, show_models=True, row_color=NAVY,
        )
        _render_cursor_volume_user_list(
            reqs, sid,
            users=bottom_users, x=low_x, y=row_y, w=list_w, content_ceiling=content_ceiling,
            oid_prefix="lu", limit=list_limit, show_models=False, row_color=GRAY,
        )
        _render_takeaway_band(reqs, sid, _cursor_takeaway(cu, takeaway_focus))
    elif layout == "power_chart":
        chart_title = f"Model usage by power user ({scope_label}, tokens, {window_days}d)"
        _render_cursor_user_model_chart(
            reqs, sid,
            matrix=power_matrix, charts=charts,
            x=content_x, y=body_top, w=content_w, content_ceiling=content_ceiling,
            chart_title=chart_title, oid="pchart",
        )
    elif layout == "light_chart":
        light_matrix = _matrix_from_volume_users(bottom_users)
        chart_title = f"Model usage by light users ({scope_label}, tokens, {window_days}d)"
        _render_cursor_user_model_chart(
            reqs, sid,
            matrix=light_matrix, charts=charts,
            x=content_x, y=body_top, w=content_w, content_ceiling=content_ceiling,
            chart_title=chart_title, oid="lchart",
        )

    return idx + 1


def cursor_users_volume_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Highest/lowest token-volume lists for dev-* engineers."""
    return _render_cursor_users_slide(reqs, sid, report, idx, audience="engineers", layout="volume_lists")


def cursor_users_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Model mix chart for dev-* engineer power users."""
    return _render_cursor_users_slide(reqs, sid, report, idx, audience="engineers", layout="power_chart")


def cursor_users_light_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Model mix chart for dev-* engineer light users."""
    return _render_cursor_users_slide(reqs, sid, report, idx, audience="engineers", layout="light_chart")


def cursor_users_non_engineers_volume_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int,
) -> int:
    """Highest/lowest token-volume lists for non-engineering Cursor users."""
    return _render_cursor_users_slide(
        reqs, sid, report, idx, audience="non_engineers", layout="volume_lists",
    )


def cursor_users_non_engineers_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Model mix chart for non-engineering power users."""
    return _render_cursor_users_slide(reqs, sid, report, idx, audience="non_engineers", layout="power_chart")


def cursor_users_non_engineers_light_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int,
) -> int:
    """Model mix chart for non-engineering light users."""
    return _render_cursor_users_slide(reqs, sid, report, idx, audience="non_engineers", layout="light_chart")


def _github_productivity_blob(report: dict[str, Any]) -> dict[str, Any]:
    return report.get("github_productivity") or {}


def _ai_productivity_blob(report: dict[str, Any]) -> dict[str, Any]:
    return report.get("ai_productivity") or {}


def _productivity_takeaway(blob: dict[str, Any], key: str, fallback: str = "") -> str:
    takeaways = blob.get("takeaways") or {}
    return str(takeaways.get(key) or fallback).strip()


def productivity_summary_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Landing KPIs for engineer-scoped output vs AI token spend."""
    ai = _ai_productivity_blob(report)
    if not ai.get("configured"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "Productivity summary (requires Cursor + GitHub productivity data)",
        )

    company = ai.get("company") or {}
    window_days = int(ai.get("window_days") or 30)
    commits = int(company.get("commits") or 0)
    merged = int(company.get("merged_prs") or 0)
    tokens = int(company.get("total_tokens") or 0)
    cpt = company.get("commits_per_1k_tokens")
    cpt_str = f"{cpt:g}" if isinstance(cpt, (int, float)) else "—"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(
        reqs, sid, "Engineering Productivity Summary",
        f"{commits} commits and {merged} merged PRs in {window_days}d",
    )

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            (f"Commits ({window_days}d)", str(commits)),
            ("Merged PRs", str(merged)),
            ("Tokens (window)", _fmt_tokens(tokens)),
            ("Commits / 1K tokens", cpt_str),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    takeaway = _productivity_takeaway(ai, "productivity_summary", "")
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


def productivity_trend_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Weekly commits, merges, and token spend."""
    ai = _ai_productivity_blob(report)
    if not ai.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Productivity trend (requires correlation data)")

    window_days = int(ai.get("window_days") or 30)
    weekly = ai.get("weekly_trend") or []

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Productivity Trend")

    body_top = BODY_Y + 4
    charts = report.get("_charts")
    if weekly and charts:
        try:
            chart_title = f"Commits, merges, and tokens by week ({window_days}d)"
            labels = [str(w.get("label") or w.get("week") or "") for w in weekly]
            commit_series = [int(w.get("commits") or 0) for w in weekly]
            merge_series = [int(w.get("merged_prs") or 0) for w in weekly]
            token_series = [round(int(w.get("tokens") or 0) / 1000.0, 1) for w in weekly]
            chart_h = int(_cursor_chart_panel_reserve(
                content_ceiling=_ENG_CONTENT_BOTTOM - 6, start_y=body_top, legend_rows=3,
            ))
            ss_id, chart_id = charts.add_combo_chart(
                title=chart_title,
                labels=labels,
                bar_series={"Commits": commit_series, "Merged PRs": merge_series},
                line_series={"Tokens (÷1K)": token_series},
                show_title=False,
                suppress_legend=True,
            )
            _cursor_embed_chart_panel(
                reqs, sid=sid, oid="ptt", x=MARGIN, y=body_top, w=CONTENT_W, chart_h=chart_h,
                spreadsheet_id=ss_id, chart_id=chart_id,
                title=chart_title,
                legend=[
                    ("Commits", BRAND_SERIES_COLORS[0]),
                    ("Merged PRs", BRAND_SERIES_COLORS[1]),
                    ("Tokens (÷1K)", BRAND_SERIES_COLORS[2]),
                ],
            )
        except Exception as exc:
            logger.warning("Productivity trend chart embed failed: %s", exc)
    else:
        empty = "No weekly productivity data in window"
        _box(reqs, f"{sid}_pte", sid, MARGIN, body_top, CONTENT_W, 14, empty)
        _style(reqs, f"{sid}_pte", 0, len(empty), size=9, color=GRAY, font=FONT)

    takeaway = _productivity_takeaway(ai, "productivity_trend", "")
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


_PRODUCTIVITY_COACHING_ROW_STEP = 13.0
_COACHING_TABLE_COL_WIDTHS: tuple[float, ...] = (280.0, 110.0, 110.0, 124.0)
_COACHING_TABLE_ALIGNS = ("START", "END", "END", "END")
_COACHING_TABLE_ROW_H = 22.0
_COACHING_TABLE_HEADER_PT = 9.0
_COACHING_TABLE_BODY_PT = 9.0

_GITHUB_REPO_TABLE_COL_WIDTHS: tuple[float, ...] = (340.0, 140.0, 144.0)
_GITHUB_REPO_TABLE_ALIGNS = ("START", "END", "END")
_GITHUB_REPO_TABLE_ROW_H = 22.0
_GITHUB_REPO_TABLE_HEADER_PT = 9.0
_GITHUB_REPO_TABLE_BODY_PT = 9.0


def productivity_coaching_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """High-token / low-output engineers flagged for coaching."""
    ai = _ai_productivity_blob(report)
    if not ai.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "Productivity coaching (requires correlation data)")

    window_days = int(ai.get("window_days") or 30)
    review = ai.get("review") or []

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(
        reqs, sid, "Coaching Focus",
        f"High token / low output engineers ({window_days}d)",
    )

    panel_x = MARGIN
    panel_y = BODY_Y + 4
    panel_w = CONTENT_W
    panel_pad = 2.0
    inner_w = panel_w - 2 * panel_pad
    col_widths = _scale_col_widths(_COACHING_TABLE_COL_WIDTHS, inner_w)
    table_x = panel_x + panel_pad
    table_top = panel_y + panel_pad
    table_bottom = _ENG_CONTENT_BOTTOM - panel_pad
    max_body_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=table_bottom,
        row_height_pt=_COACHING_TABLE_ROW_H,
        reserved_table_rows=1,
        max_rows_cap=12,
    )
    if not review:
        empty = "No under-yield engineers flagged in this window"
        _box(reqs, f"{sid}_pce", sid, MARGIN, table_top, CONTENT_W, 14, empty)
        _style(reqs, f"{sid}_pce", 0, len(empty), size=10, color=GRAY, font=FONT)
    else:
        headers = ("Engineer", "Tokens", "Commits", "Commits / 1K tokens")
        body_rows: list[list[str]] = []
        for row in review[:max_body_rows]:
            cpt = row.get("commits_per_1k_tokens")
            cpt_str = f"{cpt:g}" if isinstance(cpt, (int, float)) else "—"
            body_rows.append([
                _short_email(str(row.get("email") or ""), 28),
                _fmt_tokens(int(row.get("tokens") or 0)),
                str(int(row.get("commits") or 0)),
                cpt_str,
            ])
        num_rows = 1 + len(body_rows)
        panel_h = 2 * panel_pad + num_rows * _COACHING_TABLE_ROW_H
        _bar_rect(reqs, f"{sid}_pcpnl", sid, panel_x, panel_y, panel_w, panel_h, WHITE, outline=GRAY)
        table_id = f"{sid}_pctbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": sid,
                    "size": _sz(inner_w, num_rows * _COACHING_TABLE_ROW_H),
                    "transform": _tf(table_x, table_top),
                },
                "rows": num_rows,
                "columns": len(headers),
            }
        })
        _clean_table(reqs, table_id, num_rows, len(headers))
        _table_column_widths(reqs, table_id, col_widths)
        for ci, head in enumerate(headers):
            _table_cell_text(reqs, table_id, 0, ci, head)
            _table_cell_style(
                reqs, table_id, 0, ci, len(head),
                bold=True, color=GRAY, size=_COACHING_TABLE_HEADER_PT, font=FONT,
                align=_COACHING_TABLE_ALIGNS[ci],
            )
        for ri, cells in enumerate(body_rows, start=1):
            for ci, text in enumerate(cells):
                _table_cell_text(reqs, table_id, ri, ci, text)
                _table_cell_style(
                    reqs, table_id, ri, ci, len(text),
                    bold=(ci == 0), color=NAVY, size=_COACHING_TABLE_BODY_PT,
                    font=FONT if ci == 0 else MONO, align=_COACHING_TABLE_ALIGNS[ci],
                )

    takeaway = _productivity_takeaway(ai, "productivity_coaching", "")
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


def github_engineering_output_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """GitHub engineering output for dev-* engineers (commits, PRs, lines, repos)."""
    gp = _github_productivity_blob(report)
    if not gp.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "GitHub productivity (set GITHUB_TOKEN and GITHUB_ORG)")

    company = gp.get("company_engineers") or {}
    window_days = int(gp.get("window_days") or 30)
    repos = gp.get("repos_summary") or []
    active_repos = sorted(
        (r for r in repos if int(r.get("commits") or 0) >= 1),
        key=lambda r: int(r.get("commits") or 0),
        reverse=True,
    )
    repos_updated = len(active_repos)

    _slide(reqs, sid, idx)
    _github_bg(reqs, sid)
    _eng_title(
        reqs, sid, "GitHub Engineering Output",
        f"{int(company.get('commits') or 0)} commits across {repos_updated} repos ({window_days}d)",
    )

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            (f"Commits ({window_days}d)", str(int(company.get("commits") or 0))),
            ("Merged PRs", str(int(company.get("merged_prs") or 0))),
            ("Lines added", _fmt_tokens(int(company.get("lines_added") or 0))),
            ("Repos Updated", str(repos_updated)),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    body_top = kpi_y + 10
    panel_x = MARGIN
    panel_y = body_top
    panel_w = CONTENT_W
    panel_pad = 2.0
    inner_w = panel_w - 2 * panel_pad
    col_widths = _scale_col_widths(_GITHUB_REPO_TABLE_COL_WIDTHS, inner_w)
    table_x = panel_x + panel_pad
    table_top = panel_y + panel_pad
    table_bottom = _ENG_CONTENT_BOTTOM - panel_pad
    max_body_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=table_bottom,
        row_height_pt=_GITHUB_REPO_TABLE_ROW_H,
        reserved_table_rows=1,
        max_rows_cap=14,
    )
    display_repos = active_repos[:max_body_rows]
    if not display_repos:
        empty = f"No repositories with commits in the last {window_days}d"
        _box(reqs, f"{sid}_gre", sid, MARGIN, table_top, CONTENT_W, 14, empty)
        _style(reqs, f"{sid}_gre", 0, len(empty), size=10, color=GRAY, font=FONT)
    else:
        headers = ("Repository", "Commits", "Merged PRs")
        body_rows = [
            [
                _github_repo_display_name(str(row.get("full_name") or "")),
                str(int(row.get("commits") or 0)),
                str(int(row.get("merged_prs") or 0)),
            ]
            for row in display_repos
        ]
        num_rows = 1 + len(body_rows)
        panel_h = 2 * panel_pad + num_rows * _GITHUB_REPO_TABLE_ROW_H
        _bar_rect(reqs, f"{sid}_grpnl", sid, panel_x, panel_y, panel_w, panel_h, WHITE, outline=GRAY)
        table_id = f"{sid}_grtbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": sid,
                    "size": _sz(inner_w, num_rows * _GITHUB_REPO_TABLE_ROW_H),
                    "transform": _tf(table_x, table_top),
                },
                "rows": num_rows,
                "columns": len(headers),
            }
        })
        _clean_table(reqs, table_id, num_rows, len(headers))
        _table_column_widths(reqs, table_id, col_widths)
        for ci, head in enumerate(headers):
            _table_cell_text(reqs, table_id, 0, ci, head)
            _table_cell_style(
                reqs, table_id, 0, ci, len(head),
                bold=True, color=GRAY, size=_GITHUB_REPO_TABLE_HEADER_PT, font=FONT,
                align=_GITHUB_REPO_TABLE_ALIGNS[ci],
            )
        repo_chars = max_chars_one_line_for_table_col(col_widths[0], _GITHUB_REPO_TABLE_BODY_PT)
        for ri, cells in enumerate(body_rows, start=1):
            cells = list(cells)
            cells[0] = _truncate_one_line(cells[0], repo_chars)
            for ci, text in enumerate(cells):
                _table_cell_text(reqs, table_id, ri, ci, text)
                _table_cell_style(
                    reqs, table_id, ri, ci, len(text),
                    bold=(ci == 0), color=NAVY, size=_GITHUB_REPO_TABLE_BODY_PT,
                    font=FONT, align=_GITHUB_REPO_TABLE_ALIGNS[ci],
                )

    takeaway = _productivity_takeaway(
        gp,
        "github_output",
        f"{int(company.get('commits') or 0)} commits across {repos_updated} repos in {window_days}d.",
    )
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


def _fmt_pr_cycle_hours(hours: Any) -> str:
    if not isinstance(hours, (int, float)):
        return "—"
    h = float(hours)
    return f"{h:.0f}h" if h < 48 else f"{h / 24:.1f}d"


_GITHUB_CONTRIB_ROW_STEP = 13.0


def github_engineer_contribution_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Rank dev-* engineers by merged PRs and commits."""
    gp = _github_productivity_blob(report)
    if not gp.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "GitHub productivity (set GITHUB_TOKEN and GITHUB_ORG)")

    company = gp.get("company_engineers") or {}
    contributors = gp.get("top_contributors") or []
    window_days = int(gp.get("window_days") or 30)

    _slide(reqs, sid, idx)
    _github_bg(reqs, sid)
    _eng_title(reqs, sid, "GitHub Engineer Contribution")

    total_prs = int(company.get("merged_prs") or 0)
    top3_prs = sum(int(r.get("merged_prs") or 0) for r in contributors[:3])
    top_share = f"{top3_prs / total_prs * 100:.0f}%" if total_prs else "—"

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Contributors", str(int(company.get("contributor_count") or len(contributors)))),
            ("Merged PRs", str(total_prs)),
            ("Top 3 share", top_share),
            ("Median PR cycle", _fmt_pr_cycle_hours(company.get("median_pr_cycle_hours"))),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    section_y = _cursor_section_header(
        reqs, sid, "gce", MARGIN, kpi_y + 12, CONTENT_W,
        f"Engineers ranked by merged PRs ({window_days}d)",
    )
    y = section_y + 4
    bar_ceiling = _ENG_CONTENT_BOTTOM - 4
    rank_key = "merged_prs"
    if contributors and not any(int(r.get("merged_prs") or 0) for r in contributors):
        rank_key = "commits"
    max_val = max(
        (float(r.get(rank_key) or 0) for r in contributors),
        default=1.0,
    ) or 1.0
    bar_max_w = min(420.0, CONTENT_W - 120.0)

    for i, row in enumerate(contributors):
        if y + _GITHUB_CONTRIB_ROW_STEP > bar_ceiling:
            break
        metric = float(row.get(rank_key) or 0)
        label = _short_email(str(row.get("email") or ""), 22)
        suffix = " PRs" if rank_key == "merged_prs" else " commits"
        _eng_share_bar(
            reqs, sid, f"gc{i}",
            label=label,
            value_label=f"{int(metric)}{suffix}",
            fraction=metric / max_val,
            x=MARGIN, y=y, w=CONTENT_W,
            bar_max_w=bar_max_w,
            color=BRAND_SERIES_COLORS[i % len(BRAND_SERIES_COLORS)],
            value_w=72.0,
            value_font_pt=9.0,
            value_color=NAVY,
        )
        y += _GITHUB_CONTRIB_ROW_STEP

    takeaway = _productivity_takeaway(
        gp,
        "github_contribution",
        f"{len(contributors)} active contributors; top 3 account for {top_share} of merged PRs.",
    )
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


def github_delivery_flow_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Weekly commits vs merged PRs and review-queue KPIs."""
    gp = _github_productivity_blob(report)
    if not gp.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "GitHub productivity (set GITHUB_TOKEN and GITHUB_ORG)")

    company = gp.get("company_engineers") or {}
    org = gp.get("company_all") or {}
    weekly = gp.get("weekly") or []
    window_days = int(gp.get("window_days") or 30)

    _slide(reqs, sid, idx)
    _github_bg(reqs, sid)
    _eng_title(reqs, sid, "GitHub Delivery Flow")

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Open PRs", str(int(org.get("open_prs") or 0))),
            (f"Merged PRs ({window_days}d)", str(int(company.get("merged_prs") or 0))),
            ("Median PR cycle", _fmt_pr_cycle_hours(company.get("median_pr_cycle_hours"))),
            (f"Releases ({window_days}d)", str(int(org.get("releases") or 0))),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    body_top = kpi_y + 12
    charts = report.get("_charts")
    if weekly and charts:
        try:
            chart_title = f"Commits vs merged PRs by week ({window_days}d, engineering team)"
            labels = [str(w.get("label") or w.get("week") or "") for w in weekly]
            commit_series = [int(w.get("engineer_commits") or 0) for w in weekly]
            pr_series = [int(w.get("engineer_merged_prs") or 0) for w in weekly]
            chart_h = int(_cursor_chart_panel_reserve(
                content_ceiling=_ENG_CONTENT_BOTTOM - 6, start_y=body_top, legend_rows=2,
            ))
            ss_id, chart_id = charts.add_combo_chart(
                title=chart_title,
                labels=labels,
                bar_series={"Commits": commit_series},
                line_series={"Merged PRs": pr_series},
                show_title=False,
                suppress_legend=True,
                width_pixels=int((CONTENT_W - 2 * _CURSOR_CHART_PAD) * 2),
                height_pixels=int(chart_h * 2),
            )
            _cursor_embed_chart_panel(
                reqs, sid=sid, oid="gdf", x=MARGIN, y=body_top, w=CONTENT_W, chart_h=chart_h,
                spreadsheet_id=ss_id, chart_id=chart_id,
                title=chart_title,
                legend=[("Commits", BRAND_SERIES_COLORS[0]), ("Merged PRs", BRAND_SERIES_COLORS[1])],
                legend_above=True,
            )
        except Exception as exc:
            logger.warning("GitHub delivery flow chart embed failed: %s", exc)
    else:
        empty = "No weekly GitHub activity in window"
        _box(reqs, f"{sid}_gde", sid, MARGIN, body_top, CONTENT_W, 14, empty)
        _style(reqs, f"{sid}_gde", 0, len(empty), size=9, color=GRAY, font=FONT)

    takeaway = _productivity_takeaway(
        gp,
        "github_delivery",
        (gp.get("delivery_insights") or {}).get("takeaway")
        or "Compare weekly commits to merged PRs to spot review bottlenecks and WIP buildup.",
    )
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


_GITHUB_CHANGE_PANEL_PAD = 2.0
_GITHUB_CHANGE_TABLE_ROW_H = 22.0
_GITHUB_CHANGE_TABLE_HEADER_PT = 8.0
_GITHUB_CHANGE_TABLE_BODY_PT = 8.0
_GITHUB_CHANGE_TABLE_COL_WIDTHS: tuple[float, ...] = (188.0, 72.0, 72.0, 72.0, 64.0, 72.0)
_GITHUB_CHANGE_TABLE_ALIGNS = ("START", "END", "END", "END", "END", "END")


def _github_repo_display_name(full_name: str) -> str:
    """Repo short name without org prefix for change-profile tables."""
    name = str(full_name or "").strip()
    if "/" in name:
        return name.split("/", 1)[1]
    return name


def _github_change_profile_rows(repos: list[dict[str, Any]], cap: int) -> list[list[str]]:
    active = sorted(
        (
            r for r in repos
            if int(r.get("commits") or 0) >= 1
            or int(r.get("lines_added") or 0) > 0
            or int(r.get("lines_deleted") or 0) > 0
        ),
        key=lambda r: int(r.get("lines_added") or 0),
        reverse=True,
    )
    cap = max(1, cap)
    show_other = len(active) > cap
    display_cap = cap - 1 if show_other else cap
    rows: list[list[str]] = []
    for repo in active[:display_cap]:
        adds = int(repo.get("lines_added") or 0)
        dels = int(repo.get("lines_deleted") or 0)
        net = adds - dels
        del_pct = f"{dels / adds * 100:.0f}%" if adds else "—"
        short_name = _github_repo_display_name(str(repo.get("full_name") or ""))
        rows.append([
            short_name,
            _fmt_tokens(adds),
            _fmt_tokens(dels),
            _fmt_tokens(net),
            del_pct,
            str(int(repo.get("merged_prs") or 0)),
        ])
    if show_other:
        other = active[display_cap:]
        o_adds = sum(int(r.get("lines_added") or 0) for r in other)
        o_dels = sum(int(r.get("lines_deleted") or 0) for r in other)
        o_net = o_adds - o_dels
        o_del_pct = f"{o_dels / o_adds * 100:.0f}%" if o_adds else "—"
        o_prs = sum(int(r.get("merged_prs") or 0) for r in other)
        n = len(other)
        label = f"Other ({n} repos)" if n != 1 else "Other (1 repo)"
        rows.append([label, _fmt_tokens(o_adds), _fmt_tokens(o_dels), _fmt_tokens(o_net), o_del_pct, str(o_prs)])
    return rows


def github_change_profile_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Lines added/deleted by repo — change surface and churn signal."""
    gp = _github_productivity_blob(report)
    if not gp.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "GitHub productivity (set GITHUB_TOKEN and GITHUB_ORG)")

    company = gp.get("company_engineers") or {}
    repos = gp.get("repos_summary") or []
    window_days = int(gp.get("window_days") or 30)
    lines_added = int(company.get("lines_added") or 0)
    lines_deleted = int(company.get("lines_deleted") or 0)
    lines_net = lines_added - lines_deleted
    del_ratio = f"{lines_deleted / lines_added * 100:.0f}%" if lines_added else "—"

    _slide(reqs, sid, idx)
    _github_bg(reqs, sid)
    _eng_title(reqs, sid, "GitHub Change Profile")

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Lines added", _fmt_tokens(lines_added)),
            ("Lines deleted", _fmt_tokens(lines_deleted)),
            ("Net lines", _fmt_tokens(lines_net)),
            ("Delete ratio", del_ratio),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    col_widths_base = _GITHUB_CHANGE_TABLE_COL_WIDTHS
    headers = ("Repository", "+Lines", "−Lines", "Net", "Del %", "PRs")
    aligns = _GITHUB_CHANGE_TABLE_ALIGNS
    row_h = _GITHUB_CHANGE_TABLE_ROW_H
    panel_x = MARGIN
    panel_y = kpi_y + 12
    panel_w = CONTENT_W
    panel_pad = _GITHUB_CHANGE_PANEL_PAD
    inner_w = panel_w - 2 * panel_pad
    col_widths = _scale_col_widths(col_widths_base, inner_w)
    table_x = panel_x + panel_pad
    table_top = panel_y + panel_pad
    table_bottom = _ENG_CONTENT_BOTTOM - panel_pad
    max_body_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=table_bottom,
        row_height_pt=row_h,
        reserved_table_rows=1,
        max_rows_cap=20,
    )
    body_rows = _github_change_profile_rows(repos, max_body_rows)
    if not body_rows:
        return _missing_data_slide(reqs, sid, report, idx, "GitHub change profile (no line activity in window)")

    num_rows = 1 + len(body_rows)
    panel_h = 2 * panel_pad + num_rows * row_h
    _bar_rect(reqs, f"{sid}_gcpnl", sid, panel_x, panel_y, panel_w, panel_h, WHITE, outline=GRAY)
    table_id = f"{sid}_gctbl"
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(inner_w, num_rows * row_h),
                "transform": _tf(table_x, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })
    _clean_table(reqs, table_id, num_rows, len(headers))
    _table_column_widths(reqs, table_id, col_widths)

    for ci, head in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, ci, head)
        _table_cell_style(
            reqs, table_id, 0, ci, len(head),
            bold=True, color=GRAY, size=_GITHUB_CHANGE_TABLE_HEADER_PT, font=FONT, align=aligns[ci],
        )

    repo_chars = max_chars_one_line_for_table_col(col_widths[0], _GITHUB_CHANGE_TABLE_BODY_PT)
    for ri, cells in enumerate(body_rows, start=1):
        cells = list(cells)
        cells[0] = _truncate_one_line(cells[0], repo_chars)
        for ci, text in enumerate(cells):
            if not text:
                continue
            _table_cell_text(reqs, table_id, ri, ci, text)
            font = FONT if ci == 0 else MONO
            _table_cell_style(
                reqs, table_id, ri, ci, len(text),
                bold=False, color=NAVY, size=_GITHUB_CHANGE_TABLE_BODY_PT, font=font, align=aligns[ci],
            )

    takeaway = _productivity_takeaway(
        gp,
        "github_change",
        f"{_fmt_tokens(lines_added)} added / {_fmt_tokens(lines_deleted)} deleted across active repos ({window_days}d).",
    )
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


def ai_output_correlation_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Company-level Cursor token spend vs GitHub output correlation."""
    ai = _ai_productivity_blob(report)
    if not ai.get("configured"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "AI productivity correlation (requires Cursor + GitHub productivity data)",
        )

    company = ai.get("company") or {}
    window_days = int(ai.get("window_days") or 30)
    weekly = ai.get("weekly_trend") or []

    corr = company.get("token_commit_correlation")
    corr_str = f"{corr:.2f}" if isinstance(corr, (int, float)) else "—"
    tpc = company.get("tokens_per_commit")
    cpm = company.get("cents_per_merged_pr")
    cpt = company.get("commits_per_1k_tokens")

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, "AI Spend vs. GitHub Output")

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            ("Tokens / commit", f"{tpc:g}" if isinstance(tpc, (int, float)) else "—"),
            ("Cost / merged PR", _fmt_cents(cpm) if cpm is not None else "—"),
            ("Commits / 1K tokens", f"{cpt:g}" if isinstance(cpt, (int, float)) else "—"),
            ("Token↔commit r", corr_str),
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
    )

    body_top = kpi_y + 12
    charts = report.get("_charts")
    if weekly and charts:
        try:
            chart_title = f"Tokens vs commits by week ({window_days}d)"
            labels = [str(w.get("label") or w.get("week") or "") for w in weekly]
            token_series = [int(w.get("tokens") or 0) for w in weekly]
            commit_series = [int(w.get("commits") or 0) for w in weekly]
            chart_h = int(_cursor_chart_panel_reserve(
                content_ceiling=_ENG_CONTENT_BOTTOM - 6, start_y=body_top, legend_rows=2,
            ))
            ss_id, chart_id = charts.add_combo_chart(
                title=chart_title,
                labels=labels,
                bar_series={"Commits": commit_series},
                line_series={"Tokens (÷1K)": [round(t / 1000.0, 1) for t in token_series]},
                show_title=False,
                suppress_legend=True,
                axis_font_size=_SPRINT_SNAPSHOT_CHART_AXIS_PT,
            )
            _cursor_embed_chart_panel(
                reqs, sid=sid, oid="aich", x=MARGIN, y=body_top, w=CONTENT_W, chart_h=chart_h,
                spreadsheet_id=ss_id, chart_id=chart_id,
                title=chart_title,
                legend=[("Commits", BRAND_SERIES_COLORS[0]), ("Tokens (÷1K)", BRAND_SERIES_COLORS[1])],
            )
        except Exception as exc:
            logger.warning("AI correlation chart embed failed: %s", exc)

    takeaway = _productivity_takeaway(
        ai,
        "correlation",
        f"Engineer-scoped: {int(company.get('commits') or 0)} commits vs "
        f"{_fmt_tokens(int(company.get('total_tokens') or 0))} tokens; correlation is diagnostic, not causal.",
    )
    _render_takeaway_band(reqs, sid, takeaway)
    return idx + 1


_AI_MATRIX_PANEL_PAD = 2.0
_AI_MATRIX_TABLE_ROW_H = 20.0
_AI_MATRIX_TABLE_FIT_ROW_H = 25.0
_AI_MATRIX_TABLE_HEADER_PT = 8.0
_AI_MATRIX_TABLE_BODY_PT = 8.0
_AI_MATRIX_TABLE_COL_WIDTHS: tuple[float, ...] = (168.0, 68.0, 56.0, 48.0, 88.0, 96.0)
_AI_MATRIX_TABLE_ALIGNS = ("START", "END", "END", "END", "END", "START")
_AI_MATRIX_QUADRANT_KPI: tuple[tuple[str, str], ...] = (
    ("high_tokens_high_output", "High token / high out"),
    ("high_tokens_low_output", "High token / low out"),
    ("low_tokens_high_output", "Low token / high out"),
    ("low_tokens_low_output", "Low token / low out"),
)
_AI_MATRIX_QUADRANT_CELL = {
    "high_tokens_high_output": "High / high",
    "high_tokens_low_output": "High / low",
    "low_tokens_high_output": "Low / high",
    "low_tokens_low_output": "Low / low",
}


def _ai_matrix_table_rows(ai: dict[str, Any], cap: int) -> list[list[str]]:
    """Ranked engineer rows for the productivity matrix table."""
    from .ai_productivity_correlation import _MIN_TOKENS_FOR_RANK, _quadrant

    medians = ai.get("medians") or {}
    med_tokens = float(medians.get("tokens") or 0.0)
    med_commits = float(medians.get("commits") or 0.0)
    ranked = [
        row
        for row in (ai.get("individuals") or [])
        if int(row.get("tokens") or 0) >= _MIN_TOKENS_FOR_RANK
    ]
    ranked.sort(
        key=lambda r: (float(r.get("commits_per_1k_tokens") or 0), int(r.get("commits") or 0)),
        reverse=True,
    )
    cap = max(1, cap)
    body: list[list[str]] = []
    for row in ranked[:cap]:
        tokens_n = int(row.get("tokens") or 0)
        commits_n = int(row.get("commits") or 0)
        q_key = _quadrant(tokens_n, commits_n, med_tokens=med_tokens, med_commits=med_commits)
        cpt = row.get("commits_per_1k_tokens")
        cpt_str = f"{cpt:g}" if isinstance(cpt, (int, float)) else "—"
        body.append([
            _short_email(str(row.get("email") or ""), 24),
            _fmt_tokens(tokens_n),
            str(commits_n),
            str(int(row.get("merged_prs") or 0)),
            cpt_str,
            _AI_MATRIX_QUADRANT_CELL.get(q_key, "—"),
        ])
    return body


def ai_productivity_matrix_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Per-engineer yield table and quadrant summary (tokens × GitHub output)."""
    ai = _ai_productivity_blob(report)
    if not ai.get("configured"):
        return _missing_data_slide(reqs, sid, report, idx, "AI productivity matrix (requires correlation data)")

    window_days = int(ai.get("window_days") or 30)
    counts = ai.get("quadrant_counts") or {}

    _slide(reqs, sid, idx)
    _cursor_bg(reqs, sid)
    _eng_title(reqs, sid, "AI Productivity Matrix")

    kpi_y = _eng_kpi_row(
        reqs, sid,
        [
            (label, str(int(counts.get(key) or 0)))
            for key, label in _AI_MATRIX_QUADRANT_KPI
        ],
        y=_ENG_KPI_AFTER_TITLE_Y,
        h=48.0,
    )

    headers = ("Engineer", "Tokens", "Commits", "PRs", "Commits / 1K tokens", "Quadrant")
    aligns = _AI_MATRIX_TABLE_ALIGNS
    row_h = _AI_MATRIX_TABLE_FIT_ROW_H
    panel_x = MARGIN
    panel_y = kpi_y + 10
    panel_w = CONTENT_W
    panel_pad = _AI_MATRIX_PANEL_PAD
    inner_w = panel_w - 2 * panel_pad
    col_widths = _scale_col_widths(_AI_MATRIX_TABLE_COL_WIDTHS, inner_w)
    table_x = panel_x + panel_pad
    table_top = panel_y + panel_pad
    table_bottom = _ENG_TAKEAWAY_Y - 18
    max_body_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=table_bottom,
        row_height_pt=row_h,
        reserved_table_rows=1,
        max_rows_cap=10,
    )
    body_rows = _ai_matrix_table_rows(ai, max_body_rows)
    if not body_rows:
        return _missing_data_slide(reqs, sid, report, idx, "AI productivity matrix (no engineer matches in window)")

    num_rows = 1 + len(body_rows)
    panel_h = 2 * panel_pad + num_rows * row_h
    _bar_rect(reqs, f"{sid}_aimpnl", sid, panel_x, panel_y, panel_w, panel_h, WHITE, outline=GRAY)
    table_id = f"{sid}_aimtbl"
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(inner_w, num_rows * row_h),
                "transform": _tf(table_x, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })
    _clean_table(reqs, table_id, num_rows, len(headers))
    _table_column_widths(reqs, table_id, col_widths)

    for ci, head in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, ci, head)
        _table_cell_style(
            reqs, table_id, 0, ci, len(head),
            bold=True, color=GRAY, size=_AI_MATRIX_TABLE_HEADER_PT, font=FONT, align=aligns[ci],
        )

    engineer_chars = max_chars_one_line_for_table_col(col_widths[0], _AI_MATRIX_TABLE_BODY_PT)
    for ri, cells in enumerate(body_rows, start=1):
        cells = list(cells)
        cells[0] = _truncate_one_line(cells[0], engineer_chars)
        for ci, text in enumerate(cells):
            if not text:
                continue
            _table_cell_text(reqs, table_id, ri, ci, text)
            font = FONT if ci in (0, 5) else MONO
            _table_cell_style(
                reqs, table_id, ri, ci, len(text),
                bold=False, color=NAVY, size=_AI_MATRIX_TABLE_BODY_PT, font=font, align=aligns[ci],
            )

    ranked_count = len([
        row for row in (ai.get("individuals") or [])
        if int(row.get("tokens") or 0) >= 1000
    ])
    omitted = ranked_count - len(body_rows)
    takeaway = _productivity_takeaway(
        ai,
        "matrix",
        (
            f"Ranked by commits per 1K tokens ({window_days}d, dev-* engineers). "
            f"Quadrants split on median token usage and commits."
            + (f" {omitted} more engineer(s) not shown." if omitted > 0 else "")
        ),
    )
    _render_takeaway_band(reqs, sid, takeaway)
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
        subtitle = "1 escalation from support"
    elif total:
        subtitle = f"{total:,} escalations from support"
    else:
        subtitle = "No ticket data available"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _eng_title(reqs, sid, "Support Pressure", subtitle)

    body_top = BODY_Y + 4
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
