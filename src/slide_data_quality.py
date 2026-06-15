"""Data Sources & Quality slide — governance, lineage, and cross-check findings."""

from __future__ import annotations

from typing import Any

from .slide_metadata import ordered_dq_data_sources_for_slide_plan
from .slide_primitives import (
    background as _bg,
    pill as _pill,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, LIGHT, MARGIN, NAVY, WHITE, _cap_chunk_list

_GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
_AMBER = {"red": 0.9, "green": 0.65, "blue": 0.0}
_RED = {"red": 0.85, "green": 0.15, "blue": 0.15}

_SEV_COLOR = {"ERROR": _RED, "WARNING": _AMBER, "INFO": GRAY}
_SEV_DOT = {"ERROR": "\u2716", "WARNING": "\u26a0", "INFO": "\u2139"}

_PILL_W = 118.0
_PILL_H = 20.0
_PILL_GAP = 6.0
_SECTION_PT = 9.0
_BODY_PT = 8.0
_LINE_H = 11.0


def _status_icon_color(status: str) -> tuple[str, dict[str, float]]:
    s = (status or "ok").lower()
    if s == "ok":
        return "\u2713", _GREEN
    if s in ("error", "unavailable"):
        return "\u2717", _RED
    if s in ("unconfigured", "omitted"):
        return "\u2014", GRAY
    return "\u2717", _AMBER


def _truncate(text: str, limit: int) -> str:
    t = (text or "").strip()
    if len(t) <= limit:
        return t
    return t[: limit - 3] + "..."


def _render_section_header(reqs: list[dict], page_sid: str, oid: str, y: float, text: str) -> float:
    box_id = f"{page_sid}_{oid}"
    _box(reqs, box_id, page_sid, MARGIN, y, CONTENT_W, 12, text)
    _style(reqs, box_id, 0, len(text), bold=True, size=_SECTION_PT, color=NAVY, font=FONT)
    return y + 14


def _render_bullet_lines(
    reqs: list[dict],
    page_sid: str,
    oid_prefix: str,
    y: float,
    lines: list[str],
    *,
    color: dict[str, float] = NAVY,
    size: float = _BODY_PT,
) -> float:
    for i, line in enumerate(lines):
        text = _truncate(line, 120)
        oid = f"{page_sid}_{oid_prefix}{i}"
        _box(reqs, oid, page_sid, MARGIN + 4, y, CONTENT_W - 4, _LINE_H, f"· {text}")
        _style(reqs, oid, 0, len(text) + 2, size=size, color=color, font=FONT)
        y += _LINE_H
    return y


def _render_source_pills(
    reqs: list[dict],
    page_sid: str,
    sources: dict[str, str],
    y: float,
) -> float:
    if not sources:
        return y
    x = MARGIN
    row_y = y
    max_x = MARGIN + CONTENT_W
    for idx, (name, status) in enumerate(sources.items()):
        icon, color = _status_icon_color(status)
        label = _truncate(f"{icon} {name}", 22)
        if x + _PILL_W > max_x and x > MARGIN:
            x = MARGIN
            row_y += _PILL_H + _PILL_GAP
        _pill(reqs, f"{page_sid}_src{idx}", page_sid, x, row_y, _PILL_W, _PILL_H, label, WHITE, color)
        x += _PILL_W + _PILL_GAP
    return row_y + _PILL_H + 8


def _render_flag_row(page_sid: str, flag_index: int, flag: dict, y_pos: float, reqs: list[dict]) -> None:
    severity = flag.get("severity") or "WARNING"
    dot = _SEV_DOT.get(severity, "?")
    dot_color = _SEV_COLOR.get(severity, GRAY)
    msg = flag.get("message") or ""
    detail_parts = []
    if flag.get("expected") is not None and flag.get("actual") is not None:
        detail_parts.append(f"expected {flag['expected']}, got {flag['actual']}")
    if flag.get("sources"):
        detail_parts.append(" vs ".join(flag["sources"]))
    line = f"{dot}  {msg}"
    detail = ""
    if detail_parts:
        detail = f"    {' · '.join(detail_parts)}"
    full = line + detail
    if len(full) > 120:
        full = full[:117] + "..."
    object_id = f"{page_sid}_f{flag_index}"
    _box(reqs, object_id, page_sid, MARGIN, y_pos, CONTENT_W, 18, full)
    _style(reqs, object_id, 0, len(full), size=9, color=NAVY, font=FONT)
    _style(reqs, object_id, 0, len(dot), color=dot_color, size=10, bold=True)
    if detail:
        _style(reqs, object_id, len(line), len(full), color=GRAY, size=8)


def data_quality_slide(reqs: list[dict], sid: str, report: dict, idx: int) -> tuple[int, list[str]]:
    from .qa import qa

    slide_plan = report.get("_slide_plan") or []
    gov = report.get("_governance") or {}
    dq_order = ordered_dq_data_sources_for_slide_plan(slide_plan)
    snap = qa.summary(report=report, data_source_order=dq_order)

    if isinstance(gov, dict) and gov.get("source_status"):
        sources = gov["source_status"]
    else:
        sources = snap.get("data_sources", {})

    discrepancies = []
    if isinstance(gov, dict) and gov.get("discrepancies"):
        discrepancies = list(gov["discrepancies"])
    if not discrepancies:
        discrepancies = [
            {"severity": f["severity"], "message": f["message"], **f}
            for f in snap.get("flags", [])
        ]

    max_rows = 8
    sorted_flags = sorted(
        discrepancies,
        key=lambda flag: {"ERROR": 0, "WARNING": 1, "INFO": 2}.get(flag.get("severity", "WARNING"), 3),
    )
    flag_chunks = _cap_chunk_list(
        [sorted_flags[i: i + max_rows] for i in range(0, len(sorted_flags), max_rows)]
    )
    if not flag_chunks:
        flag_chunks = [[]]
    num_pages = len(flag_chunks)
    object_ids: list[str] = []

    cross = (gov.get("cross_checks") if isinstance(gov, dict) else None) or {}
    total_checks = cross.get("total_checks", snap.get("total_checks"))
    total_flags = cross.get("total_flags", snap.get("total_flags"))
    errors = cross.get("errors", snap.get("errors"))
    warnings = cross.get("warnings", snap.get("warnings"))

    for page_index, chunk in enumerate(flag_chunks):
        page_sid = f"{sid}_p{page_index}" if num_pages > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        page_bg = WHITE if (report.get("type") or "") == "support_kpis" else LIGHT
        _bg(reqs, page_sid, page_bg)

        if page_index == 0:
            _slide_title(reqs, page_sid, "Data Sources & Quality")
            y = _render_source_pills(reqs, page_sid, sources, BODY_Y)

            if isinstance(gov, dict):
                scope_lines = gov.get("scope") or []
                if scope_lines:
                    y = _render_section_header(reqs, page_sid, "sc_h", y, "Scope & filters")
                    y = _render_bullet_lines(reqs, page_sid, "sc", y, scope_lines)

                fresh_lines = gov.get("freshness") or []
                if fresh_lines:
                    y = _render_section_header(reqs, page_sid, "fr_h", y, "Freshness")
                    y = _render_bullet_lines(reqs, page_sid, "fr", y, fresh_lines, color=GRAY)

                lineage = gov.get("lineage") or []
                if lineage:
                    y = _render_section_header(reqs, page_sid, "ln_h", y, "Lineage (deck-scoped)")
                    lineage_lines = [
                        f"{row.get('description', 'Data')} — {row.get('source', '?')}: {row.get('query', '')}"
                        for row in lineage[:6]
                    ]
                    y = _render_bullet_lines(reqs, page_sid, "ln", y, lineage_lines, color=GRAY, size=7.5)

            if total_flags == 0:
                status = f"All {total_checks} cross-source checks passed"
                status_color = _GREEN
            elif errors and int(errors) > 0:
                status = (
                    f"{errors} error{'s' if errors != 1 else ''} and "
                    f"{warnings} warning{'s' if warnings != 1 else ''} to note"
                )
                status_color = _RED
            else:
                status = f"{warnings or total_flags} finding{'s' if (warnings or total_flags) != 1 else ''} to note"
                status_color = _AMBER

            if chunk:
                y = _render_section_header(reqs, page_sid, "dc_h", y, "Known gaps & discrepancies")
                _box(reqs, f"{page_sid}_st", page_sid, MARGIN, y, CONTENT_W, 14, status)
                _style(reqs, f"{page_sid}_st", 0, len(status), bold=True, size=10, color=status_color, font=FONT)
                y += 18
            else:
                _box(reqs, f"{page_sid}_st", page_sid, MARGIN, y, CONTENT_W, 14, status)
                _style(reqs, f"{page_sid}_st", 0, len(status), bold=True, size=10, color=status_color, font=FONT)
                y += 18
        else:
            title = f"Data Sources & Quality — findings ({page_index + 1} of {num_pages})"
            _slide_title(reqs, page_sid, title)
            y = BODY_Y

        for index, flag in enumerate(chunk):
            if y + 18 > BODY_BOTTOM - 36:
                break
            _render_flag_row(page_sid, page_index * 100 + index, flag, y, reqs)
            y += 18

        if page_index == num_pages - 1:
            footnote = (
                (gov.get("authority_footnote") if isinstance(gov, dict) else None)
                or (
                    "Salesforce is system of record for customer inventory and contract status; "
                    "Pendo, Jira, and Cursor enrich narrative. Single-source KPIs are not cross-verified."
                )
            )
            note_y = max(y + 4, BODY_BOTTOM - 32)
            _box(reqs, f"{page_sid}_note", page_sid, MARGIN, note_y, CONTENT_W, 24, footnote)
            _style(reqs, f"{page_sid}_note", 0, len(footnote), size=7, color=GRAY, font=FONT, italic=True)

    return idx + num_pages, object_ids
