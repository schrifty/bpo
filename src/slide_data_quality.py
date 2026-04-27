"""Data Quality slide rendering."""

from __future__ import annotations

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


def data_quality_slide(reqs: list[dict], sid: str, report: dict, idx: int) -> tuple[int, list[str]]:
    from .qa import qa

    slide_plan = report.get("_slide_plan") or []
    dq_order = ordered_dq_data_sources_for_slide_plan(slide_plan)
    snap = qa.summary(report=report, data_source_order=dq_order)

    max_rows = 10
    flags = snap["flags"]
    sorted_flags = sorted(flags, key=lambda flag: {"ERROR": 0, "WARNING": 1, "INFO": 2}.get(flag["severity"], 3))
    flag_chunks = _cap_chunk_list(
        [sorted_flags[i: i + max_rows] for i in range(0, len(sorted_flags), max_rows)]
    )
    if not flag_chunks:
        flag_chunks = [[]]
    num_pages = len(flag_chunks)
    object_ids: list[str] = []

    def render_flag_row(page_sid: str, flag_index: int, flag: dict, y_pos: float) -> None:
        severity = flag["severity"]
        dot = _SEV_DOT.get(severity, "?")
        dot_color = _SEV_COLOR.get(severity, GRAY)
        msg = flag["message"]
        detail_parts = []
        if flag["expected"] is not None and flag["actual"] is not None:
            detail_parts.append(f"expected {flag['expected']}, got {flag['actual']}")
        if flag["sources"]:
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

    for page_index, chunk in enumerate(flag_chunks):
        page_sid = f"{sid}_p{page_index}" if num_pages > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        _bg(reqs, page_sid, LIGHT)
        if page_index == 0:
            _slide_title(reqs, page_sid, "Data Quality")
            sources = snap.get("data_sources", {})
            src_x = MARGIN
            src_y = BODY_Y
            for source_index, (name, status) in enumerate(sources.items()):
                if status == "ok":
                    icon, color = "\u2713", _GREEN
                else:
                    icon, color = "\u2717", _AMBER
                label = f"{icon} {name}"
                _pill(reqs, f"{page_sid}_src{source_index}", page_sid, src_x, src_y, 120, 22, label, WHITE, color)
                src_x += 130
            total_checks = snap["total_checks"]
            total_flags = snap["total_flags"]
            errors = snap["errors"]
            warnings = snap["warnings"]
            summary_y = src_y + 36
            if total_flags == 0:
                status = f"All {total_checks} cross-source checks passed"
                status_color = _GREEN
            elif errors > 0:
                status = (
                    f"{errors} error{'s' if errors != 1 else ''} and "
                    f"{warnings} warning{'s' if warnings != 1 else ''} found"
                )
                status_color = _RED
            else:
                status = f"{warnings} finding{'s' if warnings != 1 else ''} to note"
                status_color = _AMBER
            _box(reqs, f"{page_sid}_st", page_sid, MARGIN, summary_y, CONTENT_W, 20, status)
            _style(reqs, f"{page_sid}_st", 0, len(status), bold=True, size=12, color=status_color, font=FONT)
            y = summary_y + 28
        else:
            title = f"Data Quality — findings ({page_index + 1} of {num_pages})"
            _slide_title(reqs, page_sid, title)
            y = BODY_Y

        for index, flag in enumerate(chunk):
            render_flag_row(page_sid, page_index * 100 + index, flag, y)
            y += 20

        if page_index == num_pages - 1:
            note_y = max(y + 6, BODY_BOTTOM - 40)
            note = (
                "Single-source metrics (feature adoption, exports, guides, dollar values) "
                "are not independently verified across sources."
            )
            _box(reqs, f"{page_sid}_note", page_sid, MARGIN, note_y, CONTENT_W, 28, note)
            _style(reqs, f"{page_sid}_note", 0, len(note), size=7, color=GRAY, font=FONT, italic=True)

    return idx + num_pages, object_ids
