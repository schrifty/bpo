"""Notable Signals slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    background as _bg,
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BLUE, BODY_Y, CONTENT_W, FONT, GRAY, LIGHT, MARGIN, NAVY


MAX_SIGNAL_BULLETS = 8


def render_signal_list_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    *,
    signals: list[str],
    title: str,
    missing_label: str,
    trend_banner: str = "",
) -> int | tuple[int, list[str]]:
    """Render the canonical Notable/Critical Signals numbered-list layout."""
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, missing_label)

    shown = signals[:MAX_SIGNAL_BULLETS]
    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, title)
    trend_h = 0
    if trend_banner:
        trend_h = 46
        _box(reqs, f"{sid}_trend", sid, MARGIN, BODY_Y, CONTENT_W, trend_h - 4, trend_banner)
        _style(reqs, f"{sid}_trend", 0, len(trend_banner), size=11, color=GRAY, font=FONT)

    lines: list[str] = []
    for signal_index, signal in enumerate(shown, start=1):
        lines.append(f"{signal_index}.   {signal}")
        lines.append("")
    text = "\n".join(lines)
    oid = f"{sid}_sig"
    body_top = BODY_Y + trend_h
    body_h = max(120, 290 - trend_h)
    _box(reqs, oid, sid, MARGIN, body_top, CONTENT_W, body_h, text)
    _style(reqs, oid, 0, len(text), size=12, color=NAVY, font=FONT)
    offset = 0
    for line in lines:
        if line and line[0].isdigit():
            dot = line.index(".")
            _style(reqs, oid, offset, offset + dot + 1, bold=True, color=BLUE)
        offset += len(line) + 1
    return idx + 1


def signals_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    signals = list(report.get("signals") or [])
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, "action signals")

    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "").strip() or "Notable Signals"
    return render_signal_list_slide(
        reqs,
        sid,
        report,
        idx,
        signals=signals,
        title=title,
        missing_label="action signals",
        trend_banner=(report.get("signals_trends_display") or "").strip(),
    )
