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
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, LIGHT, MARGIN, NAVY, _cap_chunk_list


def signals_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    signals = report.get("signals", [])
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, "action signals")

    max_signals = max(1, (BODY_BOTTOM - BODY_Y) // 32 - 1)
    chunks = _cap_chunk_list([signals[i: i + max_signals] for i in range(0, len(signals), max_signals)])
    object_ids: list[str] = []
    for page_index, shown in enumerate(chunks):
        page_sid = f"{sid}_p{page_index}" if len(chunks) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        _bg(reqs, page_sid, LIGHT)
        title = "Notable Signals" if len(chunks) == 1 else f"Notable Signals ({page_index + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, title)
        trend_banner = (report.get("signals_trends_display") or "").strip()
        trend_h = 0
        if page_index == 0 and trend_banner:
            trend_h = 46
            _box(reqs, f"{page_sid}_trend", page_sid, MARGIN, BODY_Y, CONTENT_W, trend_h - 4, trend_banner)
            _style(reqs, f"{page_sid}_trend", 0, len(trend_banner), size=11, color=GRAY, font=FONT)

        base = page_index * max_signals
        lines: list[str] = []
        for signal_index, signal in enumerate(shown, start=base + 1):
            lines.append(f"{signal_index}.   {signal}")
            lines.append("")
        text = "\n".join(lines)
        oid = f"{page_sid}_sig"
        body_top = BODY_Y + trend_h
        body_h = max(120, 290 - trend_h)
        _box(reqs, oid, page_sid, MARGIN, body_top, CONTENT_W, body_h, text)
        _style(reqs, oid, 0, len(text), size=12, color=NAVY, font=FONT)
        offset = 0
        for line in lines:
            if line and line[0].isdigit():
                dot = line.index(".")
                _style(reqs, oid, offset, offset + dot + 1, bold=True, color=BLUE)
            offset += len(line) + 1
    return idx + len(chunks), object_ids
