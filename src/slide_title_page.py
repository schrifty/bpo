"""Opening title slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    background as _bg,
    rect as _rect,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import (
    BLUE,
    CONTENT_W,
    FONT,
    FONT_SERIF,
    GRAY,
    MARGIN,
    NAVY,
    SLIDE_W,
    WHITE,
    _date_range,
)


def title_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    acct = report["account"]
    name = report["customer"]
    sub = (
        "Product Usage Review  ·  "
        f"{_date_range(report['days'], report.get('quarter'), report.get('quarter_start'), report.get('quarter_end'))}"
    )
    meta = f"CSM: {acct['csm']}  |  {acct['total_sites']} sites · {acct['total_visitors']} users  |  {report['generated']}"

    _rect(reqs, f"{sid}_bar", sid, 0, 190, SLIDE_W, 3, BLUE)

    _box(reqs, f"{sid}_n", sid, MARGIN, 100, CONTENT_W, 60, name)
    _style(reqs, f"{sid}_n", 0, len(name), bold=True, size=40, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 200, CONTENT_W, 30, sub)
    _style(reqs, f"{sid}_s", 0, len(sub), size=15, color=BLUE, font=FONT)

    _box(reqs, f"{sid}_m", sid, MARGIN, 350, CONTENT_W, 24, meta)
    _style(reqs, f"{sid}_m", 0, len(meta), size=9, color=GRAY, font=FONT)

    label = "INTERNAL ONLY"
    _box(reqs, f"{sid}_int", sid, MARGIN, 160, CONTENT_W, 22, label)
    _style(reqs, f"{sid}_int", 0, len(label), bold=True, size=10, color=BLUE, font=FONT)

    return idx + 1
