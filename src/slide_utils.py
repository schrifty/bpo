"""Small shared utilities for Google Slides request builders."""

from __future__ import annotations

import hashlib

SLIDES_OBJECT_ID_BASE_MAX = 32


def blob_recent_tickets_window_days(blob: dict, closed: bool) -> int | None:
    """Return Jira ``*_within_days`` window, or None if unbounded."""
    key = "closed_within_days" if closed else "opened_within_days"
    if key not in blob:
        return 45
    value = blob.get(key)
    if value is None:
        return None
    return int(value)


def slide_object_id_base(slide_id: str, seq: int) -> str:
    """Build a page-level objectId base that stays under Slides' 50-char cap."""
    raw = f"s_{slide_id}_{seq}"
    if len(raw) <= SLIDES_OBJECT_ID_BASE_MAX:
        return raw
    digest = hashlib.blake2s(f"{slide_id}\n{seq}".encode(), digest_size=5).hexdigest()[:10]
    return f"s_{seq}_{digest}"


def unique_slide_object_id_base(slide_id: str, seq: int, used: set[str]) -> str:
    """Like :func:`slide_object_id_base`, but guaranteed unique within ``used``.

    Two slide-plan entries that resolve to the same ``(slide_id, seq)`` would emit duplicate
    page object IDs, which makes Google Slides ``batchUpdate`` fail hard with a 400
    ("The object ID ... should be unique among all pages and page elements"). Because every
    child element ID a builder emits is derived from this base, keeping the base unique keeps
    the whole slide unique. When a collision is detected the base is salted with an increasing
    suffix until unique. ``used`` is not mutated; the caller records the returned value.
    """
    base = slide_object_id_base(slide_id, seq)
    if base not in used:
        return base
    n = 2
    while True:
        candidate = slide_object_id_base(f"{slide_id}_{n}", seq)
        if candidate not in used:
            return candidate
        n += 1


def slide_size(width: float, height: float) -> dict:
    return {
        "width": {"magnitude": width, "unit": "PT"},
        "height": {"magnitude": height, "unit": "PT"},
    }


def slide_transform(x: float, y: float) -> dict:
    return {"scaleX": 1, "scaleY": 1, "translateX": x, "translateY": y, "unit": "PT"}


def truncate_table_cell(value: str | None, max_len: int) -> str:
    """Trim to one line with ``...``; when truncating, prefer a word break."""
    if value is None:
        return "—"
    text = str(value).strip()
    if not text:
        return "—"
    if len(text) <= max_len:
        return text
    if max_len < 4:
        return text[:max_len]
    room = max_len - 3
    head = text[:room]
    if " " in head:
        idx = head.rfind(" ")
        if idx > max(6, max_len // 4):
            head = head[:idx].rstrip()
    if not head:
        head = text[:room]
    return head + "..."


def max_chars_one_line_for_table_col(col_width_pt: float, font_pt: float = 8.0) -> int:
    """Upper bound on characters for a single-line table cell."""
    if col_width_pt <= 0:
        return 8
    inner = max(20.0, float(col_width_pt) - 18.0)
    per = max(3.2, float(font_pt) * 0.58)
    return max(4, int((inner / per) * 0.88))


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out
