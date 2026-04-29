"""Pendo-specific appendix and diagnostic slide builders."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_wrapped_text_box as _wrap_box
from .slides_theme import (
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    MARGIN,
    NAVY,
    _list_data_rows_fit_span,
)


def pendo_sentiment_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    poll_events = report.get("poll_events") or {}
    if not isinstance(poll_events, dict):
        return _missing_data_slide(reqs, sid, report, idx, "poll / NPS data")
    if poll_events.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, str(poll_events.get("error")))
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Product sentiment & surveys")
    response_count = int(poll_events.get("response_count") or 0)
    lines: list[str] = []
    nps = poll_events.get("nps")
    if isinstance(nps, dict) and int(nps.get("count") or 0) >= 1:
        lines.append(f"NPS: median {nps.get('median')} · avg {nps.get('avg')} · n={nps.get('count')}")
    lines.append(f"Poll responses (window): {response_count}")
    by_type = poll_events.get("by_poll_type") or {}
    if isinstance(by_type, dict) and by_type:
        lines.append("Responses by poll type:")
        for key, value in sorted(by_type.items(), key=lambda item: -int(item[1] or 0))[:12]:
            lines.append(f"  · {key}: {value}")
    responses = poll_events.get("responses") if isinstance(poll_events.get("responses"), list) else []
    if responses:
        lines.append("Sample responses:")
        for response in responses[:14]:
            if isinstance(response, dict):
                lines.append(f"  · {response.get('poll_type', '?')}: score={response.get('poll_response')!s}")
    if response_count == 0:
        lines.append("No poll responses recorded in this period.")
    text = "\n".join(lines)
    body_h = BODY_BOTTOM - BODY_Y - 16
    body = text[:5200]
    _wrap_box(reqs, f"{sid}_body", sid, MARGIN, BODY_Y + 10, CONTENT_W, body_h, body)
    _style(reqs, f"{sid}_body", 0, len(body), size=11, color=NAVY, font=FONT)
    return idx + 1


def pendo_friction_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    frustration = report.get("frustration") or {}
    if not isinstance(frustration, dict) or frustration.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, "UX friction aggregates")
    total = int(frustration.get("total_frustration_signals") or 0)
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "UX friction dashboard")
    totals = frustration.get("totals") if isinstance(frustration.get("totals"), dict) else {}
    # Fixed-height TEXT_BOX does not reliably clip in Slides; cap lines to the body span
    # (previously up to 4 + 12 + 2 + 12 lines overflowed ~20 lines that fit at 11pt).
    body_y = BODY_Y + 10
    body_bottom = BODY_BOTTOM - 4
    font_body_pt = 11
    max_lines = _list_data_rows_fit_span(
        y_top=body_y,
        y_bottom=body_bottom,
        font_body_pt=font_body_pt,
        reserved_header_lines=0,
        max_rows_cap=50,
    )
    # Lines before variable rows: summary, breakdown, blank, "Top pages:", blank, "Top features:".
    static_lines = 6
    avail = max(0, max_lines - static_lines)
    raw_pages = [r for r in (frustration.get("top_pages") or []) if isinstance(r, dict)]
    raw_features = [r for r in (frustration.get("top_features") or []) if isinstance(r, dict)]
    if not raw_pages and not raw_features:
        np, nf = 0, 0
    elif not raw_pages:
        np, nf = 0, min(12, len(raw_features), avail)
    elif not raw_features:
        np, nf = min(12, len(raw_pages), avail), 0
    else:
        np = min(12, len(raw_pages), max(0, (avail + 1) // 2))
        nf = min(12, len(raw_features), max(0, avail - np))

    lines = [
        f"Total frustration signals: {total:,}",
        (
            f"Rage: {int(totals.get('rageClickCount') or 0):,} · "
            f"dead: {int(totals.get('deadClickCount') or 0):,} · "
            f"errors: {int(totals.get('errorClickCount') or 0):,} · "
            f"U-turns: {int(totals.get('uTurnCount') or 0):,}"
        ),
        "",
        "Top pages:",
    ]
    for row in raw_pages[:np]:
        lines.append(f"  · {str(row.get('page') or '?')[:44]}")
    lines.append("")
    lines.append("Top features:")
    for row in raw_features[:nf]:
        lines.append(f"  · {str(row.get('feature') or '?')[:44]}")
    text = "\n".join(lines)
    body = text[:5200]
    body_h = body_bottom - body_y
    _wrap_box(reqs, f"{sid}_pf", sid, MARGIN, body_y, CONTENT_W, body_h, body)
    _style(reqs, f"{sid}_pf", 0, len(body), size=11, color=NAVY, font=FONT)
    return idx + 1


def pendo_localization_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    visitor_languages = report.get("visitor_languages") or {}
    if isinstance(visitor_languages, dict) and visitor_languages.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, str(visitor_languages.get("error")))
    languages = visitor_languages.get("languages") if isinstance(visitor_languages, dict) else []
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Localization & visitor languages")
    lines: list[str] = []
    total_visitors = int(visitor_languages.get("total_visitors") or 0) if isinstance(visitor_languages, dict) else 0
    if total_visitors:
        lines.append(f"Visitors in scope: {total_visitors:,}")
    if languages:
        lines.append("UI language distribution (metadata.agent.language):")
        for row in languages[:16]:
            if isinstance(row, dict):
                lines.append(f"  · {row.get('language')}: {row.get('users')} users")
    else:
        lines.append("No language metadata returned for this account.")
    text = "\n".join(lines)
    body = text[:4000]
    body_h = BODY_BOTTOM - BODY_Y - 16
    _wrap_box(reqs, f"{sid}_loc", sid, MARGIN, BODY_Y + 10, CONTENT_W, body_h, body)
    _style(reqs, f"{sid}_loc", 0, len(body), size=11, color=NAVY, font=FONT)
    return idx + 1


def pendo_track_analytics_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    track_breakdown = report.get("track_events_breakdown") or {}
    if isinstance(track_breakdown, dict) and track_breakdown.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, str(track_breakdown.get("error")))
    breakdown = track_breakdown.get("breakdown") if isinstance(track_breakdown.get("breakdown"), list) else []
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Custom track events (pendo.track)")
    lines = [f"Distinct track names with activity: {int(track_breakdown.get('distinct_track_types') or 0)}", ""]
    if breakdown:
        lines.append("Track name · events · unique users")
        for row in breakdown[:28]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("track_name") or "?")[:46]
            events = int(row.get("events") or 0)
            users = int(row.get("unique_users") or 0)
            lines.append(f"  · {name}  ·  {events:,}  ·  {users}")
    else:
        lines.append("No custom track events in this window (or none matched filters).")
    text = "\n".join(lines)
    body = text[:5200]
    body_h = BODY_BOTTOM - BODY_Y - 16
    _wrap_box(reqs, f"{sid}_trk", sid, MARGIN, BODY_Y + 10, CONTENT_W, body_h, body)
    _style(reqs, f"{sid}_trk", 0, len(body), size=10, color=NAVY, font=FONT)
    return idx + 1


def pendo_definitions_appendix_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    appendix = report.get("pendo_catalog_appendix") or {}
    if isinstance(appendix, dict) and appendix.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, str(appendix.get("error")))
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Pendo definitions appendix")
    lines = [
        "REST catalogs list definitions only — not computed report results.",
        (
            f"Counts — track types: {appendix.get('tracktype_total', '—')} · "
            f"segments: {appendix.get('segment_total', '—')} · "
            f"saved reports: {appendix.get('report_total', '—')}"
        ),
        "",
        "Sample track type names:",
    ]
    for name in (appendix.get("tracktype_sample_names") or [])[:12]:
        lines.append(f"  · {name}")
    lines.append("")
    lines.append("Sample segment names:")
    for name in (appendix.get("segment_sample_names") or [])[:12]:
        lines.append(f"  · {name}")
    lines.append("")
    lines.append("Sample saved report names:")
    for name in (appendix.get("report_sample_names") or [])[:12]:
        lines.append(f"  · {name}")
    text = "\n".join(lines)
    body = text[:5200]
    body_h = BODY_BOTTOM - BODY_Y - 16
    _wrap_box(reqs, f"{sid}_def", sid, MARGIN, BODY_Y + 10, CONTENT_W, body_h, body)
    _style(reqs, f"{sid}_def", 0, len(body), size=9, color=NAVY, font=FONT)
    return idx + 1
