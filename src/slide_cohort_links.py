"""Helpers for linking QBR cohort bundle slides."""

from __future__ import annotations

from typing import Any

from googleapiclient.errors import HttpError

from .config import logger
from .slides_api import presentations_batch_update_chunked
from .slide_text import iter_flat_page_elements, slides_shape_text_plain, utf16_ranges_for_phrases

COHORT_BUNDLE_SIGNAL_LINK_PHRASES: tuple[str, ...] = ("cohort median", "portfolio median")


def apply_cohort_bundle_links_to_notable_signals(
    slides_svc: Any,
    pres_id: str,
    cohort_deck_url: str,
    *,
    page_object_ids: list[str] | None = None,
) -> int:
    """Hyperlink cohort/portfolio median wording on Notable Signals to the cohort review deck."""
    link_url = (cohort_deck_url or "").strip()
    if not link_url:
        return 0
    if "/edit" not in link_url:
        link_url = link_url.rstrip("/") + "/edit"

    try:
        presentation = slides_svc.presentations().get(presentationId=pres_id).execute()
    except HttpError as error:
        logger.warning("apply_cohort_bundle_links: could not read presentation %s: %s", pres_id[:12], error)
        return 0

    slides_by_id = {slide["objectId"]: slide for slide in presentation.get("slides", [])}
    if page_object_ids:
        slides_to_scan = [slides_by_id[page_id] for page_id in page_object_ids if page_id in slides_by_id]
    else:
        slides_to_scan = list(slides_by_id.values())

    requests: list[dict[str, Any]] = []
    for slide in slides_to_scan:
        for element in iter_flat_page_elements(slide.get("pageElements")):
            object_id = element.get("objectId") or ""
            if not object_id.endswith("_sig"):
                continue
            shape = element.get("shape") or {}
            text_body = shape.get("text") or {}
            full_text = slides_shape_text_plain(text_body)
            if not full_text:
                continue
            for start, end in utf16_ranges_for_phrases(full_text, COHORT_BUNDLE_SIGNAL_LINK_PHRASES):
                if start >= end:
                    continue
                requests.append({
                    "updateTextStyle": {
                        "objectId": object_id,
                        "textRange": {
                            "type": "FIXED_RANGE",
                            "startIndex": start,
                            "endIndex": end,
                        },
                        "style": {"link": {"url": link_url}},
                        "fields": "link",
                    }
                })

    if not requests:
        return 0
    try:
        presentations_batch_update_chunked(slides_svc, pres_id, requests)
    except HttpError as error:
        logger.warning("apply_cohort_bundle_links: batchUpdate failed for %s: %s", pres_id[:12], error)
        return 0
    logger.info(
        "Linked cohort/portfolio median text -> cohort deck (%d span(s)) in presentation %s...",
        len(requests),
        pres_id[:12],
    )
    return len(requests)
