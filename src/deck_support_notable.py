"""Second-pass support Notable slide insertion (support review + support-kpis)."""

from __future__ import annotations

import socket
from typing import Any

from googleapiclient.errors import HttpError

from .config import logger
from .deck_data_enrichment import SUPPORT_DECK_IDS, SUPPORT_KPI_DECK_IDS
from .deck_builder_utils import _normalize_builder_return
from .slide_cs_notable import cs_notable_slide as _cs_notable_slide
from .slide_support_kpis_notable import support_kpis_notable_slide as _support_kpis_notable_slide
from .slides_api import presentations_batch_update_chunked

_NOTABLE_DECK_IDS = SUPPORT_DECK_IDS | SUPPORT_KPI_DECK_IDS


def insert_support_notable_slide(
    slides_service: Any,
    presentation_id: str,
    report: dict[str, Any],
    notable_deferred: dict[str, Any] | None,
    plan_work: list[dict[str, Any]],
    note_targets: list[tuple[str, dict[str, Any]]],
    slides_created: int,
    customer: str | None,
    deck_id: str,
) -> tuple[int, list[tuple[str, dict[str, Any]]], dict[str, Any] | None]:
    """Insert the deferred Notable slide after the main slide batch.

    Returns ``(slides_created, note_targets, error_result)``.
    """
    if deck_id not in _NOTABLE_DECK_IDS or not notable_deferred or slides_created <= 0:
        return slides_created, note_targets, None

    titles = [e.get("title", "") for e in plan_work]
    ne = dict(notable_deferred)

    if deck_id in SUPPORT_KPI_DECK_IDS:
        from .support_notable_llm import (
            NotableLlmError,
            build_support_kpis_digest,
            generate_support_kpis_notable_bullets_via_llm,
        )

        try:
            digest = build_support_kpis_digest(report, slide_titles=titles)
        except Exception as e:
            logger.warning("Support KPIs Notable: digest build failed; LLM may have thin context. %s", e)
            digest = {}
        try:
            bullets, src = generate_support_kpis_notable_bullets_via_llm(digest, ne)
        except NotableLlmError as e:
            return slides_created, note_targets, {
                "error": str(e),
                "presentation_id": presentation_id,
                "url": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
                "customer": customer,
                "slides_created": slides_created,
                "deck_id": deck_id,
                "hint": (
                    "Notable Findings slide was not added. The deck is otherwise complete. "
                    "Set BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK=true to insert static bullets, "
                    "or fix the LLM path and regenerate."
                ),
            }
        report["support_kpis_notable_bullets"] = bullets
        report["support_kpis_notable_bullets_source"] = src
        report["_current_slide"] = ne
        nreq: list[dict] = []
        nsid = "s_sknb1"
        ret_n = _support_kpis_notable_slide(nreq, nsid, report, 1)
        builder_label = "support_kpis_notable"
    else:
        from .support_notable_llm import (
            NotableLlmError,
            build_support_review_digest,
            generate_notable_bullets_via_llm,
        )

        try:
            digest = build_support_review_digest(report, slide_titles=titles)
        except Exception as e:
            logger.warning("Notable: digest build failed; LLM may have thin context. %s", e)
            digest = {}
        try:
            bullets, src = generate_notable_bullets_via_llm(digest, ne)
        except NotableLlmError as e:
            return slides_created, note_targets, {
                "error": str(e),
                "presentation_id": presentation_id,
                "url": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
                "customer": customer,
                "slides_created": slides_created,
                "deck_id": deck_id,
                "hint": (
                    "Notable slide was not added. The deck is otherwise complete. "
                    "Set BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK=true to insert generic bullets, "
                    "or fix the Notable/LLM path and regenerate."
                ),
            }
        ne["notable_items"] = bullets
        report["support_notable_bullets"] = bullets
        report["support_notable_bullets_source"] = src
        report["_current_slide"] = ne
        nreq = []
        nsid = "s_snb1"
        ret_n = _cs_notable_slide(nreq, nsid, report, 1)
        builder_label = "cs_notable"

    _nidx, n_note_ids = _normalize_builder_return(ret_n, nsid)
    del _nidx
    try:
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(60.0)
            presentations_batch_update_chunked(slides_service, presentation_id, nreq)
        finally:
            socket.setdefaulttimeout(old_timeout)
    except HttpError as e:
        logger.error("%s: second batch (insert at index 1) failed: %s", builder_label, e)
    else:
        slides_created += 1
        for nid in n_note_ids:
            note_targets.append((nid, ne))

    return slides_created, note_targets, None
