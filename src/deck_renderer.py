"""Render resolved deck slide plans into Google Slides API requests."""

from __future__ import annotations

from typing import Any

from .config import logger
from .deck_data_enrichment import SUPPORT_DECK_IDS
from .deck_builder_utils import _normalize_builder_return
from .slide_registry import _SLIDE_BUILDERS
from .slide_utils import slide_object_id_base as _slide_object_id_base


def render_slide_plan(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    deck_id: str,
) -> tuple[list[dict], int, list[tuple[str, dict[str, Any]]], dict[str, Any] | None, list[dict[str, Any]]]:
    """Build batchUpdate requests and speaker-note targets for a resolved slide plan."""
    # Build every slide except "Notable" on the first pass; fetches are already in ``report`` for support.
    # The Notable slide (cs_notable) is inserted in a second batch at insertionIndex 1 after the LLM runs on a digest
    # of the same in-memory Jira data (so we do not refetch; bullets reflect the same dataset as the rest of the deck).
    plan_work: list[dict[str, Any]] = list(slide_plan)
    notable_deferred: dict[str, Any] | None = None
    if deck_id in SUPPORT_DECK_IDS:
        kept2: list[dict[str, Any]] = []
        for e in plan_work:
            if (e.get("slide_type") or e.get("id", "")) == "cs_notable" and notable_deferred is None:
                notable_deferred = e
            else:
                kept2.append(e)
        plan_work = kept2

    reqs: list[dict] = []
    idx = 1
    note_targets: list[tuple[str, dict[str, Any]]] = []

    for entry in plan_work:
        slide_type = entry.get("slide_type", entry["id"])
        builder = _SLIDE_BUILDERS.get(slide_type)
        if not builder:
            logger.warning(
                "create_health_deck: no _SLIDE_BUILDERS entry for slide_type=%r (deck %s entry id=%r)",
                slide_type,
                deck_id,
                entry.get("id"),
            )
            continue
        report["_current_slide"] = entry
        sid = _slide_object_id_base(str(entry["id"]), idx)
        ret = builder(reqs, sid, report, idx)
        next_idx, note_ids = _normalize_builder_return(ret, sid)
        if slide_type == "cohort_profiles" and note_ids:
            blks = report.get("_cohort_profile_speaker_note_blocks") or []
            for i, nid in enumerate(note_ids):
                note_entry = dict(entry)
                if i < len(blks):
                    note_entry["_cohort_profile_block"] = blks[i]
                note_targets.append((nid, note_entry))
        else:
            for nid in note_ids:
                note_targets.append((nid, dict(entry)))
        idx = next_idx

    slides_created = idx - 1
    return reqs, slides_created, note_targets, notable_deferred, plan_work
