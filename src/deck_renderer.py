"""Render resolved deck slide plans into Google Slides API requests."""

from __future__ import annotations

from typing import Any

from .config import logger
from .deck_data_enrichment import SUPPORT_DECK_IDS, SUPPORT_KPI_DECK_IDS
from .deck_builder_utils import _normalize_builder_return
from .slide_registry import get_slide_builder
from .slide_utils import (
    slide_object_id_base as _slide_object_id_base,
    unique_slide_object_id_base as _unique_slide_object_id_base,
)


def render_slide_plan(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    deck_id: str,
) -> tuple[list[dict], int, list[tuple[str, dict[str, Any]]], dict[str, Any] | None, list[dict[str, Any]]]:
    """Build batchUpdate requests and speaker-note targets for a resolved slide plan."""
    # Build every slide except "Notable" on the first pass; fetches are already in ``report`` for support.
    # Notable slides are inserted in a second batch at insertionIndex 1 after the LLM runs on a digest
    # of the same in-memory data (no refetch; bullets reflect the same dataset as the rest of the deck).
    plan_work: list[dict[str, Any]] = list(slide_plan)
    notable_deferred: dict[str, Any] | None = None
    deferred_types: frozenset[str] | None = None
    if deck_id in SUPPORT_DECK_IDS:
        deferred_types = frozenset({"cs_notable"})
    elif deck_id in SUPPORT_KPI_DECK_IDS:
        deferred_types = frozenset({"support_kpis_notable"})
    if deferred_types:
        kept2: list[dict[str, Any]] = []
        for e in plan_work:
            st = e.get("slide_type") or e.get("id", "")
            if st in deferred_types and notable_deferred is None:
                notable_deferred = e
            else:
                kept2.append(e)
        plan_work = kept2

    reqs: list[dict] = []
    idx = 1
    note_targets: list[tuple[str, dict[str, Any]]] = []
    used_slide_sids: set[str] = set()

    for entry in plan_work:
        slide_type = entry.get("slide_type", entry["id"])
        builder = get_slide_builder(slide_type)
        if not builder:
            logger.warning(
                "create_health_deck: no _SLIDE_BUILDERS entry for slide_type=%r (deck %s entry id=%r)",
                slide_type,
                deck_id,
                entry.get("id"),
            )
            continue
        report["_current_slide"] = entry
        # Page object IDs must be globally unique in the batch or Google Slides returns a 400.
        # Guard against two plan entries resolving to the same (id, seq) base; every child
        # element ID is derived from this base, so a unique base keeps the whole slide unique.
        base_sid = _slide_object_id_base(str(entry["id"]), idx)
        sid = _unique_slide_object_id_base(str(entry["id"]), idx, used_slide_sids)
        if sid != base_sid:
            logger.warning(
                "render_slide_plan: duplicate slide object id %r (deck %s, slide id=%r, idx=%d); "
                "remapping to %r to avoid a Slides 400. Check the deck plan for duplicate slide entries.",
                base_sid,
                deck_id,
                entry.get("id"),
                idx,
                sid,
            )
        used_slide_sids.add(sid)
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
