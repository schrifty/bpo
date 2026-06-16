"""Replace selected slides in an existing Google Slides deck with freshly built versions."""

from __future__ import annotations

import re
from typing import Any

from .config import logger
from .deck_builder_utils import _build_slide_jql_speaker_notes, _normalize_builder_return
from .deck_data_enrichment import enrich_cursor_usage_if_needed
from .deck_governance import _CURSOR_SLIDE_TYPES
from .deck_loader import resolve_deck
from .deck_orchestrator import _PORTFOLIO_DRIVE_TITLE_TAIL
from .hydrate_extract import extract_text
from .slide_registry import _SLIDE_BUILDERS
from .slide_utils import slide_object_id_base as _slide_object_id_base
from .slides_api import _get_service, presentations_batch_update_chunked
from .speaker_notes import set_speaker_notes_batch

_SLIDE_TYPE_TITLES: dict[str, str] = {
    "cursor_cost": "Cursor AI Coding Spend",
    "cursor_cost_models": "Cursor AI Spend by Model",
    "cursor_efficiency": "Cursor AI Coding Efficiency - Engineering",
    "cursor_efficiency_engineers": "Cursor AI Coding Efficiency - Non-Engineers",
    "cursor_usage": "Cursor AI Token Usage",
    "cursor_usage_non_engineers": "Cursor AI Token Usage — Non-Engineering",
    "cursor_model_usage": "Cursor AI Model Usage",
    "cursor_users": "Cursor AI Power Users",
    "cursor_users_non_engineers": "Cursor AI Power Users — Non-Engineering",
}

_PRESENTATION_ID_RE = re.compile(r"/presentation/d/([a-zA-Z0-9_-]+)")


def parse_presentation_id(value: str) -> str:
    raw = (value or "").strip()
    m = _PRESENTATION_ID_RE.search(raw)
    if m:
        return m.group(1)
    return raw


def infer_slide_type_from_page(slide: dict[str, Any]) -> str | None:
    """Best-effort slide_type match from on-slide title text."""
    texts: list[str] = []
    for element in slide.get("pageElements") or []:
        texts.extend(extract_text(element))
    blob = "\n".join(texts)
    # Match longer titles first (engineers vs non-engineering variants).
    for slide_type, title in sorted(_SLIDE_TYPE_TITLES.items(), key=lambda kv: -len(kv[1])):
        if title in blob:
            return slide_type
    return None


def find_latest_presentation_for_deck(
    drive_service: Any,
    *,
    deck_id: str,
) -> dict[str, Any] | None:
    """Return the newest presentation file for a portfolio deck id."""
    from .drive_config import get_deck_output_folder_id, get_qbr_output_root_folder_id

    tail = _PORTFOLIO_DRIVE_TITLE_TAIL.get(deck_id)
    if not tail:
        return None
    name_hint = tail
    folder_ids = [get_deck_output_folder_id(), get_qbr_output_root_folder_id()]
    best: dict[str, Any] | None = None
    for folder_id in folder_ids:
        if not folder_id:
            continue
        q = (
            f"'{folder_id}' in parents and "
            "mimeType='application/vnd.google-apps.presentation' and "
            f"name contains '{name_hint}' and trashed=false"
        )
        best = _drive_search_newest(drive_service, q, best)

    if best is None:
        q = (
            "mimeType='application/vnd.google-apps.presentation' and "
            f"name contains 'Portfolio - {name_hint}' and trashed=false"
        )
        best = _drive_search_newest(drive_service, q, best)
    return best


def _drive_search_newest(
    drive_service: Any,
    q: str,
    current_best: dict[str, Any] | None,
) -> dict[str, Any] | None:
    best = current_best
    try:
        resp = (
            drive_service.files()
            .list(
                q=q,
                fields="files(id,name,modifiedTime,webViewLink)",
                orderBy="modifiedTime desc",
                pageSize=5,
            )
            .execute()
        )
    except Exception as exc:
        logger.warning("Drive search failed (%s): %s", q[:80], exc)
        return best
    for row in resp.get("files") or []:
        if best is None or (row.get("modifiedTime") or "") > (best.get("modifiedTime") or ""):
            best = row
    return best


def regenerate_deck_slides(
    presentation_id: str,
    *,
    deck_id: str = "engineering-portfolio",
    slide_types: set[str] | frozenset[str],
    days: int = 30,
    report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Delete and rebuild matching slides in place (same insertion indices)."""
    pres_id = parse_presentation_id(presentation_id)
    if not pres_id:
        return {"error": "presentation_id is required"}

    unknown = set(slide_types) - set(_SLIDE_BUILDERS.keys())
    if unknown:
        return {"error": f"Unknown slide types: {', '.join(sorted(unknown))}"}

    resolved = resolve_deck(deck_id, None)
    if resolved.get("error"):
        return {"error": resolved["error"]}

    slide_plan = list(resolved.get("slides") or [])
    plan_by_type = {
        (entry.get("slide_type") or entry.get("id") or ""): entry
        for entry in slide_plan
    }

    try:
        slides_service, drive_service, _sheets = _get_service()
    except (ValueError, FileNotFoundError) as exc:
        return {"error": str(exc)}

    pres = slides_service.presentations().get(presentationId=pres_id).execute()
    pages = pres.get("slides") or []
    targets: list[tuple[int, str, str]] = []
    for index, page in enumerate(pages):
        slide_type = infer_slide_type_from_page(page)
        if slide_type and slide_type in slide_types:
            targets.append((index, page["objectId"], slide_type))

    if not targets:
        return {
            "error": "No matching slides found in presentation",
            "presentation_id": pres_id,
            "slide_types": sorted(slide_types),
        }

    rep: dict[str, Any] = dict(report or {})
    rep.setdefault("type", "engineering_portfolio")
    rep.setdefault("customer", "Engineering")
    rep.setdefault("days", days)
    rep["_deck_id"] = deck_id
    rep["_slides_svc"] = slides_service
    rep["_drive_svc"] = drive_service

    cursor_plan = [plan_by_type[st] for _, _, st in targets if st in plan_by_type]
    enrich_cursor_usage_if_needed(rep, cursor_plan, deck_id=deck_id)
    if not (rep.get("cursor_usage") or {}).get("configured"):
        return {"error": "Cursor usage not configured — set CURSOR_ADMIN_API_KEY", "presentation_id": pres_id}

    from .charts import DeckCharts

    rep["_charts"] = DeckCharts(str(pres.get("title") or deck_id))

    rebuilt: list[str] = []
    for insertion_index, page_id, slide_type in sorted(targets, key=lambda t: t[0], reverse=True):
        entry = plan_by_type.get(slide_type)
        if not entry:
            logger.warning("Slide type %s not in deck plan — skipping", slide_type)
            continue
        builder = _SLIDE_BUILDERS.get(slide_type)
        if not builder:
            continue

        rep["_current_slide"] = entry
        sid = _slide_object_id_base(str(entry.get("id") or slide_type), insertion_index + 1)
        reqs: list[dict[str, Any]] = [{"deleteObject": {"objectId": page_id}}]
        try:
            ret = builder(reqs, sid, rep, insertion_index)
            next_idx, note_ids = _normalize_builder_return(ret, sid)
        except Exception as exc:
            logger.warning("Rebuild failed for %s: %s", slide_type, exc)
            return {
                "error": f"Rebuild failed for {slide_type}: {exc}",
                "presentation_id": pres_id,
                "rebuilt": rebuilt,
            }

        if len(reqs) <= 1:
            logger.warning("Builder returned no create requests for %s", slide_type)
            continue

        try:
            presentations_batch_update_chunked(slides_service, pres_id, reqs)
        except Exception as exc:
            return {
                "error": str(exc),
                "presentation_id": pres_id,
                "rebuilt": rebuilt,
            }

        notes = _build_slide_jql_speaker_notes(rep, entry)
        if note_ids:
            set_speaker_notes_batch(slides_service, pres_id, [(nid, notes) for nid in note_ids])
        rebuilt.append(slide_type)
        logger.info("Regenerated slide %s at index %d", slide_type, insertion_index)

    url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
    return {
        "presentation_id": pres_id,
        "url": url,
        "rebuilt": rebuilt,
        "skipped": sorted(set(slide_types) - set(rebuilt)),
    }
