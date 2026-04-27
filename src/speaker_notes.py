"""Google Slides speaker-note read/write helpers."""

from __future__ import annotations

from typing import Any

from googleapiclient.errors import HttpError

from .config import logger
from .slides_api import slides_presentations_batch_update

__all__ = [
    "get_speaker_notes_object_id",
    "set_speaker_notes",
    "set_speaker_notes_batch",
]


def get_speaker_notes_object_id(slides_svc, pres_id: str, slide_page_id: str) -> str | None:
    """Return the object ID of the speaker-notes shape for the given slide, or None if not found.

    Uses slide's slideProperties.notesPage (embedded) or notesPageId + pages.get for
    notesProperties.speakerNotesObjectId.
    """
    fields = "slides(objectId,slideProperties(notesPage(objectId,notesProperties(speakerNotesObjectId))))"
    pres = slides_svc.presentations().get(
        presentationId=pres_id, fields=fields
    ).execute()
    for page in pres.get("slides", []):
        if page.get("objectId") != slide_page_id:
            continue
        sp = page.get("slideProperties") or {}
        notes_page = sp.get("notesPage")
        if isinstance(notes_page, dict):
            oid = (notes_page.get("notesProperties") or {}).get("speakerNotesObjectId")
            if oid:
                return oid
            notes_page_id = notes_page.get("objectId")
        else:
            notes_page_id = sp.get("notesPageId")
        if not notes_page_id:
            logger.debug("speaker_notes: slide %s has no notesPage/notesPageId", slide_page_id[:12])
            return None
        try:
            notes_page = slides_svc.presentations().pages().get(
                presentationId=pres_id, pageObjectId=notes_page_id
            ).execute()
        except HttpError as e:
            logger.warning("speaker_notes: failed to get notes page for slide %s: %s", slide_page_id[:12], e)
            return None
        oid = (notes_page.get("notesProperties") or {}).get("speakerNotesObjectId")
        if not oid:
            logger.debug("speaker_notes: notes page has no speakerNotesObjectId")
        return oid
    logger.debug("speaker_notes: slide %s not found in presentation", slide_page_id[:12])
    return None


def set_speaker_notes(slides_svc, pres_id: str, slide_page_id: str, notes_text: str) -> bool:
    """Write text to the speaker notes for the given slide. Returns True if successful."""
    oid = get_speaker_notes_object_id(slides_svc, pres_id, slide_page_id)
    if not oid:
        logger.warning("set_speaker_notes: no speaker notes object for slide %s (pres %s)", slide_page_id[:12], pres_id[:12])
        return False
    text = notes_text or ""
    reqs = [
        {"deleteText": {"objectId": oid, "textRange": {"type": "ALL"}}},
        {"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}},
    ]
    try:
        slides_presentations_batch_update(slides_svc, pres_id, reqs)
        return True
    except HttpError as e:
        err_str = str(e)
        if "startIndex 0 must be less than the endIndex 0" in err_str:
            try:
                slides_presentations_batch_update(
                    slides_svc,
                    pres_id,
                    [{"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}}],
                )
                return True
            except HttpError as e2:
                logger.warning("set_speaker_notes: insertText (empty-notes fallback) failed for slide %s: %s", slide_page_id[:12], e2)
                return False
        logger.warning("set_speaker_notes: batchUpdate failed for slide %s: %s", slide_page_id[:12], e)
        return False


def _build_notes_shape_map(slides_svc, pres_id: str) -> dict[str, str]:
    """Single presentations.get -> map of slide_page_id to speakerNotesObjectId."""
    fields = "slides(objectId,slideProperties(notesPage(objectId,notesProperties(speakerNotesObjectId))))"
    pres = slides_svc.presentations().get(
        presentationId=pres_id, fields=fields
    ).execute()
    result: dict[str, str] = {}
    for page in pres.get("slides", []):
        slide_id = page.get("objectId")
        sp = page.get("slideProperties") or {}
        notes_page = sp.get("notesPage")
        if isinstance(notes_page, dict):
            oid = (notes_page.get("notesProperties") or {}).get("speakerNotesObjectId")
            if oid:
                result[slide_id] = oid
                continue
            notes_page_id = notes_page.get("objectId")
        else:
            notes_page_id = sp.get("notesPageId")
        if notes_page_id:
            try:
                np = slides_svc.presentations().pages().get(
                    presentationId=pres_id, pageObjectId=notes_page_id
                ).execute()
                oid = (np.get("notesProperties") or {}).get("speakerNotesObjectId")
                if oid:
                    result[slide_id] = oid
            except HttpError:
                pass
    return result


def set_speaker_notes_batch(
    slides_svc, pres_id: str, items: list[tuple[str, str]]
) -> int:
    """Write speaker notes for many slides in one batchUpdate.

    ``items`` is a list of ``(slide_page_id, notes_text)`` pairs. Returns the
    number of slides successfully mapped into the update request.
    """
    if not items:
        return 0
    notes_map = _build_notes_shape_map(slides_svc, pres_id)
    reqs: list[dict[str, Any]] = []
    mapped = 0
    for slide_id, text in items:
        oid = notes_map.get(slide_id)
        if not oid:
            logger.warning("set_speaker_notes_batch: no notes shape for slide %s", slide_id[:12])
            continue
        reqs.append({"deleteText": {"objectId": oid, "textRange": {"type": "ALL"}}})
        reqs.append({"insertText": {"objectId": oid, "text": text or "", "insertionIndex": 0}})
        mapped += 1
    if not reqs:
        return 0
    try:
        slides_presentations_batch_update(slides_svc, pres_id, reqs)
        return mapped
    except HttpError as e:
        err_str = str(e)
        if "startIndex 0 must be less than the endIndex 0" in err_str:
            insert_only = [r for r in reqs if "insertText" in r]
            try:
                slides_presentations_batch_update(slides_svc, pres_id, insert_only)
                return mapped
            except HttpError as e2:
                logger.warning("set_speaker_notes_batch: insert-only fallback failed: %s", e2)
                return 0
        logger.warning("set_speaker_notes_batch: batchUpdate failed: %s", e)
        return 0
