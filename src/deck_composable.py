"""Composable deck API for creating decks and appending one slide at a time."""

from __future__ import annotations

import socket
from typing import Any

from googleapiclient.errors import HttpError

from .config import logger
from .deck_builder_utils import (
    _build_slide_jql_speaker_notes,
    _normalize_builder_return,
)
from .slide_metadata import SLIDE_DATA_REQUIREMENTS
from .slide_registry import _SLIDE_BUILDERS
from .slide_utils import slide_object_id_base as _slide_object_id_base
from .slides_api import (
    _get_service,
    presentations_batch_update_chunked,
    slides_presentations_batch_update,
)
from .slides_theme import _date_range
from .speaker_notes import set_speaker_notes_batch


def _get_deck_output_folder() -> str | None:
    """Return the base QBR Generator folder ID for individual deck outputs."""
    from .drive_config import get_deck_output_folder_id

    return get_deck_output_folder_id()


def create_empty_deck(customer: str, days: int = 30, deck_name: str | None = None) -> dict[str, Any]:
    """Create an empty presentation. Returns {deck_id, url} for use with add_slide."""
    try:
        slides_service, drive_service, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    label = deck_name or "Usage Health Review"
    title = f"{customer} — {label} ({_date_range(days)})"
    try:
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            file_meta["parents"] = [output_folder]

        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Drive operations
            f = drive_service.files().create(body=file_meta).execute()
        finally:
            socket.setdefaulttimeout(old_timeout)

        deck_id = f["id"]
        logger.info("Created deck %s: %s", deck_id, title)
    except HttpError as e:
        return {"error": str(e)}

    # Delete the default blank slide.
    try:
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Slides API
            pres = slides_service.presentations().get(presentationId=deck_id).execute()
            default_id = pres["slides"][0]["objectId"]
            slides_presentations_batch_update(
                slides_service,
                deck_id,
                [{"deleteObject": {"objectId": default_id}}],
            )
        finally:
            socket.setdefaulttimeout(old_timeout)
    except Exception:
        pass

    return {
        "deck_id": deck_id,
        "url": f"https://docs.google.com/presentation/d/{deck_id}/edit",
    }


_slide_counter: dict[str, int] = {}


def add_slide(deck_id: str, slide_type: str, data: dict[str, Any]) -> dict[str, Any]:
    """Add one slide to an existing deck."""
    builder = _SLIDE_BUILDERS.get(slide_type)
    if not builder:
        return {"error": f"Unknown slide type '{slide_type}'. Valid: {', '.join(_SLIDE_BUILDERS)}"}

    try:
        slides_service, _ds, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    # Use a local counter as insertion index to avoid an API round-trip per slide.
    count = _slide_counter.get(deck_id, 0)
    _slide_counter[deck_id] = count + 1
    idx = count
    sid = _slide_object_id_base(slide_type, count)

    reqs: list[dict] = []
    try:
        ret = builder(reqs, sid, data, idx)
        _new_idx, note_ids = _normalize_builder_return(ret, sid)
    except (KeyError, TypeError, IndexError) as e:
        required = SLIDE_DATA_REQUIREMENTS.get(slide_type, [])
        return {
            "error": f"Slide '{slide_type}' data is missing required key: {e}. Required keys: {required}",
            "slide_type": slide_type,
        }

    if not reqs:
        return {"slide_type": slide_type, "status": "skipped (no data)"}

    try:
        presentations_batch_update_chunked(slides_service, deck_id, reqs)
    except HttpError as e:
        return {"error": str(e), "slide_type": slide_type}

    note_entry = {
        "id": slide_type,
        "slide_type": slide_type,
        "title": data.get("title", slide_type.replace("_", " ").title()),
    }
    note_payload = dict(data)
    note_payload["_current_slide"] = note_entry
    notes = _build_slide_jql_speaker_notes(note_payload, note_entry)
    if note_ids:
        n = set_speaker_notes_batch(slides_service, deck_id, [(nid, notes) for nid in note_ids])
        if n < len(note_ids):
            logger.warning(
                "Could not write JQL speaker notes for %d/%d slides in deck %s",
                len(note_ids) - n,
                len(note_ids),
                deck_id[:12],
            )

    return {"slide_type": slide_type, "status": "added", "position": idx + 1, "pages": len(note_ids)}
