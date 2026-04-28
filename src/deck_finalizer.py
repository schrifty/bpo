"""Final result assembly for generated decks."""

from __future__ import annotations

from typing import Any

from .config import logger
from .deck_builder_utils import _build_slide_jql_speaker_notes
from .slide_thumbnail_export import export_slide_thumbnails
from .speaker_notes import set_speaker_notes_batch


def finalize_health_deck(
    slides_service: Any,
    presentation_id: str,
    report: dict[str, Any],
    note_targets: list[tuple[str, dict[str, Any]]],
    customer: str | None,
    slides_created: int,
    thumbnails: bool = True,
) -> dict[str, Any]:
    """Write speaker notes, export optional thumbnails, and return deck metadata."""
    notes_items = [(sid, _build_slide_jql_speaker_notes(report, entry)) for sid, entry in note_targets]
    if notes_items:
        n = set_speaker_notes_batch(slides_service, presentation_id, notes_items)
        logger.info("Speaker notes: wrote %d/%d slide notes in single batchUpdate", n, len(notes_items))

    result = {
        "presentation_id": presentation_id,
        "url": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
        "customer": customer,
        "slides_created": slides_created,
    }
    nsrc = report.get("support_notable_bullets_source")
    if nsrc:
        result["notable_bullets_source"] = nsrc

    if thumbnails:
        try:
            thumbs = export_slide_thumbnails(presentation_id)
            result["thumbnails"] = [str(p) for p in thumbs]
            logger.info("Saved %d slide thumbnails for %s", len(thumbs), customer)
        except Exception as e:
            logger.warning("Thumbnail export failed: %s", e)

    return result
