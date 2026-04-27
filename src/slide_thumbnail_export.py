"""Thumbnail export helpers for generated Google Slides decks."""

from __future__ import annotations

import re
import tempfile
import urllib.request
from pathlib import Path

from .config import logger
from .slides_api import _get_service


def export_slide_thumbnails(
    presentation_id: str,
    output_dir: str | Path | None = None,
    size: str = "LARGE",
) -> list[Path]:
    """Download PNG thumbnails for every slide in a presentation.

    Args:
        presentation_id: Google Slides presentation ID or full URL.
        output_dir: Where to save PNGs. Defaults to a temp directory.
        size: Thumbnail size, "SMALL" (default 200px) or "LARGE" (default 800px).

    Returns:
        List of saved PNG file paths.
    """
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", presentation_id)
    pres_id = match.group(1) if match else presentation_id

    slides_service, _ds, _ = _get_service()
    pres = slides_service.presentations().get(presentationId=pres_id).execute()
    title = pres.get("title", pres_id)
    slides = pres.get("slides", [])

    if not slides:
        logger.warning("Presentation %s has no slides", pres_id)
        return []

    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix=f"bpo-thumbs-{pres_id[:12]}-"))
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []
    for i, slide in enumerate(slides):
        page_id = slide["objectId"]
        thumb = slides_service.presentations().pages().getThumbnail(
            presentationId=pres_id,
            pageObjectId=page_id,
            thumbnailProperties_thumbnailSize=size,
        ).execute()
        url = thumb["contentUrl"]
        dest = out / f"slide_{i + 1:02d}.png"
        urllib.request.urlretrieve(url, str(dest))
        saved.append(dest)

    logger.info("Exported %d thumbnails for '%s' to %s", len(saved), title, out)
    return saved
