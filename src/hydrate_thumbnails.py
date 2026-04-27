"""Thumbnail helpers used by evaluate and hydrate flows."""

from __future__ import annotations

import base64
import time

import requests


def get_slide_thumbnail_url(slides_svc, pres_id: str, page_id: str) -> str:
    """Get a slide thumbnail content URL.

    The Google API client is not thread-safe, so callers should use a thread-local
    Slides service when calling this from worker threads.
    """
    thumb = slides_svc.presentations().pages().getThumbnail(
        presentationId=pres_id,
        pageObjectId=page_id,
        thumbnailProperties_thumbnailSize="LARGE",
    ).execute()
    return thumb["contentUrl"]


def download_thumbnail_b64(url: str, max_retries: int = 3) -> str:
    """Download a thumbnail URL and return base64-encoded PNG bytes."""
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return base64.b64encode(resp.content).decode()
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    raise last_err


def get_slide_thumbnail_b64(slides_svc, pres_id: str, page_id: str) -> str:
    """Get a slide thumbnail URL and download it as base64."""
    url = get_slide_thumbnail_url(slides_svc, pres_id, page_id)
    return download_thumbnail_b64(url)
