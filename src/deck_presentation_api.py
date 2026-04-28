"""Google Slides presentation operations used by deck orchestration."""

from __future__ import annotations

import socket
from typing import Any

from googleapiclient.errors import HttpError

from .config import logger
from .deck_composable import _get_deck_output_folder
from .slides_api import _google_api_unreachable_hint, presentations_batch_update_chunked


def create_presentation(
    drive_service: Any,
    title: str,
    output_folder_id: str | None = None,
) -> tuple[str | None, dict[str, Any] | None]:
    """Create a Google Slides presentation and return ``(presentation_id, error_result)``."""
    try:
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Drive operations

            file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
            output_folder = output_folder_id if output_folder_id else _get_deck_output_folder()
            if output_folder:
                file_meta["parents"] = [output_folder]
            file = drive_service.files().create(body=file_meta).execute()
            pres_id = file["id"]
            logger.info("Created presentation %s: %s", pres_id, title)
            return pres_id, None
        finally:
            socket.setdefaulttimeout(old_timeout)
    except HttpError as e:
        err_str = str(e)
        if "rate" in err_str.lower() or "quota" in err_str.lower():
            return None, {"error": f"Rate limit: {err_str}. Wait and retry."}
        return None, {"error": err_str}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            return None, {"error": str(e), "hint": hint}
        raise


def append_default_slide_delete_if_needed(
    slides_service: Any,
    presentation_id: str,
    reqs: list[dict],
    slides_created: int,
    deck_id: str,
    customer: str | None,
    slide_plan_len: int,
) -> None:
    """Append a deleteObject request for the default blank slide when slides were built."""
    try:
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Slides API
            pres = slides_service.presentations().get(presentationId=presentation_id).execute()
        finally:
            socket.setdefaulttimeout(old_timeout)

        default_id = pres["slides"][0]["objectId"]
        if slides_created > 0:
            reqs.append({"deleteObject": {"objectId": default_id}})
        else:
            logger.error(
                "create_health_deck: built 0 slides (deck_id=%s customer=%r plan_len=%d). "
                "Leaving default slide; check warnings above for missing builders.",
                deck_id,
                customer,
                slide_plan_len,
            )
    except Exception:
        pass


def submit_slide_requests(
    slides_service: Any,
    presentation_id: str,
    reqs: list[dict],
    customer: str | None,
    deck_id: str,
) -> dict[str, Any] | None:
    """Submit slide batchUpdate requests and return an error result if submission fails."""
    try:
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(60.0)  # 60 second timeout for batchUpdate (can be large)
            presentations_batch_update_chunked(slides_service, presentation_id, reqs)
        finally:
            socket.setdefaulttimeout(old_timeout)
    except HttpError as e:
        logger.exception("Failed to build slides")
        return {"error": str(e), "presentation_id": presentation_id}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            return {
                "error": str(e),
                "hint": hint,
                "presentation_id": presentation_id,
                "customer": customer,
                "deck_id": deck_id,
            }
        raise
    return None
