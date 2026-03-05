"""Google Slides client for creating usage report decks."""

import json
import time
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .config import GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_DRIVE_FOLDER_ID, GOOGLE_DRIVE_OWNER_EMAIL, logger

SCOPES = [
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive",  # Full drive access for creating presentations
]
# Slide dimensions (16:9): 720pt x 405pt
SLIDE_WIDTH_PT = 720
SLIDE_HEIGHT_PT = 405


def _get_service():
    """Build authenticated Slides API service. Tries service account first, then Application Default Credentials (user login)."""
    creds = None
    creds_path = GOOGLE_APPLICATION_CREDENTIALS
    if creds_path:
        path = Path(creds_path)
        if path.exists():
            creds = service_account.Credentials.from_service_account_file(
                str(path), scopes=SCOPES
            )
            try:
                with open(path) as f:
                    proj_id = json.load(f).get("project_id")
                if proj_id:
                    creds = creds.with_quota_project(proj_id)
            except Exception:
                pass
            # Impersonate folder owner so files count against their Drive quota (domain-wide delegation)
            if GOOGLE_DRIVE_OWNER_EMAIL:
                owner = GOOGLE_DRIVE_OWNER_EMAIL.strip()
                if owner:
                    creds = creds.with_subject(owner)
                    logger.debug("Impersonating %s (domain-wide delegation)", owner)
            logger.debug("Using service account: %s", creds_path)
    if creds is None:
        try:
            import google.auth
            creds, _ = google.auth.default(scopes=SCOPES)
            logger.debug("Using Application Default Credentials (run: gcloud auth application-default login)")
        except Exception as e:
            raise ValueError(
                "No valid credentials. Either set GOOGLE_APPLICATION_CREDENTIALS to service account JSON, "
                "or run: gcloud auth application-default login"
            ) from e
    return build("slides", "v1", credentials=creds), build("drive", "v3", credentials=creds)


def _format_site_body(site: dict[str, Any]) -> str:
    """Format site usage metrics for slide body."""
    pv = site.get("page_views", 0)
    fc = site.get("feature_clicks", 0)
    ev = site.get("total_events", 0)
    mins = site.get("total_minutes", 0)
    return (
        f"Page views: {pv}\n"
        f"Feature clicks: {fc}\n"
        f"Total events: {ev}\n"
        f"Minutes: {mins}"
    )


def create_deck_for_customer(
    customer: str,
    sites: list[dict[str, Any]],
    days: int = 30,
) -> dict[str, Any]:
    """Create a Google Slide deck for a customer with one slide per site.

    Args:
        customer: Customer name (deck title prefix).
        sites: List of site dicts with sitename, page_views, feature_clicks, total_events, total_minutes.
        days: Lookback period (for title).

    Returns:
        {presentation_id, url, slides_created} or {error: str}
    """
    if not sites:
        return {"error": f"No sites for customer '{customer}'"}

    try:
        slides_service, drive_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    title = f"{customer} - Pendo Usage Report (Last {days} days)"
    try:
        # Create via Drive API (works when Slides API create returns 403)
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        if GOOGLE_DRIVE_FOLDER_ID:
            file_meta["parents"] = [GOOGLE_DRIVE_FOLDER_ID]
        file = drive_service.files().create(body=file_meta).execute()
        pres_id = file["id"]
        logger.info("Created presentation %s: %s", pres_id, title)
    except HttpError as e:
        err_str = str(e)
        err_lower = err_str.lower()
        # Check error_details for rate limit (Google can return 403 for quota in some APIs)
        details = getattr(e, "error_details", None) or []
        reason = ""
        if isinstance(details, list):
            for d in details:
                if isinstance(d, dict) and d.get("reason") in ("RATE_LIMIT_EXCEEDED", "rateLimitExceeded"):
                    reason = "rate_limit"
                    break
        # Google sometimes returns 403 for rate limit (not just 429)
        if reason == "rate_limit" or "rate" in err_lower or "quota" in err_lower or "ratelimitexceeded" in err_lower:
            logger.warning("Slides API rate limit: %s", err_str[:200])
            return {"error": f"Rate limit: {err_str}. Wait a few minutes and retry with max_customers (e.g. 30,active,2)."}
        logger.exception("Failed to create presentation")
        return {"error": err_str}

    requests = []
    insertion_index = 1  # After the default title slide

    for i, site in enumerate(sites):
        slide_id = f"slide_site_{i}"
        sitename = site.get("sitename", "(unknown)")
        body_text = _format_site_body(site)

        # Create slide (blank)
        requests.append({
            "createSlide": {
                "objectId": slide_id,
                "insertionIndex": insertion_index,
            }
        })
        insertion_index += 1

        # Title text box (site name)
        title_box_id = f"title_{i}"
        requests.append({
            "createShape": {
                "objectId": title_box_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": {"width": {"magnitude": 600, "unit": "PT"}, "height": {"magnitude": 50, "unit": "PT"}},
                    "transform": {"scaleX": 1, "scaleY": 1, "translateX": 60, "translateY": 40, "unit": "PT"},
                },
            }
        })
        requests.append({
            "insertText": {"objectId": title_box_id, "text": sitename, "insertionIndex": 0}
        })

        # Body text box (metrics)
        body_box_id = f"body_{i}"
        requests.append({
            "createShape": {
                "objectId": body_box_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": {"width": {"magnitude": 600, "unit": "PT"}, "height": {"magnitude": 280, "unit": "PT"}},
                    "transform": {"scaleX": 1, "scaleY": 1, "translateX": 60, "translateY": 100, "unit": "PT"},
                },
            }
        })
        requests.append({
            "insertText": {"objectId": body_box_id, "text": body_text, "insertionIndex": 0}
        })

    try:
        slides_service.presentations().batchUpdate(
            presentationId=pres_id,
            body={"requests": requests},
        ).execute()
    except HttpError as e:
        logger.exception("Failed to add slides")
        return {"error": str(e), "presentation_id": pres_id}

    url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
    return {
        "presentation_id": pres_id,
        "url": url,
        "customer": customer,
        "slides_created": len(sites),
    }


def create_decks_for_all_customers(
    by_customer: dict[str, list[dict[str, Any]]],
    customer_list: list[str],
    days: int = 30,
    delay_seconds: float = 2.0,
    max_customers: int | None = None,
) -> list[dict[str, Any]]:
    """Create one deck per customer. Returns list of {presentation_id, url, customer, slides_created} or {error}.
    delay_seconds: pause between decks to stay under Slides API quota (60 writes/min).
    max_customers: limit for testing (e.g. 2 to create only 2 decks).
    Stops early if first attempt fails with 403 (auth/permission issue).
    """
    customers = customer_list[:max_customers] if max_customers else customer_list
    results = []
    for i, customer in enumerate(customers):
        if i > 0:
            time.sleep(delay_seconds)
        sites = by_customer.get(customer, [])
        result = create_deck_for_customer(customer=customer, sites=sites, days=days)
        results.append(result)
        # Fail fast: if first deck fails with 403, don't retry hundreds more
        if "error" in result:
            err = str(result.get("error", ""))
            if "403" in err:
                if "Rate limit" in err:
                    msg = "Stopped: rate limit hit. Wait a few minutes, then retry with fewer customers (e.g. 30,active,2)."
                else:
                    msg = "Stopped: 403 permission error. Fix auth (see scripts/test_slides_auth.py) then retry."
                results.append({"error": msg, "customers_attempted": i + 1})
                break
    return results
