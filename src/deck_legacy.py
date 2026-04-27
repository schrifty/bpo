"""Backward-compatible legacy deck creation helpers."""

from __future__ import annotations

import time

from googleapiclient.errors import HttpError

from .drive_config import get_deck_output_folder_id
from .slide_requests import append_text_box as _box
from .slides_api import _get_service, presentations_batch_update_chunked
from .slides_theme import _date_range


def create_deck_for_customer(customer, sites, days=30):
    if not sites:
        return {"error": f"No sites for '{customer}'"}
    try:
        slides_service, drive_service, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}
    title = f"{customer} - Usage Report ({_date_range(days)})"
    try:
        meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = get_deck_output_folder_id()
        if output_folder:
            meta["parents"] = [output_folder]
        f = drive_service.files().create(body=meta).execute()
        pid = f["id"]
    except HttpError as e:
        return {"error": str(e)}
    r = []
    ix = 1
    for i, s in enumerate(sites):
        sid = f"ls_{i}"
        r.append({"createSlide": {"objectId": sid, "insertionIndex": ix}})
        ix += 1
        _box(r, f"lt_{i}", sid, 60, 40, 600, 50, s.get("sitename", "?"))
        body = (
            f"Page views: {s.get('page_views', 0)}\n"
            f"Feature clicks: {s.get('feature_clicks', 0)}\n"
            f"Events: {s.get('total_events', 0)}\n"
            f"Minutes: {s.get('total_minutes', 0)}"
        )
        _box(r, f"lb_{i}", sid, 60, 100, 600, 280, body)
    try:
        presentations_batch_update_chunked(slides_service, pid, r)
    except HttpError as e:
        return {"error": str(e), "presentation_id": pid}
    return {
        "presentation_id": pid,
        "url": f"https://docs.google.com/presentation/d/{pid}/edit",
        "customer": customer,
        "slides_created": len(sites),
    }


def create_decks_for_all_customers(by_customer, customer_list, days=30, delay_seconds=2.0, max_customers=None):
    cs = customer_list[:max_customers] if max_customers else customer_list
    results = []
    for i, c in enumerate(cs):
        if i > 0:
            time.sleep(delay_seconds)
        results.append(create_deck_for_customer(c, by_customer.get(c, []), days))
        if "error" in results[-1] and "403" in str(results[-1].get("error", "")):
            results.append({"error": "Stopped: 403.", "customers_attempted": i + 1})
            break
    return results
