"""Deck registry, orchestration, and Google Slides API entry points."""

from __future__ import annotations

from typing import Any

from .config import logger
from .deck_data_enrichment import enrich_deck_report_data
from .slides_api import _get_service
from .deck_presentation_api import (
    append_default_slide_delete_if_needed,
    create_presentation,
    submit_slide_requests,
)
from .deck_finalizer import finalize_health_deck
from .deck_renderer import render_slide_plan
from .deck_support_notable import insert_support_notable_slide
from .slide_primitives import set_support_deck_corner_customer as _set_support_deck_corner_customer
from .slides_theme import _date_range

# ── Monolith deck creation (deck-definition-driven) ──

def create_health_deck(
    report: dict[str, Any],
    deck_id: str = "cs_health_review",
    thumbnails: bool = True,
    output_folder_id: str | None = None,
) -> dict[str, Any]:
    """Create a deck from a customer health report using a deck definition.

    Args:
        report: Full customer health report from PendoClient.get_customer_health_report().
        deck_id: Which deck definition to use. Defaults to 'cs_health_review'.
        thumbnails: Whether to export slide thumbnails. Disable for batch runs.
        output_folder_id: Optional Drive folder id for the new presentation. When omitted,
            uses ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` (if configured).
    """
    if "error" in report:
        return {"error": report["error"]}

    is_portfolio = report.get("type") == "portfolio"
    # Preserve None for "all customers" case; only default to "Portfolio" for actual portfolio reports
    if is_portfolio:
        customer = "Portfolio"
    else:
        customer = report.get("customer")  # Can be None for "all customers"
    days = report.get("days", 30)
    quarter_label = report.get("quarter")

    from .qa import qa
    qa.begin(customer)

    try:
        slides_service, drive_service, sheets_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    # Make services accessible to slide builders via the report dict
    report["_slides_svc"] = slides_service
    report["_drive_svc"] = drive_service

    from .deck_loader import resolve_deck

    # resolve_deck loads only slide YAMLs referenced by this deck (not the full slides/ catalog).
    resolved = resolve_deck(deck_id, customer)
    if resolved.get("error"):
        return {"error": resolved["error"]}

    deck_name = resolved.get("name", "Health Review")
    date_str = _date_range(days, quarter_label, report.get("quarter_start"), report.get("quarter_end"))
    
    slide_plan: list[dict[str, Any]] = list(resolved.get("slides") or [])
    
    # For support deck without customer, include full support slide lineup with all-project scope.
    if deck_id == "support" and not customer:
        title = f"{deck_name} — All Customers ({date_str})"
    elif is_portfolio:
        title = f"{deck_name} ({date_str})"
    else:
        title = f"{customer} — {deck_name} ({date_str})"

    report, slide_plan = enrich_deck_report_data(deck_id, report, slide_plan, customer)

    if not slide_plan:
        logger.error(
            "create_health_deck: empty slide plan (deck_id=%s customer=%r). "
            "Check decks/*.yaml vs slides/, Drive BPO/QBR Generator sync, and per-customer slide filters.",
            deck_id,
            customer,
        )
        return {
            "error": "Deck has no slides to generate (resolved plan is empty).",
            "hint": "Verify deck YAML slide IDs exist in slides/. If using Drive config, ensure "
            "BPO/QBR Generator decks/ and slides/ on Drive match the repo. Slides with customers: [...] exclude "
            "everyone except listed customers.",
            "customer": customer,
            "deck_id": deck_id,
        }

    pres_id, create_error = create_presentation(drive_service, title, output_folder_id)
    if create_error:
        create_error.setdefault("customer", customer)
        create_error.setdefault("deck_id", deck_id)
        return create_error

    # Provide a DeckCharts instance for Slides embeds backed by Google Sheets.
    from .charts import DeckCharts
    report["_charts"] = DeckCharts(title)

    report["_slide_plan"] = slide_plan

    if deck_id in ("support", "supply_chain_review") and customer:
        _set_support_deck_corner_customer(str(customer).strip())
    reqs, slides_created, note_targets, notable_deferred, plan_work = render_slide_plan(
        report,
        slide_plan,
        deck_id,
    )

    append_default_slide_delete_if_needed(
        slides_service,
        pres_id,
        reqs,
        slides_created,
        deck_id,
        customer,
        len(slide_plan),
    )

    submit_error = submit_slide_requests(slides_service, pres_id, reqs, customer, deck_id)
    if submit_error:
        _set_support_deck_corner_customer(None)
        return submit_error

    if slides_created == 0:
        _set_support_deck_corner_customer(None)
        url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
        return {
            "error": "No slides were built — every slide_type may be unknown or builders returned nothing.",
            "hint": "See logs for slide_type warnings. Compare slides/*.yaml slide_type to src/slides_client.py _SLIDE_BUILDERS.",
            "presentation_id": pres_id,
            "url": url,
            "customer": customer,
            "slides_created": 0,
        }

    slides_created, note_targets, notable_error = insert_support_notable_slide(
        slides_service,
        pres_id,
        report,
        notable_deferred,
        plan_work,
        note_targets,
        slides_created,
        customer,
        deck_id,
    )
    if notable_error:
        _set_support_deck_corner_customer(None)
        return notable_error

    _set_support_deck_corner_customer(None)
    return finalize_health_deck(
        slides_service,
        pres_id,
        report,
        note_targets,
        customer,
        slides_created,
        thumbnails=thumbnails,
    )
