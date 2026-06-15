"""Deck registry, orchestration, and Google Slides API entry points."""

from __future__ import annotations

import re
from typing import Any

from .config import logger
from .deck_data_enrichment import SUPPORT_DECK_IDS, enrich_deck_report_data
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

_DECK_SORT_PREFIX_RE = re.compile(r"^\d+\s*-\s*")

# Drive file titles for portfolio-class decks (installer base / aggregated). Uses a stable
# ``Portfolio - …`` prefix regardless of YAML ``name`` (which may retain sort prefixes for CLI).
_PORTFOLIO_DRIVE_TITLE_TAIL: dict[str, str] = {
    "portfolio_review": "Health Review",
    "cohort_review": "Cohort Review",
    "engineering-portfolio": "Engineering Review",
    "implementations_review": "Implementations Review",
    "support_review_portfolio": "Support Review",
}


def _strip_deck_yaml_sort_prefix(name: str) -> str:
    s = _DECK_SORT_PREFIX_RE.sub("", (name or "").strip()).strip()
    return s or "Deck"


def _health_deck_presentation_title(
    *,
    deck_id: str,
    deck_name: str,
    date_str: str,
    customer: Any,
    report: dict[str, Any],
    is_portfolio: bool,
) -> str:
    """Build the Google Drive file name for ``create_health_deck``."""
    if deck_id == "csm_book_of_business":
        csm_disp = str(report.get("csm_owner") or "").strip() or "CSM"
        return f"Portfolio - {csm_disp} — Book of Business ({date_str})"

    tail = _PORTFOLIO_DRIVE_TITLE_TAIL.get(deck_id)
    if tail is not None:
        return f"Portfolio - {tail} ({date_str})"

    if deck_id in ("support", "support-kpis") and not customer:
        return f"{deck_name} — All Customers ({date_str})"

    if is_portfolio:
        suffix = _strip_deck_yaml_sort_prefix(deck_name)
        return f"Portfolio - {suffix} ({date_str})"

    return f"{customer} — {deck_name} ({date_str})"


# ── Monolith deck creation (deck-definition-driven) ──


def create_health_deck(
    report: dict[str, Any],
    deck_id: str = "cs_health_review",
    thumbnails: bool = True,
    output_folder_id: str | None = None,
    *,
    reset_drive_cache_stats: bool = True,
    log_drive_cache_stats: bool = True,
) -> dict[str, Any]:
    """Create a deck from a customer health report using a deck definition.

    Args:
        report: Full customer health report from PendoClient.get_customer_health_report().
        deck_id: Which deck definition to use. Defaults to 'cs_health_review'.
        thumbnails: Whether to export slide thumbnails. Disable for batch runs.
        output_folder_id: Optional Drive folder id for the new presentation. When omitted,
            uses ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` (if configured).
        reset_drive_cache_stats: When True (default), clear Drive JSON cache counters at deck start.
        log_drive_cache_stats: When True (default), log Pendo/integration cache hit rates after deck build.
    """
    from .drive_cache_stats import drive_cache_stats_scope

    log_label = f"create_health_deck({deck_id})" if log_drive_cache_stats else None
    with drive_cache_stats_scope(reset=reset_drive_cache_stats, log_label=log_label):
        if "error" in report:
            return {"error": report["error"]}

        is_portfolio = report.get("type") == "portfolio"
        # Preserve None for "all customers" case; only default to "Portfolio" for actual portfolio reports
        if is_portfolio:
            customer = "Portfolio"
        else:
            customer = report.get("customer")  # Can be None for "all customers"
        if deck_id == "support_review_portfolio":
            customer = None
            report["customer"] = None
        if deck_id == "csm_book_of_business":
            # Portfolio-shaped report; title uses CSM name, not the literal "Portfolio" label.
            report["type"] = report.get("type") or "portfolio"
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
        report["_deck_id"] = deck_id

        from .deck_loader import resolve_deck

        # resolve_deck loads only slide YAMLs referenced by this deck (not the full slides/ catalog).
        resolved = resolve_deck(deck_id, customer)
        if resolved.get("error"):
            return {"error": resolved["error"]}

        deck_name = resolved.get("name", "Health Review")
        date_str = _date_range(days, quarter_label, report.get("quarter_start"), report.get("quarter_end"))

        slide_plan: list[dict[str, Any]] = list(resolved.get("slides") or [])

        # For support deck without customer, include full support slide lineup with all-project scope.
        title = _health_deck_presentation_title(
            deck_id=deck_id,
            deck_name=str(deck_name),
            date_str=date_str,
            customer=customer,
            report=report,
            is_portfolio=is_portfolio,
        )

        report, slide_plan = enrich_deck_report_data(deck_id, report, slide_plan, customer)

        if deck_id in ("portfolio_review", "csm_book_of_business") and is_portfolio:
            from .signals_llm import maybe_rewrite_portfolio_signals_with_llm

            maybe_rewrite_portfolio_signals_with_llm(report, deck_id=deck_id)

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

        if deck_id in ("support", "support-kpis", "supply_chain_review") and customer:
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
