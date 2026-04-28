"""Deck registry, orchestration, and Google Slides API entry points."""

from __future__ import annotations

from typing import Any

from googleapiclient.errors import HttpError

from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID, logger
from .cs_report_client import get_csr_section
from .deck_builder_utils import (
    _build_slide_jql_speaker_notes,
    _normalize_builder_return,
    build_slide_jql_speaker_notes_for_entry,
    normalize_builder_return,
)
from .deck_composable import (
    _get_deck_output_folder,
    _slide_counter,
    add_slide,
    create_empty_deck,
)
from .deck_data_enrichment import enrich_deck_report_data
from .slide_cohort import (
    COHORT_FINDING_ROW_GAP_PT as _COHORT_FINDING_ROW_GAP_PT,
    COHORT_FINDING_ROW_H_PT as _COHORT_FINDING_ROW_H_PT,
    cohort_findings_rows_per_page as _cohort_findings_rows_per_page,
)
from .slide_cohort_links import (
    COHORT_BUNDLE_SIGNAL_LINK_PHRASES as _COHORT_BUNDLE_SIGNAL_LINK_PHRASES,
    apply_cohort_bundle_links_to_notable_signals,
)
from .slide_cs_notable import cs_notable_slide as _cs_notable_slide
from .deck_legacy import (
    create_deck_for_customer,
    create_decks_for_all_customers,
)
from .deck_variants import (
    create_cohort_deck,
    create_health_decks_for_customers,
    create_portfolio_deck,
)
from .slide_metadata import (
    DQ_SOURCE_LABEL_ORDER as _DQ_SOURCE_LABEL_ORDER,
    REPORT_KEY_TO_DQ_SOURCE as _REPORT_KEY_TO_DQ_SOURCE,
    ordered_dq_data_sources_for_slide_plan as _ordered_dq_data_sources_for_slide_plan,
)
from .slide_registry import (
    SLIDE_DATA_REQUIREMENTS,
    _SLIDE_BUILDERS,
    get_slide_builder,
    get_slide_data_requirements,
    slide_builder_names,
)
from .slides_api import (
    _get_service,
    _google_api_unreachable_hint,
    presentations_batch_update_chunked,
)
from .slide_salesforce import sf_category_records as _sf_category_records
from .slide_salesforce import sf_format_cell as _sf_format_cell
from .slide_salesforce import sf_records_to_table as _sf_records_to_table
from .speaker_notes import set_speaker_notes_batch
from .slide_pipeline_traces import (
    CANONICAL_PIPELINE_TRACES as _SLIDE_CANONICAL_PIPELINE_TRACES,
    cohort_findings_pipeline_traces as _cohort_findings_pipeline_traces,
    cohort_profile_pipeline_rows_for_block as _cohort_profile_pipeline_rows_for_block,
    cohort_profiles_pipeline_traces as _cohort_profiles_pipeline_traces,
    cohort_summary_pipeline_traces as _cohort_summary_pipeline_traces,
    cs_notable_pipeline_traces as _cs_notable_pipeline_traces,
    health_snapshot_pipeline_traces as _health_snapshot_pipeline_traces,
    peer_benchmarks_pipeline_traces as _peer_benchmarks_pipeline_traces,
    platform_risk_pipeline_traces as _platform_risk_pipeline_traces,
    platform_value_pipeline_traces as _platform_value_pipeline_traces,
    salesforce_pipeline_traces as _salesforce_pipeline_traces,
    support_health_exec_pipeline_traces as _support_health_exec_pipeline_traces,
)
from .slide_primitives import set_support_deck_corner_customer as _set_support_deck_corner_customer
from .slide_thumbnail_export import export_slide_thumbnails
from .slide_utils import slide_object_id_base as _slide_object_id_base
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

    try:
        import socket
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
        finally:
            socket.setdefaulttimeout(old_timeout)
    except HttpError as e:
        err_str = str(e)
        if "rate" in err_str.lower() or "quota" in err_str.lower():
            return {"error": f"Rate limit: {err_str}. Wait and retry."}
        return {"error": err_str}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            return {"error": str(e), "hint": hint, "customer": customer, "deck_id": deck_id}
        raise

    # Provide a DeckCharts instance for Slides embeds backed by Google Sheets.
    from .charts import DeckCharts
    report["_charts"] = DeckCharts(title)

    report["_slide_plan"] = slide_plan

    # Build every slide except "Notable" on the first pass; fetches are already in ``report`` for support.
    # The Notable slide (cs_notable) is inserted in a second batch at insertionIndex 1 after the LLM runs on a digest
    # of the same in-memory Jira data (so we do not refetch; bullets reflect the same dataset as the rest of the deck).
    plan_work: list[dict[str, Any]] = list(slide_plan)
    notable_deferred: dict[str, Any] | None = None
    if deck_id == "support":
        kept2: list[dict[str, Any]] = []
        for e in plan_work:
            if (e.get("slide_type") or e.get("id", "")) == "cs_notable" and notable_deferred is None:
                notable_deferred = e
            else:
                kept2.append(e)
        plan_work = kept2

    reqs: list[dict] = []
    idx = 1
    note_targets: list[tuple[str, dict[str, Any]]] = []
    if deck_id in ("support", "supply_chain_review") and customer:
        _set_support_deck_corner_customer(str(customer).strip())

    for entry in plan_work:
        slide_type = entry.get("slide_type", entry["id"])
        builder = _SLIDE_BUILDERS.get(slide_type)
        if not builder:
            logger.warning(
                "create_health_deck: no _SLIDE_BUILDERS entry for slide_type=%r (deck %s entry id=%r)",
                slide_type,
                deck_id,
                entry.get("id"),
            )
            continue
        report["_current_slide"] = entry
        sid = _slide_object_id_base(str(entry["id"]), idx)
        ret = builder(reqs, sid, report, idx)
        next_idx, note_ids = _normalize_builder_return(ret, sid)
        if slide_type == "cohort_profiles" and note_ids:
            blks = report.get("_cohort_profile_speaker_note_blocks") or []
            for i, nid in enumerate(note_ids):
                note_entry = dict(entry)
                if i < len(blks):
                    note_entry["_cohort_profile_block"] = blks[i]
                note_targets.append((nid, note_entry))
        else:
            for nid in note_ids:
                note_targets.append((nid, dict(entry)))
        idx = next_idx

    slides_created = idx - 1

    try:
        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Slides API
            pres = slides_service.presentations().get(presentationId=pres_id).execute()
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
                len(slide_plan),
            )
    except Exception:
        pass

    try:
        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(60.0)  # 60 second timeout for batchUpdate (can be large)
            presentations_batch_update_chunked(slides_service, pres_id, reqs)
        finally:
            socket.setdefaulttimeout(old_timeout)
    except HttpError as e:
        logger.exception("Failed to build slides")
        _set_support_deck_corner_customer(None)
        return {"error": str(e), "presentation_id": pres_id}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            _set_support_deck_corner_customer(None)
            return {"error": str(e), "hint": hint, "presentation_id": pres_id, "customer": customer, "deck_id": deck_id}
        raise

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

    if deck_id == "support" and notable_deferred and slides_created > 0:
        from .support_notable_llm import (
            NotableLlmError,
            build_support_review_digest,
            generate_notable_bullets_via_llm,
        )

        titles = [e.get("title", "") for e in plan_work]
        try:
            digest = build_support_review_digest(report, slide_titles=titles)
        except Exception as e:
            logger.warning("Notable: digest build failed; LLM may have thin context. %s", e)
            digest = {}
        ne = dict(notable_deferred)
        try:
            bullets, src = generate_notable_bullets_via_llm(digest, ne)
        except NotableLlmError as e:
            _set_support_deck_corner_customer(None)
            url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
            return {
                "error": str(e),
                "presentation_id": pres_id,
                "url": url,
                "customer": customer,
                "slides_created": slides_created,
                "deck_id": deck_id,
                "hint": "Notable slide was not added. The deck is otherwise complete. Set BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK=true to insert generic bullets, or fix the Notable/LLM path and regenerate.",
            }
        ne["notable_items"] = bullets
        report["support_notable_bullets"] = bullets
        report["support_notable_bullets_source"] = src
        report["_current_slide"] = ne
        nreq: list[dict] = []
        nsid = "s_snb1"
        ret_n = _cs_notable_slide(nreq, nsid, report, 1)
        _nidx, n_note_ids = _normalize_builder_return(ret_n, nsid)
        del _nidx
        try:
            import socket
            o2 = socket.getdefaulttimeout()
            try:
                socket.setdefaulttimeout(60.0)
                presentations_batch_update_chunked(slides_service, pres_id, nreq)
            finally:
                socket.setdefaulttimeout(o2)
        except HttpError as e:
            logger.error("Notable: second batch (insert at index 1) failed: %s", e)
        else:
            slides_created += 1
            for nid in n_note_ids:
                note_targets.append((nid, ne))

    _set_support_deck_corner_customer(None)
    notes_items = [(sid, _build_slide_jql_speaker_notes(report, entry)) for sid, entry in note_targets]
    if notes_items:
        n = set_speaker_notes_batch(slides_service, pres_id, notes_items)
        logger.info("Speaker notes: wrote %d/%d slide notes in single batchUpdate", n, len(notes_items))

    result = {
        "presentation_id": pres_id,
        "url": f"https://docs.google.com/presentation/d/{pres_id}/edit",
        "customer": customer,
        "slides_created": slides_created,
    }
    nsrc = report.get("support_notable_bullets_source")
    if nsrc:
        result["notable_bullets_source"] = nsrc

    if thumbnails:
        try:
            thumbs = export_slide_thumbnails(pres_id)
            result["thumbnails"] = [str(p) for p in thumbs]
            logger.info("Saved %d slide thumbnails for %s", len(thumbs), customer)
        except Exception as e:
            logger.warning("Thumbnail export failed: %s", e)

    return result
