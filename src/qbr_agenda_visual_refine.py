"""Post-hydrate visual QA loop for QBR agenda slides (template path only).

After ``adapt_custom_slides``, optionally re-run adapt for the agenda page with extra instructions
until a fast vision model marks the slide thumbnail as acceptable, or ``max_refinements`` is hit.
"""

from __future__ import annotations

import json
from typing import Any

from .config import LLM_MODEL, logger
from .evaluate import (
    _add_incomplete_banner,
    _apply_adaptations,
    _build_data_summary,
    _build_hydrate_speaker_notes,
    _download_thumbnail_b64,
    _ensure_charts_and_images_marked,
    _extract_slide_text_elements,
    _get_data_replacements,
    _get_slide_thumbnail_url,
    _merge_qbr_agenda_title_replacements,
    _qbr_agenda_hydrate_config,
    _red_style_placeholders,
    _sanitize_adapt_replacements_percent_semantics,
    _sanitize_adapt_replacements_plausible_years,
    _should_add_incomplete_banner,
    _slide_matches_qbr_agenda_hydrate,
    _slide_metric_font_clamp_requests,
    apply_synonym_resolution_to_replacements,
)
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence
from .slides_client import set_speaker_notes, slides_presentations_batch_update


_QBR_AGENDA_REFINEMENT_RULES = """REFINEMENT PASS — prior thumbnail review reported issues:
{feedback}

Fix layout/readability: use shorter replacement text (truncate long section titles with "…" if needed).
Avoid long numbers or dense strings in small agenda rows. Prefer mapped=false with [???] over wrong values.
Do not invent data. Keep agenda section titles readable and non-overlapping where possible."""


def _qbr_agenda_visual_refinement_config(report: dict) -> dict[str, Any]:
    h = _qbr_agenda_hydrate_config(report)
    vr = (h.get("template") or {}).get("visual_refinement") or {}
    if not isinstance(vr, dict):
        return {"enabled": False, "max_refinements": 0}
    try:
        mx = int(vr.get("max_refinements", 2))
    except (TypeError, ValueError):
        mx = 2
    return {
        "enabled": bool(vr.get("enabled", True)),
        "max_refinements": max(0, min(mx, 5)),
    }


def find_qbr_agenda_page_id(
    slides_svc,
    pres_id: str,
    adapt_page_ids: list[str],
    report: dict,
) -> str | None:
    """Return the objectId of the slide that matches ``qbr_agenda`` hydrate detection, if any."""
    ag = _qbr_agenda_hydrate_config(report)
    try:
        pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    except Exception as e:
        logger.warning("QBR agenda refine: could not read presentation: %s", e)
        return None
    slides_by_id = {s["objectId"]: s for s in pres.get("slides", [])}
    for pid in adapt_page_ids:
        slide = slides_by_id.get(pid)
        if not slide:
            continue
        te = _extract_slide_text_elements(slide.get("pageElements", []))
        if _slide_matches_qbr_agenda_hydrate(te, ag):
            return pid
    return None


def _qbr_agenda_visual_quality_ok(oai, thumb_b64: str | None) -> tuple[bool, str]:
    """Return (passes, issues text). If no thumbnail, pass to avoid blocking."""
    if not thumb_b64:
        return True, ""
    system = (
        "You evaluate ONE slide image (QBR agenda / section list). "
        "Decide if it is acceptable for a customer-facing deck: text should be readable, "
        "not severely overlapping or stacked illegibly, and not a chaotic jumble of labels. "
        "Some [???] placeholders are acceptable if data is missing. "
        "Return ONLY JSON: {\"ok\": true or false, \"issues\": \"short English\"}"
    )
    try:
        resp = _llm_create_with_retry(
            oai,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=400,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"},
                        },
                        {"type": "text", "text": "Is this agenda slide visually acceptable?"},
                    ],
                },
            ],
        )
        raw = resp.choices[0].message.content
        data = json.loads(_strip_json_code_fence(raw or "{}"))
        ok = bool(data.get("ok"))
        issues = str(data.get("issues") or "").strip()[:1200]
        return ok, issues
    except Exception as e:
        logger.warning("QBR agenda visual QA failed (%s) — treating as pass", e)
        return True, ""


def _apply_single_page_hydrate(
    slides_svc,
    pres_id: str,
    page_id: str,
    replacements: list[dict],
    text_elements: list[dict],
    report: dict,
    data_summary: dict,
    oai,
    *,
    title_slide_object_id: str | None,
    ordered_ids: list[str],
    analysis: dict | None,
) -> None:
    """Apply replacements, font clamp, incomplete styling, speaker notes for one slide."""
    slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
    replace_reqs, has_unmapped, has_static_images = _apply_adaptations(
        slides_svc, pres_id, page_id, replacements
    )
    if replace_reqs:
        try:
            slides_presentations_batch_update(slides_svc, pres_id, replace_reqs)
            try:
                pres_fresh = slides_svc.presentations().get(presentationId=pres_id).execute()
                slide_fresh = next(
                    (s for s in pres_fresh.get("slides", []) if s.get("objectId") == page_id),
                    None,
                )
                if slide_fresh:
                    clamp_reqs = _slide_metric_font_clamp_requests(slide_fresh, replacements)
                    if clamp_reqs:
                        slides_presentations_batch_update(slides_svc, pres_id, clamp_reqs)
            except Exception as e:
                logger.warning(
                    "QBR agenda refine: font clamp failed slide %s: %s",
                    slide_num,
                    e,
                )
        except Exception as e:
            logger.warning("QBR agenda refine: replace failed slide %s: %s", slide_num, e)
            return

    if has_unmapped:
        style_reqs = _red_style_placeholders(slides_svc, pres_id, page_id)
        if _should_add_incomplete_banner(page_id, replacements, title_slide_object_id, analysis):
            style_reqs.extend(_add_incomplete_banner(page_id, has_static_images=has_static_images))
        if style_reqs:
            try:
                slides_presentations_batch_update(slides_svc, pres_id, style_reqs)
            except Exception as e:
                logger.warning("QBR agenda refine: style/banner failed: %s", e)

    notes = _build_hydrate_speaker_notes(
        replacements,
        text_elements,
        report=report,
        data_summary=data_summary,
        has_unmapped=has_unmapped,
        has_static_images=has_static_images,
        analysis=analysis,
        slide_title=(analysis or {}).get("title") if analysis else None,
        oai=oai,
    )
    set_speaker_notes(slides_svc, pres_id, page_id, notes)


def run_qbr_agenda_visual_refinement_loop(
    slides_svc,
    pres_id: str,
    page_id: str,
    report: dict,
    oai,
    *,
    title_slide_object_id: str | None = None,
) -> dict[str, Any]:
    """Thumbnail-based QA + up to ``max_refinements`` re-adapt passes for the agenda slide only.

    Returns stats: ``enabled``, ``skipped``, ``passed``, ``refinements_used``, ``last_issues``.
    """
    cfg = _qbr_agenda_visual_refinement_config(report)
    if not cfg["enabled"] or cfg["max_refinements"] <= 0:
        return {
            "enabled": False,
            "skipped": True,
            "passed": True,
            "refinements_used": 0,
            "last_issues": "",
        }

    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    ordered_ids = [s["objectId"] for s in pres.get("slides", [])]
    data_summary = _build_data_summary(report)
    max_r = cfg["max_refinements"]

    def _thumb() -> str | None:
        try:
            url = _get_slide_thumbnail_url(slides_svc, pres_id, page_id)
            return _download_thumbnail_b64(url)
        except Exception as e:
            logger.warning("QBR agenda refine: thumbnail failed: %s", e)
            return None

    thumb_b64 = _thumb()
    ok, issues = _qbr_agenda_visual_quality_ok(oai, thumb_b64)
    if ok:
        logger.info("QBR agenda visual QA: pass (no refinement needed)")
        return {
            "enabled": True,
            "skipped": False,
            "passed": True,
            "refinements_used": 0,
            "last_issues": "",
        }

    logger.info("QBR agenda visual QA: issues — %s", (issues or "?")[:300])

    refinements_used = 0
    feedback = issues or "Overlapping or unreadable text; shorten values and reduce density."

    for _ in range(max_r):
        refinements_used += 1
        pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        slides_by_id = {s["objectId"]: s for s in pres.get("slides", [])}
        slide = slides_by_id.get(page_id)
        if not slide:
            break
        text_elements = _extract_slide_text_elements(slide.get("pageElements", []))
        if not text_elements:
            break
        thumb_b64 = _thumb()
        extra = _QBR_AGENDA_REFINEMENT_RULES.format(feedback=feedback)
        replacements = _get_data_replacements(
            oai,
            text_elements,
            data_summary,
            thumb_b64,
            slide_label=f"qbr-agenda-refine-{refinements_used}",
            extra_system_rules=extra,
        )
        replacements = apply_synonym_resolution_to_replacements(
            replacements, text_elements, data_summary
        )
        replacements = _sanitize_adapt_replacements_plausible_years(replacements)
        replacements = _sanitize_adapt_replacements_percent_semantics(replacements, text_elements)
        replacements = _ensure_charts_and_images_marked(text_elements, replacements)
        replacements = _merge_qbr_agenda_title_replacements(text_elements, replacements, report)

        if not replacements:
            logger.warning("QBR agenda refine: no replacements on pass %s", refinements_used)
        else:
            _apply_single_page_hydrate(
                slides_svc,
                pres_id,
                page_id,
                replacements,
                text_elements,
                report,
                data_summary,
                oai,
                title_slide_object_id=title_slide_object_id,
                ordered_ids=ordered_ids,
                analysis=None,
            )

        thumb_b64 = _thumb()
        ok, issues = _qbr_agenda_visual_quality_ok(oai, thumb_b64)
        if ok:
            logger.info(
                "QBR agenda visual QA: pass after %s refinement pass(es)",
                refinements_used,
            )
            return {
                "enabled": True,
                "skipped": False,
                "passed": True,
                "refinements_used": refinements_used,
                "last_issues": "",
            }
        feedback = issues or feedback

    logger.warning(
        "QBR agenda visual QA: still not passing after %s refinement(s). Last issues: %s",
        refinements_used,
        (issues or "")[:400],
    )
    return {
        "enabled": True,
        "skipped": False,
        "passed": False,
        "refinements_used": refinements_used,
        "last_issues": issues or feedback,
    }
