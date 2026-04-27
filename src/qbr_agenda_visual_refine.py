"""Post-hydrate visual QA loop for QBR agenda slides (template path only).

After ``adapt_custom_slides``, optionally re-run adapt for the agenda page with extra instructions
until a fast vision model marks the slide thumbnail as acceptable, or ``max_refinements`` is hit.
"""

from __future__ import annotations

import json
from typing import Any

from .config import LLM_MODEL, logger

# Log prefix for the agenda thumbnail → vision → optional re-adapt cycle (grep-friendly).
_QBR_VCYCLE = "QBR agenda visual cycle"
from .evaluate import (
    _add_incomplete_banner,
    _apply_adaptations,
    _build_data_summary,
    _build_hydrate_speaker_notes,
    _build_qbr_agenda_reshorten_replacements,
    _download_thumbnail_b64,
    _ensure_charts_and_images_marked,
    _extract_slide_text_elements,
    _get_data_replacements,
    _get_slide_thumbnail_url,
    _merge_qbr_agenda_title_replacements,
    _qbr_agenda_hydrate_config,
    _qbr_agenda_title_bar_shape_object_ids,
    _red_style_placeholders,
    _sanitize_adapt_replacements_percent_semantics,
    _sanitize_adapt_replacements_plausible_years,
    _shape_autofit_none_requests,
    _should_add_incomplete_banner,
    _slide_matches_qbr_agenda_hydrate,
    _slide_metric_font_clamp_requests,
    apply_synonym_resolution_to_replacements,
)
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence
from .speaker_notes import set_speaker_notes
from .slides_client import slides_presentations_batch_update


_QBR_AGENDA_REFINEMENT_RULES = """REFINEMENT PASS — prior thumbnail review reported issues:
{feedback}

Fix layout/readability: use shorter replacement text (truncate long section titles with "…" if needed).
Avoid long numbers or dense strings in small agenda rows. Prefer mapped=false with [???] over wrong values.
Do not invent data. Keep agenda section titles readable and non-overlapping where possible."""


def _find_page_element_recursive(page_elements: list[dict], oid: str) -> dict | None:
    for el in page_elements or []:
        if el.get("objectId") == oid:
            return el
        grp = el.get("elementGroup", {})
        if grp:
            found = _find_page_element_recursive(grp.get("children") or [], oid)
            if found:
                return found
    return None


def _layout_hints_from_vision_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize ``layout_hints`` from vision JSON (optional)."""
    raw = data.get("layout_hints")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    for k in ("font_size_pt", "widen_title_boxes_percent", "max_chars_scale"):
        if k in raw and raw[k] is not None:
            out[k] = raw[k]
    sm = raw.get("shorten_mode")
    if isinstance(sm, str) and sm.strip():
        out["shorten_mode"] = sm.strip().lower()
    return out


def _apply_qbr_agenda_layout_hints_from_vision(
    slides_svc,
    pres_id: str,
    page_id: str,
    slide: dict,
    report: dict,
    hints: dict[str, Any],
) -> None:
    """Widen title boxes + lower font via Slides API; set ``report`` keys for reshorten merge."""
    if not hints:
        return
    te = _extract_slide_text_elements(slide.get("pageElements", []))
    oids = _qbr_agenda_title_bar_shape_object_ids(te, report)
    reqs: list[dict[str, Any]] = []
    widen = hints.get("widen_title_boxes_percent")
    font_pt = hints.get("font_size_pt")
    if oids and widen is not None:
        try:
            p = float(widen)
            if 0 < p <= 50:
                factor = 1.0 + p / 100.0
                for oid in oids:
                    el = _find_page_element_recursive(slide.get("pageElements", []), oid)
                    if not el:
                        continue
                    size = el.get("size") or {}
                    w_mag = float(size.get("width", {}).get("magnitude") or 0)
                    h_mag = float(size.get("height", {}).get("magnitude") or 0)
                    if w_mag <= 0 or h_mag <= 0:
                        continue
                    reqs.append({
                        "updatePageElementProperties": {
                            "objectId": oid,
                            "pageElementProperties": {
                                "size": {
                                    "width": {"magnitude": round(w_mag * factor), "unit": "EMU"},
                                    "height": {"magnitude": round(h_mag), "unit": "EMU"},
                                },
                            },
                            "fields": "size",
                        }
                    })
        except (TypeError, ValueError):
            pass
    if not oids and (widen is not None or font_pt is not None):
        logger.warning(
            "%s: layout hints include widen/font but no title shape ids (set after first merge); "
            "skipping box/font API",
            _QBR_VCYCLE,
        )
    if oids and font_pt is not None:
        try:
            pt = float(font_pt)
            pt = max(8.0, min(18.0, pt))
            for oid in oids:
                reqs.append({
                    "updateTextStyle": {
                        "objectId": oid,
                        "textRange": {"type": "ALL"},
                        "style": {"fontSize": {"magnitude": pt, "unit": "PT"}},
                        "fields": "fontSize",
                    }
                })
        except (TypeError, ValueError):
            pass
    if reqs:
        try:
            slides_presentations_batch_update(slides_svc, pres_id, reqs)
            logger.info(
                "%s: applied %s layout hint request(s) (widen/font)",
                _QBR_VCYCLE,
                len(reqs),
            )
        except Exception as e:
            logger.warning("%s: layout hints batch failed: %s", _QBR_VCYCLE, e)

    sm = hints.get("shorten_mode")
    if isinstance(sm, str) and sm.strip():
        report["_qbr_agenda_shorten_mode_override"] = sm.strip().lower()
    mcs = hints.get("max_chars_scale")
    if mcs is not None:
        try:
            report["_qbr_agenda_max_chars_scale"] = float(mcs)
        except (TypeError, ValueError):
            pass


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
            logger.info(
                "%s: matched qbr_agenda slide objectId=%s (will run view/review if enabled)",
                _QBR_VCYCLE,
                pid,
            )
            return pid
    return None


def _qbr_agenda_visual_quality_ok(
    oai, thumb_b64: str | None, *, review_label: str = "review"
) -> tuple[bool, str, dict[str, Any]]:
    """Return (passes, issues text, layout_hints dict).

    ``layout_hints`` may be empty when ok=true. When ok=false, the model should suggest
    concrete layout fixes (font size, widen boxes, shortening strategy) the app can apply.
    """
    if not thumb_b64:
        logger.warning(
            "%s: %s — no thumbnail; cannot run vision review",
            _QBR_VCYCLE,
            review_label,
        )
        return False, "Slide thumbnail unavailable; visual QA could not run.", {}
    system = (
        "You evaluate ONE slide image (QBR agenda / section list) for a customer-facing deck. "
        "Be strict: set ok=false if any of these apply: text overlaps other text or shapes, "
        "labels are stacked or crowded so they are hard to read, long strings overflow or "
        "crowd small rows, numbers or titles are illegible at normal viewing distance, "
        "or the layout looks chaotic. "
        "A few [???] placeholders alone are acceptable when data is missing; ok=false if "
        "[???] appears together with severe crowding or overlap. "
        "When in doubt between acceptable and not, choose ok=false. "
        "When ok=false, you MUST also fill layout_hints with concrete suggestions so the app "
        "can fix the slide: font_size_pt (8–18, smaller if crowded), widen_title_boxes_percent "
        "(0–35, increase column width for title rows), shorten_mode (one of: acronym, "
        "first_word, first_two_words — acronym is best for long multi-word section names), "
        "and optionally max_chars_scale (0.5–1.0 to tighten character budget). "
        "Return ONLY JSON: {\"ok\": true or false, \"issues\": \"short English\", "
        "\"layout_hints\": {\"font_size_pt\": number, \"widen_title_boxes_percent\": number, "
        "\"shorten_mode\": \"acronym\", \"max_chars_scale\": 0.7}} "
        "layout_hints may be {} when ok=true."
    )
    try:
        resp = _llm_create_with_retry(
            oai,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=500,
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
                        {
                            "type": "text",
                            "text": (
                                "Would you ship this slide as-is to an executive audience? "
                                "If there is overlap, unreadable density, or unclear labels, answer ok=false."
                            ),
                        },
                    ],
                },
            ],
        )
        raw = resp.choices[0].message.content
        data = json.loads(_strip_json_code_fence(raw or "{}"))
        ok = bool(data.get("ok"))
        issues = str(data.get("issues") or "").strip()[:1200]
        hints = _layout_hints_from_vision_payload(data)
        logger.info(
            "%s: %s — vision verdict ok=%s issues=%s",
            _QBR_VCYCLE,
            review_label,
            ok,
            (issues or "(none)")[:400],
        )
        if hints:
            logger.info("%s: %s — layout_hints=%s", _QBR_VCYCLE, review_label, hints)
        return ok, issues, hints
    except Exception as e:
        logger.warning("QBR agenda visual QA failed (%s) — not treating as pass", e)
        return False, f"Vision QA error: {e}", {}


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
    logger.info(
        "%s: hydrate apply slide %s — %s replace batch request(s), unmapped=%s",
        _QBR_VCYCLE,
        slide_num,
        len(replace_reqs),
        has_unmapped,
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
                    af_reqs = _shape_autofit_none_requests(
                        _qbr_agenda_title_bar_shape_object_ids(text_elements, report)
                    )
                    if af_reqs:
                        try:
                            slides_presentations_batch_update(slides_svc, pres_id, af_reqs)
                        except Exception as ae:
                            logger.warning(
                                "QBR agenda refine: title-bar autofit NONE failed slide %s: %s",
                                slide_num,
                                ae,
                            )
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
    slide_idx = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
    data_summary = _build_data_summary(report)
    max_r = cfg["max_refinements"]

    logger.info(
        "%s: start presentation=%s page_id=%s slide_index=%s max_refinements=%s model=%s",
        _QBR_VCYCLE,
        pres_id,
        page_id,
        slide_idx,
        max_r,
        LLM_MODEL,
    )

    def _thumb(phase: str) -> str | None:
        logger.info("%s: fetch thumbnail (%s) page_id=%s", _QBR_VCYCLE, phase, page_id)
        try:
            url = _get_slide_thumbnail_url(slides_svc, pres_id, page_id)
            b64 = _download_thumbnail_b64(url)
            if b64:
                logger.info(
                    "%s: thumbnail ok (%s) base64_len=%s",
                    _QBR_VCYCLE,
                    phase,
                    len(b64),
                )
            else:
                logger.warning("%s: thumbnail empty after download (%s)", _QBR_VCYCLE, phase)
            return b64
        except Exception as e:
            logger.warning("%s: thumbnail failed (%s): %s", _QBR_VCYCLE, phase, e)
            return None

    thumb_b64 = _thumb("initial_review")
    ok, issues, layout_hints = _qbr_agenda_visual_quality_ok(
        oai, thumb_b64, review_label="initial_review"
    )
    if ok:
        logger.info(
            "%s: done — pass on first review (no refinement passes)",
            _QBR_VCYCLE,
        )
        return {
            "enabled": True,
            "skipped": False,
            "passed": True,
            "refinements_used": 0,
            "last_issues": "",
        }

    logger.info(
        "%s: initial review did not pass — entering refinement (up to %s pass(es)). Issues: %s",
        _QBR_VCYCLE,
        max_r,
        (issues or "?")[:400],
    )

    refinements_used = 0
    feedback = issues or "Overlapping or unreadable text; shorten values and reduce density."

    if layout_hints:
        slides_by_id = {s["objectId"]: s for s in pres.get("slides", [])}
        slide0 = slides_by_id.get(page_id)
        if slide0:
            _apply_qbr_agenda_layout_hints_from_vision(
                slides_svc, pres_id, page_id, slide0, report, layout_hints
            )
            pres = slides_svc.presentations().get(presentationId=pres_id).execute()
            ordered_ids = [s["objectId"] for s in pres.get("slides", [])]
            slides_by_id = {s["objectId"]: s for s in pres.get("slides", [])}
            slide1 = slides_by_id.get(page_id)
            if slide1:
                text_elements0 = _extract_slide_text_elements(slide1.get("pageElements", []))
                reshort = _build_qbr_agenda_reshorten_replacements(text_elements0, report)
                if reshort:
                    logger.info(
                        "%s: applying %s reshorten replacement(s) from vision hints",
                        _QBR_VCYCLE,
                        len(reshort),
                    )
                    _apply_single_page_hydrate(
                        slides_svc,
                        pres_id,
                        page_id,
                        reshort,
                        text_elements0,
                        report,
                        data_summary,
                        oai,
                        title_slide_object_id=title_slide_object_id,
                        ordered_ids=ordered_ids,
                        analysis=None,
                    )
            thumb_b64 = _thumb("after_layout_hints")
            ok2, issues2, _hints2 = _qbr_agenda_visual_quality_ok(
                oai, thumb_b64, review_label="post_layout_hints"
            )
            if ok2:
                logger.info(
                    "%s: done — pass after vision layout_hints + reshorten (no LLM refinement passes)",
                    _QBR_VCYCLE,
                )
                return {
                    "enabled": True,
                    "skipped": False,
                    "passed": True,
                    "refinements_used": 0,
                    "last_issues": "",
                    "layout_hints_applied": True,
                }
            feedback = issues2 or feedback

    for _ in range(max_r):
        refinements_used += 1
        logger.info(
            "%s: refinement pass %s/%s starting",
            _QBR_VCYCLE,
            refinements_used,
            max_r,
        )
        pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        slides_by_id = {s["objectId"]: s for s in pres.get("slides", [])}
        slide = slides_by_id.get(page_id)
        if not slide:
            logger.warning(
                "%s: pass %s — slide objectId missing from presentation; stopping refinement",
                _QBR_VCYCLE,
                refinements_used,
            )
            break
        text_elements = _extract_slide_text_elements(slide.get("pageElements", []))
        if not text_elements:
            logger.warning(
                "%s: pass %s — no text elements on slide; stopping refinement",
                _QBR_VCYCLE,
                refinements_used,
            )
            break
        thumb_b64 = _thumb(f"refine_{refinements_used}_before_adapt")
        extra = _QBR_AGENDA_REFINEMENT_RULES.format(feedback=feedback)
        replacements = _get_data_replacements(
            oai,
            text_elements,
            data_summary,
            thumb_b64,
            slide_label=f"qbr-agenda-refine-{refinements_used}",
            extra_system_rules=extra,
        )
        _slab = f"qbr-agenda-refine-{refinements_used}"
        replacements = apply_synonym_resolution_to_replacements(
            replacements, text_elements, data_summary, slide_ref=_slab
        )
        replacements = _sanitize_adapt_replacements_plausible_years(
            replacements, slide_ref=_slab
        )
        replacements = _sanitize_adapt_replacements_percent_semantics(
            replacements, text_elements, slide_ref=_slab
        )
        replacements = _ensure_charts_and_images_marked(text_elements, replacements)
        replacements = _merge_qbr_agenda_title_replacements(text_elements, replacements, report)

        logger.info(
            "%s: pass %s — adapt produced %s replacement rule(s) (0 means slide likely unchanged)",
            _QBR_VCYCLE,
            refinements_used,
            len(replacements),
        )
        if not replacements:
            logger.warning(
                "%s: pass %s — no replacements; skipping hydrate apply (slide text unchanged this pass)",
                _QBR_VCYCLE,
                refinements_used,
            )
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

        thumb_b64 = _thumb(f"refine_{refinements_used}_after_apply")
        ok, issues, _hints_after = _qbr_agenda_visual_quality_ok(
            oai,
            thumb_b64,
            review_label=f"after_refinement_pass_{refinements_used}",
        )
        if ok:
            logger.info(
                "%s: done — pass after %s refinement pass(es)",
                _QBR_VCYCLE,
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
        "%s: finished — still not passing after %s refinement(s). Last vision issues: %s",
        _QBR_VCYCLE,
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
