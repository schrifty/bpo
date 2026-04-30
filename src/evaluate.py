"""Evaluate and hydrate Google Slides shared with the configured intake Google Group.

Evaluate: lists files shared with GOOGLE_HYDRATE_INTAKE_GROUP, exports thumbnails,
extracts text/elements, and asks GPT-4o to assess reproducibility.

Hydrate: classifies each slide in a source deck against our builder types,
then regenerates the deck using live customer data from Pendo/Jira/CS Report.
"""

from __future__ import annotations

import datetime
import functools
import json
import re
import secrets
import sys
import tempfile
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import yaml

from .config import (
    GOOGLE_QBR_GENERATOR_FOLDER_ID,
    GOOGLE_HYDRATE_INTAKE_GROUP,
    HYDRATE_MAX_SLIDES,
    HYDRATE_REMOVE_INTAKE_GROUP_PERMISSION,
    LLM_MODEL,
    LLM_MODEL_FAST,
    llm_client,
    logger,
)
from .data_field_synonyms import (
    apply_synonym_resolution_to_replacements,
    data_summary_lookup,
    data_summary_path_exists,
)
from .qbr_hydrate_mappings import (
    apply_explicit_qbr_mappings,
    build_adapt_page_slide_type_by_page_id,
)
from .drive_config import assert_qbr_prompts_ready_or_raise, get_deck_output_folder_id
from .hydrate_capabilities import (
    AVAILABLE_DATA_KEYS as _AVAILABLE_DATA_KEYS,
    CANONICAL_DATA_KEYS,
    build_capability_context as _build_capability_context,
    builder_descriptions_text,
)
from .hydrate_cache import adapt_cache_key as _adapt_cache_key
from .hydrate_cache import data_summary_fingerprint as _data_summary_fingerprint
from .hydrate_cache import default_slide_cache_dir
from .hydrate_cache import get_cached_adapt as _cache_get_adapt
from .hydrate_cache import get_cached_classification as _cache_get_classification
from .hydrate_cache import get_cached_slide_analysis as _cache_get_slide_analysis
from .hydrate_cache import set_cached_adapt as _cache_set_adapt
from .hydrate_cache import set_cached_classification as _cache_set_classification
from .hydrate_cache import set_cached_slide_analysis as _cache_set_slide_analysis
from .hydrate_cache import slide_content_hash as _slide_content_hash
from .hydrate_data_summary import build_data_summary as _build_data_summary
from .hydrate_data_summary import format_data_summary_for_adapt_prompt as _format_data_summary_for_adapt_prompt
from .hydrate_data_summary import prune_data_summary_for_prompt as _prune_data_summary_for_prompt
from .hydrate_data_summary import reset_for_tests as _reset_hydrate_data_summary_for_tests
from .hydrate_data_summary import truncate_strings_in_obj as _truncate_strings_in_obj
from .hydrate_extract import describe_elements as _describe_elements
from .hydrate_extract import extract_slide_text_elements as _extract_slide_text_elements
from .hydrate_extract import extract_text as _extract_text
from .hydrate_classification import classify_slide as _classify_slide
from .hydrate_classification import detect_customer as _detect_customer
from .hydrate_classification import COMPANY_NAMES_FOR_DETECT
from .hydrate_intake import convert_pptx_to_slides as _convert_pptx_to_slides
from .hydrate_intake import drive_query_escape as _drive_query_escape
from .hydrate_intake import fallback_intake_presentations_by_group_permission as _fallback_intake_presentations_by_group_permission
from .hydrate_intake import file_has_group_permission as _file_has_group_permission
from .hydrate_intake import intake_entries_from_drive_file as _intake_entries_from_drive_file
from .hydrate_intake import list_presentations_shared_with_group as _list_presentations_shared_with_group
from .hydrate_intake import log_intake_decks_for_run as _log_intake_decks_for_run
from .hydrate_intake import parent_folder_for_file as _parent_folder_for_file
from .hydrate_intake import remove_intake_group_permission_from_file as _remove_intake_group_permission_from_file
from .hydrate_qbr_agenda import build_qbr_agenda_reshorten_replacements as _build_qbr_agenda_reshorten_replacements
from .hydrate_qbr_agenda import build_qbr_title_hash_replacements as _build_qbr_title_hash_replacements
from .hydrate_qbr_agenda import compiled_title_slot_pattern as _compiled_title_slot_pattern
from .hydrate_qbr_agenda import merge_qbr_agenda_title_replacements as _merge_qbr_agenda_title_replacements
from .hydrate_qbr_agenda import qbr_agenda_adapt_extra_rules as _qbr_agenda_adapt_extra_rules
from .hydrate_qbr_agenda import qbr_agenda_effective_max_chars as _qbr_agenda_effective_max_chars
from .hydrate_qbr_agenda import qbr_agenda_hydrate_config as _qbr_agenda_hydrate_config
from .hydrate_qbr_agenda import qbr_agenda_items_from_plan as _qbr_agenda_items_from_plan
from .hydrate_qbr_agenda import qbr_agenda_label_shortening_mode as _qbr_agenda_label_shortening_mode
from .hydrate_qbr_agenda import qbr_agenda_title_bar_shape_object_ids as _qbr_agenda_title_bar_shape_object_ids
from .hydrate_qbr_agenda import shape_autofit_none_requests as _shape_autofit_none_requests
from .hydrate_qbr_agenda import shorten_agenda_label as _shorten_agenda_label
from .hydrate_qbr_agenda import slide_looks_like_qbr_agenda_titles_legacy as _slide_looks_like_qbr_agenda_titles_legacy
from .hydrate_qbr_agenda import slide_matches_qbr_agenda_hydrate as _slide_matches_qbr_agenda_hydrate
from .hydrate_qbr_agenda import truncate_agenda_line as _truncate_agenda_line
from .hydrate_reproducibility import cache_hit_rate_line as _cache_hit_rate_line
from .hydrate_reproducibility import derive_reproducibility as _derive_reproducibility
from .hydrate_replacements import dedupe_replacements_by_original as _dedupe_replacements_by_original
from .hydrate_replacements import element_may_contain_data as _element_may_contain_data
from .hydrate_replacements import first_number_in_new_value as _adapt_first_number_in_new_value
from .hydrate_replacements import normalize_adapt_replacements as _normalize_adapt_replacements
from .hydrate_replacements import original_reads_as_percent_on_slide as _adapt_original_reads_as_percent_on_slide
from .hydrate_replacements import placeholder_for_percent_mismatch as _adapt_placeholder_for_percent_mismatch
from .hydrate_replacements import placeholder_for_years_context as _adapt_placeholder_for_years_context
from .hydrate_replacements import sanitize_adapt_replacements_percent_semantics as _sanitize_adapt_replacements_percent_semantics
from .hydrate_replacements import sanitize_adapt_replacements_plausible_years as _sanitize_adapt_replacements_plausible_years
from .hydrate_replacements import text_has_percentage_semantics as _adapt_text_has_percentage_semantics
from .hydrate_slide_mutation import add_incomplete_banner as _add_incomplete_banner
from .hydrate_slide_mutation import apply_adaptations as _apply_adaptations
from .hydrate_slide_mutation import has_text_placeholder_incomplete as _has_text_placeholder_incomplete
from .hydrate_slide_mutation import mapped_new_values_for_font_clamp as _mapped_new_values_for_font_clamp
from .hydrate_slide_mutation import red_style_placeholders as _red_style_placeholders
from .hydrate_slide_mutation import replacement_row_is_static_visual_incomplete as _replacement_row_is_static_visual_incomplete
from .hydrate_slide_mutation import should_add_incomplete_banner as _should_add_incomplete_banner
from .hydrate_slide_mutation import slide_metric_font_clamp_requests as _slide_metric_font_clamp_requests
from .hydrate_slide_mutation import unmapped_nonvisual_rows_all_editorial_headings as _unmapped_nonvisual_rows_all_editorial_headings
from .hydrate_thumbnails import download_thumbnail_b64 as _download_thumbnail_b64
from .hydrate_thumbnails import get_slide_thumbnail_b64 as _get_slide_thumbnail_b64
from .hydrate_thumbnails import get_slide_thumbnail_url as _get_slide_thumbnail_url
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence
from . import matching_log
from .slide_loader import get_slide_definition
from .slide_requests import append_slide, append_text_box, append_wrapped_text_box
from .slides_api import (
    _build_slides_service_for_thread,
    _get_service,
    slides_presentations_batch_update,
)
from .speaker_notes import set_speaker_notes, set_speaker_notes_batch

def _slide_cache_dir() -> Path:
    """Directory for persisted slide analysis cache (classification, adapt)."""
    return default_slide_cache_dir()


_BROAD_ANALYSIS_MAX_TOKENS = 8192


def _log_slide_visual_findings(pres_name: str, slide_num: int, total: int, charts: list[Any]) -> None:
    """Log LLM chart/image visual analysis (multi-line, human-readable)."""
    if not charts:
        return
    label = (pres_name or "(untitled deck)")[:100]
    n_ok = sum(1 for c in charts if isinstance(c, dict))
    idx = 0
    for ch in charts:
        if not isinstance(ch, dict):
            continue
        idx += 1
        vk = (ch.get("visual_kind") or "—").strip()
        ctype = (ch.get("chart_type") or "—").strip()
        xa = (ch.get("x_axis") or "").strip()
        ya = (ch.get("y_axis") or "").strip()
        interp = (ch.get("interpretation") or "").replace("\n", " ").strip()
        if len(interp) > 600:
            interp = interp[:597] + "…"
        keys_raw = ch.get("data_recommended_keys")
        if isinstance(keys_raw, list):
            keys_list = [str(x).strip() for x in keys_raw if x]
            keys_s = ", ".join(keys_list) if keys_list else "—"
        else:
            keys_s = "—"
        cov = (ch.get("data_coverage_note") or "").replace("\n", " ").strip()
        if len(cov) > 500:
            cov = cov[:497] + "…"

        lines = [
            "",
            "┌── visual_analysis " + "─" * 52,
            f"│  Deck:        {label}",
            f"│  Slide:       {slide_num} / {total}          Visual: {idx} / {n_ok}",
            f"│  Kind:        {vk}",
            f"│  Chart type:  {ctype}",
        ]
        if xa or ya:
            lines.append(f"│  Axes:        X: {xa or '—'}    Y: {ya or '—'}")
        lines.append(f"│  Pipeline:    {keys_s}")
        lines.append("│")
        lines.append("│  What it shows:")
        lines.extend(
            "│" + row
            for row in textwrap.wrap(
                interp or "—",
                width=86,
                initial_indent="  ",
                subsequent_indent="  ",
                break_long_words=True,
                break_on_hyphens=False,
            )
        )
        lines.append("│")
        lines.append("│  Coverage / gaps:")
        lines.extend(
            "│" + row
            for row in textwrap.wrap(
                cov or "—",
                width=86,
                initial_indent="  ",
                subsequent_indent="  ",
                break_long_words=True,
                break_on_hyphens=False,
            )
        )
        lines.append("└" + "─" * 69)
        logger.info("\n".join(lines))


def _get_cached_classification(cache_key: str) -> dict | None:
    """Return cached classification result if present and version matches."""
    return _cache_get_classification(_slide_cache_dir(), cache_key)


def _set_cached_classification(cache_key: str, result: dict) -> None:
    """Persist classification result for this cache key."""
    _cache_set_classification(_slide_cache_dir(), cache_key, result)


def _get_cached_adapt(cache_key: str) -> list[dict] | None:
    """Return cached adapt replacements if present and version matches."""
    return _cache_get_adapt(_slide_cache_dir(), cache_key)


def _set_cached_adapt(cache_key: str, replacements: list[dict]) -> None:
    """Persist adapt replacements for this cache key (values are resolved at read time)."""
    _cache_set_adapt(_slide_cache_dir(), cache_key, replacements)


def _log_slide_pipeline_done(slide_ref: str, src: str, replacements: list[dict]) -> None:
    """One summary line (JSON) per slide after LLM + synonym + sanitizers: counts + unmapped samples."""
    if not matching_log.enabled():
        return
    n_map = sum(1 for r in replacements if r.get("mapped", True))
    unmapped = [r for r in replacements if not r.get("mapped", True)]
    unmapped_details = [
        {
            "original": (r.get("original") or "")[:200],
            "field": (r.get("field") or "")[:160],
            "new_value": (r.get("new_value") or "")[:120],
        }
        for r in unmapped[:24]
    ]
    matching_log.emit(
        "slide_pipeline_summary",
        slide_ref=slide_ref,
        pipeline_source=src,
        total_rows=len(replacements),
        mapped_count=n_map,
        unmapped_count=len(unmapped),
        unmapped_rows_sample=unmapped_details,
    )


def _resolve_cached_replacements(
    cached: list[dict], data_summary: dict, *, slide_ref: str = ""
) -> list[dict]:
    """For cached replacements with mapped=true, set new_value from current data_summary.
    Tries to preserve format (e.g. "31 sites" -> "14 sites") when original has a trailing suffix.
    Supports dotted paths (e.g. ``platform_value.total_savings``).
    """
    import re as _re
    out = []
    for r in list(cached):
        r = dict(r)
        if r.get("mapped") and r.get("field"):
            key = r["field"].strip().replace(" ", "_").replace("-", "_").lower()
            if data_summary_path_exists(data_summary, key):
                val = data_summary_lookup(data_summary, key)
                if isinstance(val, (list, dict)):
                    r["new_value"] = str(val)[:200]
                else:
                    raw = str(val) if val is not None else ""
                    orig = r.get("original", "")
                    # Preserve suffix from original (e.g. "31 sites" -> "14 sites")
                    m = _re.match(r"^[\d.,\s$€£%]+", orig)
                    suffix = (orig[m.end():].strip() if m else "").strip()
                    r["new_value"] = f"{raw} {suffix}".strip() if suffix else raw
                if matching_log.enabled():
                    matching_log.emit(
                        "cache_refresh_row",
                        slide_ref=slide_ref,
                        field=key,
                        path_exists=True,
                        original=(str(r.get("original", "")) or "")[:200],
                        new_value=(r.get("new_value") or "")[:200],
                    )
            else:
                if matching_log.enabled():
                    matching_log.emit(
                        "cache_row_path_missing",
                        slide_ref=slide_ref,
                        field=key,
                        path_exists=False,
                        original=(str(r.get("original", "")) or "")[:200],
                    )
        out.append(r)
    return out


def _get_cached_slide_analysis(cache_key: str) -> dict | None:
    """Return cached broad analysis (data_ask, purpose, slide_type) if present and version matches."""
    return _cache_get_slide_analysis(_slide_cache_dir(), cache_key)


def _set_cached_slide_analysis(cache_key: str, analysis: dict) -> None:
    """Persist broad slide analysis for this cache key."""
    _cache_set_slide_analysis(_slide_cache_dir(), cache_key, analysis)


def _analyze_slide_broad(client, text: str, elements: dict, thumb_b64: str | None,
                         slide_num: int, total: int, pres_name: str) -> dict:
    """One-time broad analysis: what data does this slide ask for, and what is its purpose?

    Returns data_ask (list of {key, example_from_slide}), purpose, slide_type, title, etc.
    Cached so we don't re-run when we add data sources or capabilities later.
    """
    builder_list = builder_descriptions_text()
    keys_list = ", ".join(CANONICAL_DATA_KEYS)

    system = (
        "You are analyzing a slide from a customer QBR deck to capture (1) what DATA it asks for, "
        "(2) its PURPOSE, and (3) CHART CONFIGURATION for every chart and graph.\n\n"
        "DATA ASK: List every piece of data the slide displays or expects. "
        f"Canonical keys we support: {keys_list}. "
        "For each data item return: key (canonical or slug), example_from_slide. "
        "Include embedded charts/images: key '_embedded_chart' or '_embedded_image', example_from_slide the marker text.\n\n"
        "PURPOSE: One sentence — what is this slide communicating?\n\n"
        f"SLIDE TYPE: Choose from: {builder_list}\n"
        "Prefer 'title' (opening title), 'qbr_cover' (branded QBR cover), or "
        "'qbr_divider' (section/chapter title) when the slide is **primarily a title or cover** "
        "with no customer metrics to refresh — hydration will not rewrite numbers on those types.\n\n"
        "VISUALS (charts, graphs, plot images): You MUST analyze every visualization on the slide — including "
        "native Slides **chart** elements AND **image** elements that show charts, plots, dashboards, or data graphics. "
        "Use the slide thumbnail; read axis titles, legends, and labels when visible.\n"
        "For EACH distinct visualization add one object to the 'charts' array (even if it is a pasted screenshot). "
        "If the slide has no charts or data images, return charts: [].\n"
        "For each visualization return:\n"
        "  visual_kind: one of native_chart | image_or_screenshot | table_as_chart | unknown\n"
        "  interpretation: REQUIRED — 1–2 short sentences (max ~280 characters) describing what DATA the visual encodes "
        "(metrics, time span, comparison). If illegible, say so briefly.\n"
        "  chart_type: line, bar, stacked_bar, column, pie, donut, area, combo, scatter, table, heatmap, or short text\n"
        "  x_axis: label/description of the x axis; empty string if N/A\n"
        "  y_axis: label/description of the y axis; empty string if N/A\n"
        "  transformations: array of how data is transformed (e.g. 'group by quarter', 'rolling average')\n"
        "  configuration: optional — legend, series names, colors, gridlines, etc.\n"
        "  data_recommended_keys: array of 0–10 strings — ONLY from this exact list (exact spellings): "
        f"{keys_list}. "
        "Pick the **minimum** set of pipeline fields that could **replace or rebuild** this visual with our automated data. "
        "Use [] if the visual needs metrics we do not model (name them in data_coverage_note).\n"
        "  data_coverage_note: 1–2 sentences. State whether our pipeline likely has this data. "
        "If something is missing (e.g. 'export usage', 'SKU-level revenue'), name it explicitly and say **not in pipeline**.\n"
        "If there are truly no charts/graphs/data images on the slide, return charts: []. "
        "At most 8 objects in charts[] — prioritize the most data-heavy visuals.\n\n"
        "JSON RULES (critical): Output ONE JSON object only, no markdown fences. "
        "Every string must be valid JSON: escape double-quotes as \\\", backslashes as \\\\, "
        "and use \\n for newlines — never put a raw line break inside a string. "
        "Keep example_from_slide under 100 characters (paraphrase; do not paste long slide quotes). "
        "Keep reasoning under 300 characters.\n\n"
        "Return JSON:\n"
        "  data_ask: [{ key, example_from_slide }, ...]\n"
        "  purpose: string\n"
        "  slide_type: one of the builder types above\n"
        "  title: string\n"
        "  reasoning: string\n"
        "  custom_sections: (only if slide_type='custom') [{header, body}]\n"
        "  charts: [{ visual_kind, interpretation, chart_type, x_axis, y_axis, transformations, configuration?, "
        "data_recommended_keys, data_coverage_note }, ...]\n"
    )

    parts: list[dict] = []
    if thumb_b64:
        parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"},
        })
    parts.append({"type": "text", "text": (
        f"Presentation: {pres_name}\nSlide {slide_num}/{total}\n\n"
        f"Extracted text:\n{text or '(no text)'}\n\n"
        f"Elements (look for type 'chart' or 'image' — each may be a chart/graph):\n{json.dumps(elements)}\n\n"
        "Analyze: (1) data_ask, (2) purpose, (3) slide_type, (4) title. "
        "Then list every visualization in charts[], each with interpretation (what data it shows) and "
        "data_recommended_keys when our pipeline can supply it."
    )})

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": parts},
    ]
    raw_content = ""
    for attempt in range(2):
        resp = _llm_create_with_retry(
            client,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=_BROAD_ANALYSIS_MAX_TOKENS,
            response_format={"type": "json_object"},
            messages=messages,
        )
        raw_content = _strip_json_code_fence(resp.choices[0].message.content or "")
        try:
            analysis = json.loads(raw_content)
        except json.JSONDecodeError as e:
            if attempt == 0:
                logger.warning(
                    "_analyze_slide_broad: invalid JSON (slide %s/%s), retrying once: %s",
                    slide_num,
                    total,
                    e,
                )
                # Avoid blowing context with a huge truncated reply
                clipped = raw_content[:6000] + ("…" if len(raw_content) > 6000 else "")
                messages.append({"role": "assistant", "content": clipped})
                messages.append({
                    "role": "user",
                    "content": (
                        f"That output was not valid JSON ({e}). "
                        "Reply with a single valid JSON object only — no markdown. "
                        "Escape every \" inside strings as \\\". "
                        "Use \\n for newlines inside strings. "
                        "Shorten strings if needed; cap charts at 6 items."
                    ),
                })
                continue
            logger.warning(
                "_analyze_slide_broad: LLM returned invalid JSON after retry (slide %s/%s): %s",
                slide_num,
                total,
                e,
            )
            import re as _re
            purpose_match = _re.search(r'"purpose"\s*:\s*"((?:[^"\\]|\\.)*)"?', raw_content)
            purpose_fallback = purpose_match.group(1).strip() if purpose_match and purpose_match.group(1) else None
            if not purpose_fallback:
                title_match = _re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)"?', raw_content)
                purpose_fallback = title_match.group(1).strip() if title_match and title_match.group(1) else None
            title_guess = (text or "").strip().split("\n")[0].strip()[:100] if text else ""
            if not purpose_fallback:
                purpose_fallback = f"Slide: {title_guess}" if title_guess else "Slide content (analysis parse failed)"
            return {
                "data_ask": [],
                "purpose": purpose_fallback,
                "slide_type": "custom",
                "title": title_guess,
                "reasoning": "",
                "charts": [],
            }
        if not isinstance(analysis.get("charts"), list):
            analysis["charts"] = []
        _log_slide_visual_findings(pres_name, slide_num, total, analysis["charts"])
        return analysis
    assert False, "_analyze_slide_broad: unreachable"  # noqa: B011


def _resolve_data_ask_to_replacements(
    data_ask: list[dict],
    data_summary: dict,
    text_elements: list[dict],
    *,
    slide_ref: str = "",
) -> list[dict]:
    """Turn cached data_ask into replacement list using current data_summary and slide text.

    Matches data_ask items to text_elements by example_from_slide; resolves key to value from data_summary.
    """
    import re as _re
    replacements: list[dict] = []
    # Build set of text snippets we can match (from slide)
    element_texts = [el.get("text", "") for el in text_elements if el.get("text")]

    for item in data_ask:
        key = (item.get("key") or "").strip().replace(" ", "_").replace("-", "_").lower()
        example = (item.get("example_from_slide") or "").strip()
        if not key:
            if matching_log.enabled():
                matching_log.emit(
                    "data_ask_skip",
                    slide_ref=slide_ref,
                    reason="empty_key",
                    item_key_snippet=repr(item)[:300],
                )
            continue
        # Special keys: visual elements we cannot replace with data
        if key in ("_embedded_chart", "_embedded_image") or key.startswith("_embedded"):
            if matching_log.enabled():
                matching_log.emit(
                    "data_ask_embedded_visual",
                    slide_ref=slide_ref,
                    key=key,
                    example=(example or "")[:200],
                    decision="static_visual_unmapped",
                )
            replacements.append({
                "original": example or f"({key})",
                "new_value": "[CHART — data cannot be auto-updated]" if "chart" in key else "[STATIC IMAGE — contains data that cannot be auto-updated]",
                "mapped": False,
                "field": key,
            })
            continue
        # Find best matching element text (exact or contains)
        original = None
        for et in element_texts:
            if example and example in et:
                original = example
                break
            if et and not original and (example in et or et in example):
                original = et
        if not original and example:
            original = example
        # Resolve value from data_summary (top-level or dotted path)
        if data_summary_path_exists(data_summary, key):
            val = data_summary_lookup(data_summary, key)
            if isinstance(val, (list, dict)):
                new_value = str(val)[:200]
            else:
                raw = str(val) if val is not None else ""
                # Preserve suffix from example (e.g. "31 sites" -> "14 sites")
                m = _re.match(r"^[\d.,\s$€£%]+", original or "")
                suffix = (original[m.end():].strip() if m and original else "").strip()
                new_value = f"{raw} {suffix}".strip() if suffix else raw
            if matching_log.enabled():
                matching_log.emit(
                    "data_ask_resolved",
                    slide_ref=slide_ref,
                    data_ask_key=key,
                    path_exists=True,
                    value_kind="container" if isinstance(val, (list, dict)) else "scalar",
                    example_from_slide=(example or "")[:200],
                    match_original=(str(original) if original else "")[:200],
                    new_value=(new_value or "")[:200],
                )
            replacements.append({
                "original": original or example or key,
                "new_value": new_value,
                "mapped": True,
                "field": key,
            })
        else:
            if matching_log.enabled():
                matching_log.emit(
                    "data_ask_unmapped",
                    slide_ref=slide_ref,
                    data_ask_key=key,
                    path_exists=False,
                    decision="no_such_path_in_data_summary",
                    example_from_slide=(example or "")[:200],
                    match_original=(str(original) if original else "")[:200],
                )
            # No current source for this key — generic on-slide placeholder (details in speaker notes)
            if original or example:
                replacements.append({
                    "original": original or example,
                    "new_value": "[???]",
                    "mapped": False,
                    "field": key,
                })
    return replacements


_print_context = "bpo"  # overridden per command

def _print(*args, **kwargs):
    """Log and print with immediate flush so output appears in real time."""
    end = kwargs.pop("end", "\n")
    sep = kwargs.pop("sep", " ")
    msg = sep.join(str(a) for a in args)
    logger.debug("%s: %s", _print_context, msg.rstrip())
    print(msg, end=end, flush=True)


# ── Main evaluation (data-centric: collect analysis, deduce reproducibility at render) ──


def _collect_hydrate_intake_presentations(
    *,
    log_prefix: str = "intake",
) -> tuple[list[dict[str, Any]], str | None]:
    """Compatibility wrapper that honors tests patching evaluate-level intake globals."""
    if not GOOGLE_HYDRATE_INTAKE_GROUP:
        return [], (
            "Set GOOGLE_HYDRATE_INTAKE_GROUP in .env to your intake Google Group email "
            "(decks shared with that group as Reader are processed)."
        )

    raw = _list_presentations_shared_with_group(GOOGLE_HYDRATE_INTAKE_GROUP)
    if not raw:
        return [], f"No presentations found shared with group {GOOGLE_HYDRATE_INTAKE_GROUP}."

    group_email = GOOGLE_HYDRATE_INTAKE_GROUP
    merged: list[dict[str, Any]] = [
        {"id": item["id"], "name": item["name"], "intake": "group", "group_email": group_email}
        for item in raw
    ]
    _log_intake_decks_for_run(merged, log_prefix=log_prefix)
    return merged, None


def evaluate_new_slides(verbose: bool = False) -> list[dict[str, Any]]:
    """Scan decks shared with GOOGLE_HYDRATE_INTAKE_GROUP; collect data-centric analysis per slide.

    Uses the same analysis as hydrate (data_ask + purpose). Reproducibility is derived at
    report time from data_ask vs current available data keys — no separate LLM assessment.
    Results are cached so hydrate can reuse them.
    """
    global _print_context
    _print_context = "evaluate"
    presentations, empty_msg = _collect_hydrate_intake_presentations(log_prefix="evaluate")
    if empty_msg:
        print(empty_msg)
        return []

    _print(f"Found {len(presentations)} presentation(s) to process:\n")
    for p in presentations:
        src = f"shared with group {p.get('group_email') or GOOGLE_HYDRATE_INTAKE_GROUP}"
        _print(f"  - {p['name']}  ({src})")
    _print()

    slides_svc, _d, _ = _get_service()
    client = llm_client()
    all_results: list[dict[str, Any]] = []
    eval_run_hits = 0
    eval_run_slides = 0

    for pres in presentations:
        pres_id = pres["id"]
        pres_name = pres["name"]
        _print(f"{'─' * 60}")
        _print(f"Evaluating: {pres_name}")
        _print(f"{'─' * 60}\n")

        full_pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        slides = full_pres.get("slides", [])
        _print(f"  {len(slides)} slides\n")

        eval_cache = {"analysis_hit": 0, "analysis_miss": 0, "no_cache_key": 0}
        for si, slide in enumerate(slides, 1):
            page_id = slide["objectId"]
            texts = []
            for el in slide.get("pageElements", []):
                texts.extend(_extract_text(el))
            slide_text = "\n".join(texts)
            elements = _describe_elements(slide)
            title_guess = texts[0][:60] if texts else "(no text)"
            _print(f"  Slide {si}/{len(slides)}  \"{title_guess}\"")

            try:
                thumb_b64 = _get_slide_thumbnail_b64(slides_svc, pres_id, page_id)
            except Exception as e:
                logger.warning("evaluate: thumbnail unavailable for slide %d of '%s': %s",
                               si, pres_name, e)
                thumb_b64 = None

            cache_key = _slide_content_hash(
                thumb_b64, slide_text[:2000] if slide_text else "", page_id=page_id
            )
            if cache_key:
                analysis = _get_cached_slide_analysis(cache_key)
                if analysis:
                    logger.debug("evaluate: [%d/%d] analysis cache hit", si, len(slides))
                    eval_cache["analysis_hit"] += 1
                else:
                    logger.debug("evaluate: [%d/%d] analyzing slide (data ask + purpose)...",
                                 si, len(slides))
                    analysis = _analyze_slide_broad(
                        client, slide_text, elements, thumb_b64, si, len(slides), pres_name
                    )
                    _set_cached_slide_analysis(cache_key, analysis)
                    eval_cache["analysis_miss"] += 1
            else:
                analysis = _analyze_slide_broad(
                    client, slide_text, elements, thumb_b64, si, len(slides), pres_name
                )
                eval_cache["no_cache_key"] += 1

            derived = _derive_reproducibility(analysis)
            result = {
                "presentation": pres_name,
                "slide_number": si,
                "title_guess": title_guess,
                "extracted_text": slide_text if verbose else slide_text[:200],
                "elements": elements,
                "purpose": analysis.get("purpose"),
                "slide_type": analysis.get("slide_type"),
                **derived,
            }
            all_results.append(result)
            _print_evaluation(result)

        n_ev = len(slides)
        ev_hit = eval_cache["analysis_hit"]
        logger.info(
            "evaluate: analysis cache summary for '%s' — %s | miss=%d no_key=%d",
            pres_name,
            _cache_hit_rate_line("cache_hit", ev_hit, n_ev),
            eval_cache["analysis_miss"],
            eval_cache["no_cache_key"],
        )
        _print(f"  Analysis cache: {ev_hit}/{n_ev} hits ({100 * ev_hit // n_ev if n_ev else 0}%) "
               f"(new analysis {eval_cache['analysis_miss']}, no thumbnail key {eval_cache['no_cache_key']})\n")
        eval_run_hits += ev_hit
        eval_run_slides += n_ev

    _print(f"{'=' * 60}")
    _print("CACHE HIT RATE (evaluate run — analysis cache)")
    if eval_run_slides:
        ep = 100 * eval_run_hits // eval_run_slides
        _print(f"  Analysis cache hits: {eval_run_hits}/{eval_run_slides} slides ({ep}%)")
        logger.info("evaluate: run summary — %s", _cache_hit_rate_line("analysis_cache_hit", eval_run_hits, eval_run_slides))
    else:
        _print("  No slides evaluated.")
    _print(f"{'=' * 60}")
    return all_results


def _print_evaluation(result: dict) -> None:
    """Pretty-print a single slide evaluation to stdout."""
    feasibility = result.get("feasibility", "?")
    confidence = result.get("confidence", "?")
    summary = result.get("summary", "")
    effort = result.get("effort_estimate", "?")
    closest = result.get("closest_existing")
    gaps = result.get("gaps", [])

    icon = {
        "fully reproducible": "✅",
        "mostly reproducible": "🟡",
        "partially reproducible": "🟠",
        "not reproducible": "❌",
    }.get(feasibility, "❓")

    _print(f"\n    {icon}  {feasibility}  (confidence: {confidence}%, effort: {effort})")
    _print(f"    {summary}")

    if closest:
        _print(f"    Closest existing slide: {closest}")

    data_needed = result.get("data_needed", [])
    if data_needed:
        _print("    Data:")
        for d in data_needed:
            avail = "✓" if d.get("available") else "✗"
            note = f" — {d['note']}" if d.get("note") else ""
            _print(f"      [{avail}] {d.get('source', '?')}: {d.get('fields', '?')}{note}")

    if gaps:
        _print("    Gaps:")
        for g in gaps:
            _print(f"      - {g}")

    _print()


# ── Visual QA ──

_QA_SYSTEM_PROMPT = (
    "You are a visual QA reviewer for auto-generated Google Slides presentations. "
    "Examine this slide thumbnail and identify any layout or formatting problems.\n\n"
    "Check for:\n"
    "- Text overlapping other text or elements\n"
    "- Text running off the right or bottom edge of the slide canvas\n"
    "- Text that is mid-word cut off (e.g. 'Shor' where 'Shortage' was expected) — "
    "  this is a TRUE truncation issue. Do NOT flag text that simply ends near the "
    "  right margin with a complete word — that is normal layout.\n"
    "- Unreadable font sizes (too small to read)\n"
    "- Misaligned or visually unbalanced layouts\n"
    "- Empty slides that should have content\n"
    "- Date formats that look raw/ugly (e.g. 2026-03-10 instead of March 10, 2026)\n"
    "- Color contrast issues (text invisible against background)\n"
    "- Tables with cells overflowing or misaligned\n\n"
    "IMPORTANT: Values in [brackets] like [000], [$000], [00/00/00], [00%], [???] "
    "are INTENTIONAL incomplete-data placeholders. Do NOT flag them as issues. "
    "Also do NOT flag '⚠ INCOMPLETE' banners as issues — they are intentional.\n\n"
    "Return JSON:\n"
    "  pass: boolean — true if the slide looks good, false if there are problems\n"
    "  issues: list of strings describing each problem found (empty if pass=true)\n"
    "  severity: 'none' | 'minor' | 'major' — overall severity\n"
)


def visual_qa(pres_id: str, slides_svc=None) -> list[dict[str, Any]]:
    """Thumbnail every slide in a presentation and review with GPT-4o Vision.

    Returns a list of per-slide QA results:
      {slide_num, page_id, pass, issues, severity}
    """
    if slides_svc is None:
        slides_svc, _d, _ = _get_service()

    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    slides = pres.get("slides", [])
    if not slides:
        return []

    oai = llm_client()
    n = len(slides)

    _print(f"\n  Visual QA: reviewing {n} slides...")

    # Step 1: Pre-fetch all thumbnail URLs in the main thread.
    # The Google API client (httplib2) is NOT thread-safe — calling it from workers
    # causes malloc double-free crashes.  Getting just the URL is fast (<0.5s/slide).
    logger.info("QA: fetching thumbnail URLs for %d slides...", n)
    thumb_urls: dict[str, str | None] = {}
    for si, slide in enumerate(slides, 1):
        page_id = slide["objectId"]
        try:
            thumb_urls[page_id] = _get_slide_thumbnail_url(slides_svc, pres_id, page_id)
        except Exception as e:
            logger.warning("QA: thumbnail URL failed for slide %d/%d: %s", si, n, e)
            thumb_urls[page_id] = None

    # Step 2: Parallelise the HTTP download + GPT Vision call (both thread-safe).
    def _review_slide(args: tuple[int, dict]) -> dict:
        si, slide = args
        page_id = slide["objectId"]
        url = thumb_urls.get(page_id)
        thumb_b64 = None
        if url:
            try:
                thumb_b64 = _download_thumbnail_b64(url)
            except Exception as e:
                logger.warning("QA: thumbnail download failed for slide %d/%d: %s", si, n, e)

        logger.debug("QA: slide %d/%d — reviewing with %s...", si, n, LLM_MODEL)
        resp = _llm_create_with_retry(oai,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=1024,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _QA_SYSTEM_PROMPT},
                {"role": "user", "content": [
                    *(
                        [{"type": "image_url",
                          "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"}}]
                        if thumb_b64 else []
                    ),
                    {"type": "text", "text": f"Slide {si}/{n}. Review this slide."},
                ]},
            ],
        )
        raw_content = resp.choices[0].message.content
        try:
            qa = json.loads(raw_content)
        except json.JSONDecodeError as e:
            logger.warning("QA: slide %d/%d — invalid JSON from LLM (%s), treating as pass", si, n, e)
            qa = {"pass": True, "issues": ["QA response invalid (JSON error)"], "severity": "none"}
        qa["slide_num"] = si
        qa["page_id"] = page_id
        return qa

    raw: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_review_slide, (si, slide)): si
                   for si, slide in enumerate(slides, 1)}
        for fut in as_completed(futures):
            try:
                qa = fut.result()
                raw[qa["slide_num"]] = qa
            except Exception as e:
                si = futures[fut]
                logger.warning("QA failed for slide %d: %s", si, e)
                raw[si] = {"slide_num": si, "pass": True, "issues": [], "severity": "none"}

    # Emit results in slide order
    results: list[dict[str, Any]] = []
    for si in range(1, n + 1):
        qa = raw.get(si, {"slide_num": si, "pass": True, "issues": [], "severity": "none"})
        results.append(qa)
        passed = qa.get("pass", True)
        severity = qa.get("severity", "none")
        issues = qa.get("issues", [])
        if passed:
            _print(f"    [{si}/{n}] OK")
        else:
            icon = "!" if severity == "major" else "~"
            _print(f"    [{si}/{n}] [{icon}] {severity.upper()}: {'; '.join(issues[:2])}")
            for issue in issues[2:]:
                _print(f"         ↳ {issue}")

    passed_count = sum(1 for r in results if r.get("pass", True))
    failed_count = len(results) - passed_count
    major = sum(1 for r in results if r.get("severity") == "major")
    minor = sum(1 for r in results if r.get("severity") == "minor")

    _print(f"  QA result: {passed_count}/{len(results)} passed", end="")
    if failed_count:
        _print(f"  ({major} major, {minor} minor)")
    else:
        _print("")

    return results


# ── Slide data adaptation ──


_ADAPT_MAX_TOKENS = 8192
_ADAPT_MAX_TOKENS_RETRY = 16384


_ADAPT_TEMPLATE_SLIDE_YAML = Path(__file__).resolve().parent.parent / "config" / "adapt_template_slide.yaml"
_ADAPT_SYSTEM_PROMPT_YAML = Path(__file__).resolve().parent.parent / "prompts" / "adapt_system_prompt.yaml"

_ADAPT_TEMPLATE_FILL_IN_FALLBACK = (
    "- **EXCEPTION — example / headline / template slides**: If the slide title or body is dominated by "
    "bracket placeholders like [000], [$000], [00%], [???], or the title includes words like "
    "**Example**, **Sample**, **Headlines**, or **Template**, treat the slide as a **fill-in pattern** "
    "to hydrate from CURRENT DATA. **Map every metric you can** (including bullets under headings such as "
    "\"Key Partnership Results\") — do **not** blanket-map=false those blocks just because the heading "
    "sounds like achievements.\n"
)


@functools.lru_cache(maxsize=1)
def _load_adapt_template_slide_rule() -> str:
    """Load adapt LLM rules from YAML: example/template exception plus optional layout rules."""
    p = _ADAPT_TEMPLATE_SLIDE_YAML
    try:
        raw = p.read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        if not isinstance(data, dict):
            return _ADAPT_TEMPLATE_FILL_IN_FALLBACK
        fill = data.get("template_fill_in_slide_rule")
        if isinstance(fill, str) and fill.strip():
            base = fill.rstrip()
        else:
            base = _ADAPT_TEMPLATE_FILL_IN_FALLBACK.rstrip()
        layout = data.get("adapt_layout_rule")
        if isinstance(layout, str) and layout.strip():
            return base + "\n" + layout.strip() + "\n"
        return base + "\n"
    except OSError as e:
        logger.warning("adapt: could not read %s — using fallback (%s)", p, e)
    except (yaml.YAMLError, TypeError) as e:
        logger.warning("adapt: invalid YAML in %s — using fallback (%s)", p, e)
    return _ADAPT_TEMPLATE_FILL_IN_FALLBACK


@functools.lru_cache(maxsize=1)
def _load_adapt_system_prompt_template() -> str:
    """Load the adapt LLM system prompt template (str.format placeholders: data_json, template_fill_in_rule).

    When ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` is set, syncs repo ``prompts/adapt_system_prompt.yaml``
    to that folder’s ``Prompts/``, then prefers Drive text — same pattern as deck/slide YAML.
    """
    p = _ADAPT_SYSTEM_PROMPT_YAML

    def _parse(raw_yaml: str) -> str | None:
        data = yaml.safe_load(raw_yaml)
        if not isinstance(data, dict):
            return None
        s = data.get("adapt_system_prompt")
        if not isinstance(s, str) or not s.strip():
            return None
        return s.rstrip("\n") + "\n"

    if GOOGLE_QBR_GENERATOR_FOLDER_ID:
        try:
            from .drive_config import (
                ensure_qbr_adapt_prompt_yaml_synced_from_repo,
                read_adapt_system_prompt_yaml_text_from_drive,
            )

            ensure_qbr_adapt_prompt_yaml_synced_from_repo()
            drive_raw = read_adapt_system_prompt_yaml_text_from_drive()
            if drive_raw is not None:
                parsed = _parse(drive_raw)
                if parsed is not None:
                    return parsed
                logger.warning(
                    "adapt: QBR Prompts %s on Drive is not valid adapt_system_prompt YAML — using local file",
                    "adapt_system_prompt.yaml",
                )
        except Exception as e:
            logger.warning("adapt: QBR Prompts Drive prompt unavailable (%s) — using local file", e)

    try:
        raw = p.read_text(encoding="utf-8")
        parsed = _parse(raw)
        if parsed is not None:
            return parsed
        raise ValueError("missing or empty adapt_system_prompt")
    except OSError as e:
        logger.warning("adapt: could not read %s — %s", p, e)
    except (yaml.YAMLError, TypeError, ValueError) as e:
        logger.warning("adapt: invalid YAML in %s — %s", p, e)
    raise RuntimeError(
        f"Adapt system prompt is required: edit or restore {p} (see repo prompts/ folder)."
    )


def reset_for_tests() -> None:
    """Clear hydrate/adapt process caches that can leak across tests."""
    _load_adapt_template_slide_rule.cache_clear()
    _load_adapt_system_prompt_template.cache_clear()
    _reset_hydrate_data_summary_for_tests()


def _get_data_replacements(oai, text_elements: list[dict], data_summary: dict,
                           thumb_b64: str | None = None,
                           slide_label: str = "?",
                           extra_system_rules: str = "") -> list[dict]:
    """Ask GPT-4o to map slide data values to current report data.

    ``extra_system_rules`` is appended to the system prompt (e.g. QBR agenda visual refinement).
    """
    # Filter to elements that could plausibly contain data values
    candidates = [el for el in text_elements if _element_may_contain_data(el)]
    # Always include image/chart markers even if they slipped through the filter
    markers = [el for el in text_elements
               if el.get("text", "").startswith("(embedded") or
               el.get("text", "").startswith("(image")]
    # Merge, dedup by element_id
    seen = set()
    filtered = []
    for el in candidates + markers:
        key = (el.get("element_id"), el.get("text"))
        if key not in seen:
            seen.add(key)
            filtered.append(el)

    data_json = _format_data_summary_for_adapt_prompt(data_summary)
    system = _load_adapt_system_prompt_template().format(
        data_json=data_json,
        template_fill_in_rule=_load_adapt_template_slide_rule(),
    ).rstrip()
    if (extra_system_rules or "").strip():
        system += "\n\n" + (extra_system_rules or "").strip() + "\n"

    def _text_desc(rows: list[dict]) -> str:
        return "\n".join(
            f"  [{t['type']}"
            + (f" row={t['row']} col={t['col']}" if t["type"] == "table_cell" else "")
            + f"]: \"{t['text']}\""
            for t in rows
        )

    def _messages_for_rows(rows: list[dict]) -> list[dict]:
        parts: list[dict] = []
        if thumb_b64:
            parts.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"},
            })
        td = _text_desc(rows)
        parts.append({
            "type": "text",
            "text": f"Slide text elements:\n{td}\n\nIdentify all data values and map them.",
        })
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": parts},
        ]

    def _call_llm(rows: list[dict], max_tokens: int) -> tuple[list[dict], str | None]:
        if not rows:
            return [], None
        resp = _llm_create_with_retry(
            oai,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            messages=_messages_for_rows(rows),
        )
        raw = resp.choices[0].message.content
        fr = resp.choices[0].finish_reason
        try:
            result = json.loads(raw or "")
        except json.JSONDecodeError as exc:
            logger.warning(
                "hydrate: slide %s — LLM response was invalid JSON (%s), skipping data replacement",
                slide_label,
                exc,
            )
            return [], fr
        repl = _normalize_adapt_replacements(result.get("replacements", []) or [])
        return repl, fr

    repl, finish_reason = _call_llm(filtered, _ADAPT_MAX_TOKENS)
    if finish_reason == "length":
        logger.warning(
            "hydrate: slide %s — LLM hit max_tokens (%d); retrying with %d",
            slide_label,
            _ADAPT_MAX_TOKENS,
            _ADAPT_MAX_TOKENS_RETRY,
        )
        repl, finish_reason = _call_llm(filtered, _ADAPT_MAX_TOKENS_RETRY)

    if finish_reason == "length" and len(filtered) > 1:
        mid = len(filtered) // 2
        logger.warning(
            "hydrate: slide %s — still truncated; splitting %d elements into two LLM calls",
            slide_label,
            len(filtered),
        )
        first, fr1 = _call_llm(filtered[:mid], _ADAPT_MAX_TOKENS)
        second, fr2 = _call_llm(filtered[mid:], _ADAPT_MAX_TOKENS)
        if fr1 == "length" or fr2 == "length":
            logger.warning(
                "hydrate: slide %s — split LLM calls still truncated; partial replacements only",
                slide_label,
            )
        repl = _dedupe_replacements_by_original(_normalize_adapt_replacements(first + second))

    if matching_log.enabled():
        if not repl:
            matching_log.emit(
                "llm_no_replacements",
                slide=slide_label,
                num_candidate_elements=len(filtered),
            )
        else:
            for r in repl:
                matching_log.emit(
                    "llm_mapping_row",
                    slide=slide_label,
                    original=(r.get("original") or "")[:300],
                    new_value=(r.get("new_value") or "")[:300],
                    mapped=bool(r.get("mapped", True)),
                    field=(r.get("field") or "")[:200],
                )
    return repl


def _unmapped_placeholder_descriptions_for_notes(oai, entries: list[dict]) -> list[str]:
    """One short line per unmapped placeholder for speaker notes only (not shown on slide)."""
    if not entries:
        return []
    from .config import LLM_MODEL_FAST
    try:
        resp = _llm_create_with_retry(
            oai,
            model=LLM_MODEL_FAST,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": (
                    "Each item is an unmapped metric on a slide: we show a generic token on the slide "
                    "and need a one-line explanation for the presenter (speaker notes only).\n"
                    "For each item, output one concise sentence (max ~120 chars) explaining what the "
                    "original text represented or what to verify. Do not repeat the placeholder token alone.\n"
                    "Return JSON: {\"lines\": [\"...\", ...]} with exactly the same length as input items."
                )},
                {"role": "user", "content": json.dumps({"items": entries}, indent=0, default=str)},
            ],
        )
        raw = resp.choices[0].message.content
        data = json.loads(raw)
        lines = data.get("lines")
        if isinstance(lines, list) and len(lines) == len(entries):
            return [str(x).strip()[:200] for x in lines]
    except Exception as e:
        logger.warning("unmapped placeholder notes batch failed: %s", e)
    out: list[str] = []
    for e in entries:
        fld = (e.get("field") or "?").strip()
        orig = (e.get("original") or "")[:100]
        out.append(f"Field `{fld}` — verify or source manually (was: {orig})")
    return out


_PLACEHOLDER_MARKERS = ("[000]", "[$000]", "[00/00/00]", "[00%]", "[???]")

# QBR template agenda: "Title #1" … "Title #N" labels → section titles from decks/qbr.yaml (via report["_slide_plan"]).
_TITLE_HASH_PLACEHOLDER_RE = re.compile(r"\bTitle\s*#\s*(\d+)\b", re.I)


# Single-line template slots (not real slide titles) when inferring speaker-note header.
_BAD_HYDRATE_TITLE_BRACKET_ONLY = re.compile(r"^\[[\?\d\$\s%/.—\-]+\]$")
_STATIC_IMAGE_MARKER = "[STATIC IMAGE"
_EMBEDDED_CHART_TEXT = "(embedded chart — contains data that cannot be auto-updated)"
_EMBEDDED_IMAGE_TEXTS = ("(embedded image)", "(image in shape)")
_CHART_MARKER = "[CHART — data cannot be auto-updated; replace or verify for current period]"
_IMAGE_MARKER = "[STATIC IMAGE — contains data that cannot be auto-updated; replace or verify]"


def _ensure_charts_and_images_marked(
    text_elements: list[dict], replacements: list[dict]
) -> list[dict]:
    """Append a replacement entry for every chart and image on the slide so they are always recognized and marked.

    Charts and graphs cannot be auto-updated; we ensure each is in the pipeline so speaker notes
    and INCOMPLETE banner reflect them.
    """
    originals_in_replacements: list[str] = [r.get("original", "") for r in replacements]
    added: list[dict] = []
    for el in text_elements:
        typ = el.get("type", "")
        text = el.get("text", "")
        if typ == "chart":
            added.append({
                "field": "chart",
                "original": _EMBEDDED_CHART_TEXT,
                "new_value": _CHART_MARKER,
                "mapped": False,
            })
        elif typ == "image" and text in _EMBEDDED_IMAGE_TEXTS:
            added.append({
                "field": "image",
                "original": text,
                "new_value": _IMAGE_MARKER,
                "mapped": False,
            })
    # Only add as many as we're missing (LLM or data_ask may have already included some)
    n_chart_in = sum(1 for o in originals_in_replacements if o == _EMBEDDED_CHART_TEXT)
    n_image_in = sum(1 for o in originals_in_replacements if o in _EMBEDDED_IMAGE_TEXTS)
    n_chart_el = sum(1 for el in text_elements if el.get("type") == "chart")
    n_image_el = sum(1 for el in text_elements if el.get("type") == "image" and el.get("text") in _EMBEDDED_IMAGE_TEXTS)
    chart_to_add = max(0, n_chart_el - n_chart_in)
    image_to_add = max(0, n_image_el - n_image_in)
    chart_added = [r for r in added if r.get("field") == "chart"][:chart_to_add]
    image_added = [r for r in added if r.get("field") == "image"][:image_to_add]
    return replacements + chart_added + image_added

# Data source attribution for presenter QA: where to verify each field.
DATA_SOURCE_BY_FIELD: dict[str, str] = {
    "customer_name": "Report",
    "report_date": "Report",
    "quarter": "Report",
    "quarter_start": "Report",
    "quarter_end": "Report",
    "total_users": "Pendo",
    "total_visitors": "Pendo",
    "unique_visitors": "Pendo",
    "active_users": "Pendo",
    "total_sites": "Pendo",
    "active_sites": "Pendo",
    "health_score": "Pendo",
    "account_total_minutes": "Pendo",
    "account_avg_weekly_hours": "Pendo",
    "total_shortages": "CS Report",
    "total_critical_shortages": "CS Report",
    "weekly_active_buyers_pct_avg": "CS Report",
    "site_details": "Pendo",
    "cs_health_sites": "CS Report",
    "support": "Jira",
    "salesforce": "Salesforce",
    "platform_value": "CS Report",
    "supply_chain": "CS Report",
    "github": "GitHub",
}


def _data_source_label_for_field(field: str) -> str:
    """Attribution for speaker notes; dotted fields use the top-level pipeline bucket."""
    top = (field or "").split(".", 1)[0].strip().lower()
    return DATA_SOURCE_BY_FIELD.get(top, "Report/data")


def _normalize_canonical_data_key(key: str) -> str:
    return (key or "").strip().replace(" ", "_").replace("-", "_").lower()


def _filter_chart_recommended_keys(raw: Any) -> list[str]:
    """Keep LLM-suggested keys that match our pipeline (deduped, order preserved)."""
    if raw is None:
        return []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        nk = _normalize_canonical_data_key(item)
        if nk in _AVAILABLE_DATA_KEYS:
            out.append(nk)
    seen: set[str] = set()
    deduped: list[str] = []
    for k in out:
        if k not in seen:
            seen.add(k)
            deduped.append(k)
    return deduped


def _format_data_summary_value(val: Any, max_chars: int = 2000) -> str:
    """Compact string for speaker notes (JSON for nested structures)."""
    import json as _json
    if val is None:
        return "(null)"
    if isinstance(val, (str, int, float, bool)):
        s = str(val)
        return s if len(s) <= max_chars else s[: max_chars - 3] + "..."
    try:
        s = _json.dumps(val, indent=2, default=str, ensure_ascii=False)
    except Exception:
        s = str(val)
    return s if len(s) <= max_chars else s[: max_chars - 3] + "..."


def _line_unfit_for_hydrate_slide_title(s: str) -> bool:
    """True for placeholder tokens and non-title markers — not a real slide heading."""
    t = (s or "").strip()
    if len(t) < 2:
        return True
    if t in _PLACEHOLDER_MARKERS:
        return True
    low = t.lower()
    if low.startswith("(embedded") or low.startswith("(image"):
        return True
    if len(t) <= 80 and _BAD_HYDRATE_TITLE_BRACKET_ONLY.match(t):
        return True
    return False


def _slide_title_for_notes(
    text_elements: list[dict],
    analysis: dict | None,
    *,
    slide_title: str | None = None,
) -> str:
    """Best-effort slide title: explicit param, then cached analysis, then first non-placeholder shape line."""
    if slide_title and str(slide_title).strip():
        st = str(slide_title).strip()
        if not _line_unfit_for_hydrate_slide_title(st):
            return st
    if analysis:
        t = (analysis.get("title") or "").strip()
        if t and not _line_unfit_for_hydrate_slide_title(t):
            return t
    for el in text_elements:
        if el.get("type") == "shape":
            raw = (el.get("text") or "").strip()
            if not raw:
                continue
            for line in raw.split("\n"):
                first = line.strip()
                if 2 <= len(first) <= 200 and not _line_unfit_for_hydrate_slide_title(first):
                    return first
    return ""


def _hydrate_line_is_qbr_banner_noise(s: str) -> bool:
    """True for post-hint banner lines that are not data fields (avoid duplicate 'MODIFIED' rows)."""
    t = (s or "").strip()
    if not t:
        return True
    if t.startswith("MODIFIED —"):
        return True
    low = t.lower()
    if "orange coaching" in low and "modified" in t:
        return True
    if "template cues processed" in low:
        return True
    return False


def _hydrate_data_field_line(r: dict) -> str:
    """One line: [slide text] -> [field description] -> [replacement value]."""
    orig = str(r.get("original") or "").replace("\n", " ").strip()[:200]
    fld = str(r.get("field") or "?")[:100]
    nv = str(r.get("new_value") or "").replace("\n", " ").strip()[:200]
    if _hydrate_speaker_note_row_is_generic_unmapped(r):
        return f"[{orig}] -> [generic placeholder — no pipeline mapping] -> [{nv}]"
    if _replacement_is_visual(r):
        vk = (
            "embedded chart — not auto-updated"
            if r.get("field") == "chart" or _EMBEDDED_CHART_TEXT in str(r.get("original", ""))
            else "embedded image — not auto-updated"
        )
        return f"[{orig}] -> [{vk}] -> [{nv}]"
    if r.get("mapped", True):
        src = _data_source_label_for_field(fld)
        desc = f"`{fld}` ({src})"
        syn = (r.get("synonym_phrase") or "").strip()
        if syn:
            spath = (r.get("synonym_path") or fld).strip()
            desc = f"`{fld}` (synonym: \"{syn}\" → `{spath}`) ({src})"
        return f"[{orig}] -> [{desc}] -> [{nv}]"
    return f"[{orig}] -> [`{fld}` unmapped] -> [{nv}]"


def _build_hydrate_speaker_notes(
    replacements: list[dict],
    text_elements: list[dict],
    *,
    report: dict | None = None,
    data_summary: dict | None = None,
    has_unmapped: bool = False,
    has_static_images: bool = False,
    analysis: dict | None = None,
    oai=None,
    slide_title: str | None = None,
    slide_yaml_hint: str | None = None,
) -> str:
    """Hydration speaker notes: timestamp + title, data context, Data Fields (arrow lines), visuals, checklist."""
    from datetime import datetime as _dt
    _ts = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
    ds: dict[str, Any] = {}
    if data_summary is not None:
        ds = data_summary
    elif report:
        ds = _build_data_summary(report)
    title_guess = _slide_title_for_notes(text_elements, analysis, slide_title=slide_title)
    head = f"{_ts} — {title_guess}" if title_guess else _ts
    lines: list[str] = [head]

    if analysis:
        purpose = (analysis.get("purpose") or "").strip()
        slide_type = (analysis.get("slide_type") or "").strip()
        if purpose:
            lines.append(f"Objective: {purpose}")
        if slide_type and not (title_guess and slide_type == "custom"):
            lines.append(f"Type: {slide_type}")
        data_ask = analysis.get("data_ask") or []
        if data_ask:
            keys = [str(item.get("key") or item.get("field") or "?") for item in data_ask]
            lines.append("Required data keys: " + ", ".join(keys))

    if report:
        customer = (report.get("customer") or report.get("customer_name") or "").strip()
        as_of = (report.get("generated") or report.get("report_date") or "").strip()
        quarter = (report.get("quarter") or "").strip()
        ctx_parts = [
            p
            for p in [
                f"Customer: {customer}" if customer else None,
                f"As-of: {as_of}" if as_of else None,
                f"Quarter: {quarter}" if quarter else None,
            ]
            if p
        ]
        if ctx_parts:
            lines.append("Data context: " + " | ".join(ctx_parts))

    if slide_yaml_hint and str(slide_yaml_hint).strip():
        lines.append("Slide YAML: " + str(slide_yaml_hint).strip()[:2000])
    else:
        lines.append(
            "YAML: Document each slide's field semantics in the deck YAML under `slides:` "
            "(match template yellow/orange cues). Use a comment such as "
            "`# HYDRATE_UNKNOWN: …` when a slot cannot be mapped reliably; otherwise note the "
            "intended pipeline field so hydration stays deterministic."
        )

    lines.append("Data Fields:")
    if replacements:
        for r in replacements:
            lines.append(_hydrate_data_field_line(r))
    else:
        lines.append("(none — narrative slide or no metrics matched the pipeline)")

    chart_and_image = [
        r
        for r in replacements
        if r.get("field") in ("chart", "image")
        or r.get("original") in _EMBEDDED_IMAGE_TEXTS + (_EMBEDDED_CHART_TEXT,)
    ]
    chart_specs = (analysis or {}).get("charts") or []
    if chart_and_image or chart_specs:
        lines.append("Visuals:")
        n_vis = len(chart_and_image)
        n_spec = len(chart_specs)
        n_charts = max(n_vis, n_spec)
        for i in range(n_charts):
            r = chart_and_image[i] if i < n_vis else None
            spec = chart_specs[i] if i < n_spec and isinstance(chart_specs[i], dict) else None
            if r:
                kind = (
                    "Chart/graph"
                    if r.get("field") == "chart" or _EMBEDDED_CHART_TEXT in str(r.get("original", ""))
                    else "Image (may contain data)"
                )
            elif spec:
                kind = "Visual (analysis only — no replacement row)"
            else:
                kind = "Visual"
            if spec:
                vk = (spec.get("visual_kind") or "").strip()
                if vk:
                    kind = f"{kind} [{vk}]"
                ctype = spec.get("chart_type") or "chart"
                x_lab = spec.get("x_axis") or ""
                y_lab = spec.get("y_axis") or ""
                trans = spec.get("transformations")
                if isinstance(trans, list):
                    trans = ", ".join(str(t) for t in trans)
                trans = (trans or "").strip()
                config = (spec.get("configuration") or "").strip()[:200]
                parts_spec = [f"Type: {ctype}"]
                if x_lab:
                    parts_spec.append(f"X: {x_lab}")
                if y_lab:
                    parts_spec.append(f"Y: {y_lab}")
                if trans:
                    parts_spec.append(f"Transforms: {trans[:120]}")
                if config:
                    parts_spec.append(f"Config: {config}")
                lines.append(f"  • {kind} — " + " | ".join(parts_spec))
                interp = (spec.get("interpretation") or "").strip()
                if interp:
                    ip = interp if len(interp) <= 1200 else interp[:1197] + "..."
                    lines.append(f"    What it shows: {ip}")
                rec_keys = _filter_chart_recommended_keys(spec.get("data_recommended_keys"))
                cov = (spec.get("data_coverage_note") or "").strip()
                cov_short = cov[:400] + ("..." if len(cov) > 400 else "")
                if rec_keys:
                    lines.append(
                        "    Pipeline fields (guess): " + ", ".join(rec_keys)
                    )
                elif cov_short:
                    lines.append("    Pipeline fields: (none matched — see coverage)")
                if cov_short:
                    lines.append(f"    Coverage / gaps: {cov_short}")
                if ds and rec_keys:
                    lines.append("    Data snapshot for this run:")
                    for pk in rec_keys:
                        src_lbl = _data_source_label_for_field(pk)
                        if pk in ds:
                            snap = _format_data_summary_value(ds.get(pk))
                            lines.append(f"      • {pk} [{src_lbl}]: {snap}")
                        else:
                            lines.append(f"      • {pk} [{src_lbl}]: (not in this report snapshot)")
                elif not rec_keys and (interp or cov_short):
                    lines.append(
                        "    Not auto-fetchable — source manually or extend integrations."
                    )
            else:
                lines.append(f"  • {kind} — cannot be auto-updated (no visual analysis)")

    if not replacements:
        lines.append("Slide copy (reference):")
        seen_txt: set[str] = set()
        c = 0
        for el in text_elements:
            for part in (el.get("text") or "").split("\n"):
                s = part.strip()
                if len(s) < 2 or s in seen_txt:
                    continue
                if _hydrate_line_is_qbr_banner_noise(s):
                    continue
                seen_txt.add(s)
                c += 1
                if c > 45:
                    break
                lines.append(f"  • {s[:320]}")
        if c == 0:
            lines.append("  (no extractable text)")

    lines.append(
        "QA: ✓ Values match source? ✓ Placeholders OK? ✓ Static visuals current?"
    )
    if has_unmapped or has_static_images:
        lines.append("⚠ INCOMPLETE — placeholders or static images remain. Confirm before presenting.")
    body = "\n".join(lines)
    if len(body) > 12000:
        body = body[:11900] + "\n\n… (truncated)"
    return body


def _replacement_is_visual(r: dict) -> bool:
    """True if this row is a static chart/image placeholder (not a text data swap)."""
    fld = str(r.get("field") or "")
    if fld in ("chart", "image"):
        return True
    orig = str(r.get("original") or "")
    return orig in _EMBEDDED_IMAGE_TEXTS + (_EMBEDDED_CHART_TEXT,)


def _hydrate_speaker_note_row_is_generic_unmapped(r: dict) -> bool:
    """True for placeholder-only unmapped rows we summarize as one line: ``[generic] unmapped``."""
    if r.get("mapped", True):
        return False
    if _replacement_row_is_static_visual_incomplete(r):
        return False
    fld = str(r.get("field") or "")
    if "generic placeholder" in fld.lower():
        return True
    orig = str(r.get("original") or "").strip()
    nv = str(r.get("new_value") or "").strip()
    return bool(orig == nv and nv in _PLACEHOLDER_MARKERS)


def _build_hydrate_data_match_notes(slide_entries: list[dict[str, Any]]) -> str:
    """Speaker-notes body: per-slide source field → target value (or none)."""
    lines: list[str] = [
        "Data matching — source identifier : target (or none)",
        "",
    ]
    for block in slide_entries:
        sn = block.get("slide_num")
        reps = block.get("replacements") or []
        if not reps:
            continue
        lines.append(f"Slide {sn}")
        for r in reps:
            fld = str(r.get("field") or "?").strip()
            if _replacement_is_visual(r):
                lines.append(f"  {fld} : none")
                continue
            mapped = r.get("mapped", True)
            nv = str(r.get("new_value") or "").replace("\n", " ").strip()
            target = nv if (mapped and nv) else "none"
            lines.append(f"  {fld} : {target}")
        lines.append("")
    body = "\n".join(lines).strip()
    if len(body) > 11500:
        body = body[:11400] + "\n\n… (truncated)"
    return body


def _append_hydrate_summary_slide(
    slides_svc,
    pres_id: str,
    *,
    body_text: str,
    notes_text: str,
) -> bool:
    """Append a slide at the end with summary body + speaker notes. Returns True on success."""
    from googleapiclient.errors import HttpError

    try:
        pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        insertion = len(pres.get("slides") or [])
    except HttpError as e:
        logger.warning("hydrate summary slide: could not read presentation: %s", e)
        return False

    sid = f"hydrate_run_{secrets.token_hex(8)}"
    title_oid = f"{sid}_t"
    body_oid = f"{sid}_b"
    reqs: list[dict[str, Any]] = []
    append_slide(reqs, sid, insertion)
    append_text_box(reqs, title_oid, sid, 36, 36, 648, 56, "Hydrate run summary")
    append_wrapped_text_box(reqs, body_oid, sid, 36, 108, 648, 360, body_text)
    try:
        slides_presentations_batch_update(slides_svc, pres_id, reqs)
    except HttpError as e:
        logger.warning("hydrate summary slide: batchUpdate failed: %s", e)
        return False
    if set_speaker_notes(slides_svc, pres_id, sid, notes_text):
        return True
    logger.warning("hydrate summary slide: could not write speaker notes on summary slide")
    return False


def adapt_custom_slides(
    slides_svc,
    pres_id: str,
    page_ids: list[str],
    report: dict,
    oai,
    *,
    source_presentation_name: str = "",
    run_started_at: datetime.datetime | None = None,
    title_slide_object_id: str | None = None,
    google_creds=None,
) -> dict[str, Any]:
    """Adapt slides by replacing data values with current data.

    Two-phase approach:
      Phase A (parallel)  — thumbnail fetch + GPT-4o per slide (I/O bound, safe to parallelise)
      Phase B (sequential) — Slides API batchUpdate per slide (mutates shared presentation state)

    Writes per-slide speaker notes (hydration QA: pipeline mapping, unmapped placeholders, slide copy);
    appends a summary slide at the end with run stats on-slide and aggregate data-matching details
    in that slide's speaker notes.

    When ``title_slide_object_id`` is set (e.g. QBR template cover), no incomplete banner is added
    on that slide. Banners are also omitted when the only unmapped rows are static images/charts
    (no text placeholders to flag), when cached analysis classifies the slide as title/cover/divider,
    or when the only unmapped text is long prose/headings (no metric-like characters).
    """
    _reset_hydrate_data_summary_for_tests()
    run_start = run_started_at or datetime.datetime.now(datetime.timezone.utc)
    data_summary = _build_data_summary(report)
    use_explicit_qbr = bool(report.get("_hydrate_explicit_qbr_mappings"))
    explicit_slide_type_by_page: dict[str, str] = {}
    if use_explicit_qbr:
        explicit_slide_type_by_page = build_adapt_page_slide_type_by_page_id(report, page_ids)
        logger.info("hydrate: QBR explicit mappings enabled (config/qbr_mappings.yaml); synonym phrase table skipped")
    t_adapt0 = time.perf_counter()
    stats = {
        "adapted": 0,
        "incomplete": 0,
        "clean": 0,
        "skipped": 0,
        "notes_only": 0,
        "summary_slide_added": False,
    }

    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    slides_by_id = {s["objectId"]: s for s in pres.get("slides", [])}
    ordered_ids = [s["objectId"] for s in pres.get("slides", [])]

    # ── Phase A: parallel GPT reasoning ──────────────────────────────────────
    # Fetch thumbnail URLs in parallel — each worker gets its own httplib2-backed
    # Slides service so there is no shared mutable state.
    thumb_urls: dict[str, str | None] = {}
    if google_creds is not None and len(page_ids) > 1:
        import threading as _thr
        _thread_local = _thr.local()

        def _thumb_worker(page_id: str) -> tuple[str, str | None]:
            if not hasattr(_thread_local, "svc"):
                _thread_local.svc = _build_slides_service_for_thread(google_creds)
            try:
                url = _get_slide_thumbnail_url(_thread_local.svc, pres_id, page_id)
                return page_id, url
            except Exception as e:
                slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
                logger.warning("hydrate: adapt slide %s thumbnail URL failed: %s", slide_num, e)
                return page_id, None

        logger.info("hydrate: fetching %d thumbnail URLs in parallel", len(page_ids))
        with ThreadPoolExecutor(max_workers=min(4, len(page_ids))) as pool:
            for page_id, url in pool.map(_thumb_worker, page_ids):
                thumb_urls[page_id] = url
    else:
        for page_id in page_ids:
            slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
            logger.debug("hydrate: adapt slide %s — fetching thumbnail...", slide_num)
            try:
                thumb_urls[page_id] = _get_slide_thumbnail_url(slides_svc, pres_id, page_id)
            except Exception as e:
                logger.warning("hydrate: adapt slide %s thumbnail URL failed: %s", slide_num, e)
                thumb_urls[page_id] = None

    preflight_s = time.perf_counter() - t_adapt0

    def _fetch_and_reason(page_id: str) -> tuple[str, list[dict], list[dict], str, dict | None]:
        """Returns (page_id, text_elements, replacements, cache_source, analysis_or_none).

        cache_source: analysis_hit | adapt_hit | llm | empty | error
        analysis is included when in cache (for speaker-notes rebuild spec).
        """
        slide = slides_by_id.get(page_id)
        if not slide:
            if matching_log.enabled():
                sn = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
                matching_log.emit(
                    "slide_skip", slide_ref=str(sn), reason="slide_object_not_in_presentation"
                )
            return page_id, [], [], "empty", None
        text_elements = _extract_slide_text_elements(slide.get("pageElements", []))
        if not text_elements:
            if matching_log.enabled():
                sn = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
                matching_log.emit("slide_skip", slide_ref=str(sn), reason="no_text_elements")
            return page_id, [], [], "empty", None
        slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
        slide_type_for_explicit = explicit_slide_type_by_page.get(page_id) if use_explicit_qbr else None

        def _post_llm_mapping(repls: list[dict]) -> list[dict]:
            if use_explicit_qbr:
                return apply_explicit_qbr_mappings(
                    repls,
                    text_elements,
                    data_summary,
                    slide_type=slide_type_for_explicit or None,
                    slide_ref=str(slide_num),
                )
            return apply_synonym_resolution_to_replacements(
                repls, text_elements, data_summary, slide_ref=str(slide_num)
            )

        extra_agenda = _qbr_agenda_adapt_extra_rules(report, text_elements)
        url = thumb_urls.get(page_id)
        thumb_b64 = None
        if url:
            try:
                thumb_b64 = _download_thumbnail_b64(url)
            except Exception:
                pass
        slide_cache_key = _slide_content_hash(thumb_b64, page_id=page_id) if thumb_b64 else None
        adapt_cache_key = _adapt_cache_key(thumb_b64, page_id, data_summary) if thumb_b64 else None
        analysis: dict | None = None
        if slide_cache_key:
            analysis = _get_cached_slide_analysis(slide_cache_key)
            if analysis and analysis.get("data_ask"):
                logger.debug("hydrate: adapt slide %s — analysis cache hit (resolving data ask)",
                             slide_num)
                replacements = _resolve_data_ask_to_replacements(
                    analysis["data_ask"],
                    data_summary,
                    text_elements,
                    slide_ref=str(slide_num),
                )
                replacements = _post_llm_mapping(replacements)
                replacements = _sanitize_adapt_replacements_plausible_years(
                    replacements, slide_ref=str(slide_num)
                )
                replacements = _sanitize_adapt_replacements_percent_semantics(
                    replacements, text_elements, slide_ref=str(slide_num)
                )
                replacements = _ensure_charts_and_images_marked(text_elements, replacements)
                _log_slide_pipeline_done(str(slide_num), "analysis_hit", replacements)
                return page_id, text_elements, replacements, "analysis_hit", analysis
            if adapt_cache_key is not None and not (extra_agenda or "").strip():
                cached = _get_cached_adapt(adapt_cache_key)
            else:
                cached = None
            if cached is not None:
                logger.debug("hydrate: adapt slide %s — adapt cache hit", slide_num)
                replacements = _resolve_cached_replacements(
                    cached, data_summary, slide_ref=str(slide_num)
                )
                replacements = _post_llm_mapping(replacements)
                replacements = _sanitize_adapt_replacements_plausible_years(
                    replacements, slide_ref=str(slide_num)
                )
                replacements = _sanitize_adapt_replacements_percent_semantics(
                    replacements, text_elements, slide_ref=str(slide_num)
                )
                replacements = _ensure_charts_and_images_marked(text_elements, replacements)
                _log_slide_pipeline_done(str(slide_num), "adapt_hit", replacements)
                return page_id, text_elements, replacements, "adapt_hit", analysis
        n_total = len(text_elements)
        n_data = sum(1 for el in text_elements if _element_may_contain_data(el))
        logger.debug("hydrate: adapt slide %s — asking %s (%d/%d elements contain data)...",
                     slide_num, LLM_MODEL, n_data, n_total)
        replacements = _get_data_replacements(
            oai,
            text_elements,
            data_summary,
            thumb_b64,
            slide_label=str(slide_num),
            extra_system_rules=extra_agenda,
        )
        replacements = _post_llm_mapping(replacements)
        replacements = _sanitize_adapt_replacements_plausible_years(
            replacements, slide_ref=str(slide_num)
        )
        replacements = _sanitize_adapt_replacements_percent_semantics(
            replacements, text_elements, slide_ref=str(slide_num)
        )
        replacements = _ensure_charts_and_images_marked(text_elements, replacements)
        if adapt_cache_key and replacements and not (extra_agenda or "").strip():
            _set_cached_adapt(adapt_cache_key, replacements)
        if slide_cache_key and not analysis:
            analysis = _get_cached_slide_analysis(slide_cache_key)
        _log_slide_pipeline_done(str(slide_num), "llm", replacements)
        return page_id, text_elements, replacements, "llm", analysis

    results: dict[str, tuple[list[dict], list[dict], dict | None]] = {}
    adapt_cache_counts: dict[str, int] = {}
    t_a0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_fetch_and_reason, pid): pid for pid in page_ids}
        for fut in as_completed(futures):
            try:
                pid, text_elements, replacements, src, analysis = fut.result()
                adapt_cache_counts[src] = adapt_cache_counts.get(src, 0) + 1
                results[pid] = (text_elements, replacements, analysis)
            except Exception as e:
                pid = futures[fut]
                sn = ordered_ids.index(pid) + 1 if pid in ordered_ids else "?"
                logger.warning("hydrate: slide %s — fetch/GPT reasoning failed: %s", sn, e)
                adapt_cache_counts["error"] = adapt_cache_counts.get("error", 0) + 1
                results[pid] = ([], [], None)
    phase_a_s = time.perf_counter() - t_a0

    # ── Phase B: sequential Slides API writes — clear per-slide notes; summary at end ─
    notes_updates: list[tuple[str, str]] = []
    t_b0 = time.perf_counter()
    for page_id in page_ids:
        text_elements, replacements, analysis = results.get(page_id, ([], [], None))
        replacements = _merge_qbr_agenda_title_replacements(text_elements, replacements, report)
        slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"

        if not text_elements:
            stats["skipped"] += 1
            _print(f"    slide {slide_num}: no text on slide")
            notes_updates.append((page_id, ""))
            continue

        if not replacements:
            stats["notes_only"] += 1
            _print(f"    slide {slide_num}: no data values — speaker notes set to hydration QA stub")
            notes_updates.append(
                (
                    page_id,
                    _build_hydrate_speaker_notes(
                        [],
                        text_elements,
                        report=report,
                        data_summary=data_summary,
                        analysis=analysis,
                        slide_title=(analysis or {}).get("title"),
                        oai=oai,
                    ),
                )
            )
            continue

        mapped_count = sum(1 for r in replacements if r.get("mapped"))
        unmapped_count = sum(1 for r in replacements if not r.get("mapped"))
        _print(f"    slide {slide_num}: {mapped_count} mapped, {unmapped_count} unmapped")

        replace_reqs, has_unmapped, has_static_images = _apply_adaptations(
            slides_svc, pres_id, page_id, replacements
        )
        if has_static_images:
            _print(f"      ↳ contains static image(s) with data")
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
                                    "hydrate: slide %s — agenda title-bar autofit NONE failed: %s",
                                    slide_num,
                                    ae,
                                )
                except Exception as e:
                    logger.warning(
                        "hydrate: slide %s — post-adapt font clamp failed (slide may show oversized metrics): %s",
                        slide_num,
                        e,
                    )
            except Exception as e:
                logger.warning("hydrate: slide %s — failed to apply text replacements: %s",
                               slide_num, e)
                stats["skipped"] += 1
                notes_updates.append((page_id, ""))
                continue

        if has_unmapped:
            style_reqs = _red_style_placeholders(slides_svc, pres_id, page_id)
            if _should_add_incomplete_banner(page_id, replacements, title_slide_object_id, analysis):
                style_reqs.extend(_add_incomplete_banner(page_id, has_static_images=has_static_images))
            if style_reqs:
                try:
                    slides_presentations_batch_update(slides_svc, pres_id, style_reqs)
                except Exception as e:
                    logger.warning("hydrate: slide %s — failed to apply red placeholder styling: %s",
                                   slide_num, e)
            stats["incomplete"] += 1
        else:
            stats["clean"] += 1

        stats["adapted"] += 1
        notes_updates.append(
            (
                page_id,
                _build_hydrate_speaker_notes(
                    replacements,
                    text_elements,
                    report=report,
                    data_summary=data_summary,
                    has_unmapped=has_unmapped,
                    has_static_images=has_static_images,
                    analysis=analysis,
                    slide_title=(analysis or {}).get("title"),
                    oai=oai,
                ),
            )
        )

    phase_b_slides_s = time.perf_counter() - t_b0

    t_n0 = time.perf_counter()
    if notes_updates:
        n = set_speaker_notes_batch(slides_svc, pres_id, notes_updates)
        logger.info("hydrate: wrote speaker notes for %d/%d slides in single batchUpdate", n, len(notes_updates))
    speaker_notes_s = time.perf_counter() - t_n0

    t_tail0 = time.perf_counter()
    run_end = datetime.datetime.now(datetime.timezone.utc)
    src_label = (source_presentation_name or pres_id or "(unknown)")[:200]

    slides_processed = len(page_ids)
    slides_with_data = 0
    charts_identified = 0
    charts_not_identified = 0
    elements_replaced = 0
    elements_not_updated = 0

    for _pid, (te, reps, analysis) in results.items():
        if reps:
            slides_with_data += 1
        for r in reps:
            if _replacement_is_visual(r):
                elements_not_updated += 1
            elif r.get("mapped"):
                elements_replaced += 1
            else:
                elements_not_updated += 1

        chs = (analysis or {}).get("charts") if analysis else None
        if isinstance(chs, list) and chs:
            for ch in chs:
                if not isinstance(ch, dict):
                    continue
                if _filter_chart_recommended_keys(ch.get("data_recommended_keys")):
                    charts_identified += 1
                else:
                    charts_not_identified += 1
        else:
            ncr = sum(
                1 for r in reps
                if r.get("field") == "chart" or _EMBEDDED_CHART_TEXT in str(r.get("original", ""))
            )
            charts_not_identified += ncr

    slide_match_entries: list[dict[str, Any]] = []
    for page_id in page_ids:
        _te, reps, _an = results.get(page_id, ([], [], None))
        if not reps:
            continue
        sn = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else 0
        slide_match_entries.append({"slide_num": sn, "replacements": reps})

    if slide_match_entries:
        match_notes = _build_hydrate_data_match_notes(slide_match_entries)
    else:
        match_notes = (
            "Data matching — source identifier : target (or none)\n\n"
            "(No slides with replacement rows in this run.)"
        )
    dur = run_end - run_start
    dur_s = int(dur.total_seconds())
    _mm, ss = divmod(dur_s, 60)
    hh, mm = divmod(_mm, 60)
    dur_str = f"{hh}:{mm:02d}:{ss:02d}" if hh else f"{mm}:{ss:02d}"

    summary_body = "\n".join([
        f"Source presentation: {src_label}",
        "",
        f"Start: {run_start.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"End:   {run_end.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Duration: {dur_str}",
        "",
        f"Slides processed: {slides_processed}",
        f"Slides with data found: {slides_with_data}",
        "",
        f"Charts & graphs — data identified: {charts_identified} / not identified: {charts_not_identified}",
        "",
        f"Data elements — replaced with current data: {elements_replaced} / not updated: {elements_not_updated}",
    ])

    if _append_hydrate_summary_slide(
        slides_svc,
        pres_id,
        body_text=summary_body,
        notes_text=match_notes,
    ):
        stats["summary_slide_added"] = True
    else:
        stats["summary_slide_added"] = False
        logger.warning("hydrate: could not append summary slide (see logs above)")

    stats["run_started_utc"] = run_start.isoformat()
    stats["run_ended_utc"] = run_end.isoformat()
    stats["duration_seconds"] = dur_s
    stats["slides_with_data"] = slides_with_data
    stats["charts_identified"] = charts_identified
    stats["charts_not_identified"] = charts_not_identified
    stats["elements_replaced"] = elements_replaced
    stats["elements_not_updated"] = elements_not_updated

    n_pages = len(page_ids)
    ah = adapt_cache_counts.get("analysis_hit", 0)
    adh = adapt_cache_counts.get("adapt_hit", 0)
    llm_n = adapt_cache_counts.get("llm", 0)
    empty_n = adapt_cache_counts.get("empty", 0)
    err_n = adapt_cache_counts.get("error", 0)
    cache_served = ah + adh
    stats["cache"] = {
        "analysis_hit": ah,
        "adapt_hit": adh,
        "llm": llm_n,
        "empty": empty_n,
        "error": err_n,
        "total_slides": n_pages,
    }
    if n_pages:
        logger.info(
            "hydrate: adapt cache summary — %s | analysis_hit=%d adapt_hit=%d llm=%d empty=%d error=%d",
            _cache_hit_rate_line("served_from_cache", cache_served, n_pages),
            ah, adh, llm_n, empty_n, err_n,
        )
    tail_s = time.perf_counter() - t_tail0
    total_adapt_s = time.perf_counter() - t_adapt0
    stats["timing"] = {
        "preflight_pres_and_thumb_urls_s": round(preflight_s, 3),
        "phase_a_parallel_gpt_s": round(phase_a_s, 3),
        "phase_b_sequential_slides_api_s": round(phase_b_slides_s, 3),
        "speaker_notes_batch_s": round(speaker_notes_s, 3),
        "tail_summary_slide_and_stats_s": round(tail_s, 3),
        "total_adapt_s": round(total_adapt_s, 3),
    }
    _den = max(total_adapt_s, 0.01)
    logger.info(
        "hydrate: adapt wall time — total=%.1fs (preflight=%.1fs, phase_A_parallel_gpt=%.1fs, "
        "phase_B_slides=%.1fs, speaker_notes=%.1fs, tail_summary+stats=%.1fs) — as %% of total: %s",
        total_adapt_s,
        preflight_s,
        phase_a_s,
        phase_b_slides_s,
        speaker_notes_s,
        tail_s,
        "/".join(
            f"{(x / _den) * 100.0:.0f}%"
            for x in (preflight_s, phase_a_s, phase_b_slides_s, speaker_notes_s, tail_s)
        ),
    )
    return stats


# ── Replication ──

# Slide types that use live data (vs. static/structural slides)
_DATA_SLIDE_TYPES = {
    "title", "health", "engagement", "sites", "features", "champions",
    "benchmarks", "exports", "depth", "kei", "guides", "jira", "signals",
    "platform_health", "supply_chain", "platform_value", "sla_health",
    "cross_validation", "engineering", "enhancements", "team",
    "qbr_cover", "qbr_deployment",
}

_STRUCTURAL_SLIDE_TYPES = {
    "qbr_agenda", "qbr_divider", "data_quality",
}

# Hydrate Phase 3: never run in-place LLM text replacement on these (editorial / title slides).
_HYDRATE_SKIP_TEXT_ADAPT_TYPES = frozenset({
    "title",
    "qbr_cover",
    "qbr_divider",
})

def hydrate_new_slides(customer_override: str | None = None) -> list[dict[str, Any]]:
    """Hydrate presentations shared with GOOGLE_HYDRATE_INTAKE_GROUP using live data.

    Auto-detects the customer from each presentation title. If customer_override
    is provided, uses that for all decks (useful for generating a template-style
    deck for a different customer).
    """
    global _print_context
    _print_context = "hydrate"
    presentations, empty_msg = _collect_hydrate_intake_presentations(log_prefix="hydrate")
    if empty_msg:
        _print(empty_msg)
        return []

    try:
        assert_qbr_prompts_ready_or_raise()
    except Exception as e:
        _print(f"\nCannot start hydrate (Prompts / adapt_system_prompt.yaml): {e}\n")
        sys.exit(1)

    # Load known customer names for auto-detection
    from .pendo_client import PendoClient
    from .quarters import resolve_quarter
    from .slides_api import _get_service
    from .slides_client import get_slide_builder
    from googleapiclient.errors import HttpError

    qr = resolve_quarter()
    days = qr.days
    pc = PendoClient()

    # Build customer list for auto-detection
    known_customers: list[str] = []
    if not customer_override:
        _print("Loading customer list for auto-detection...")
        try:
            known_customers = pc.get_sites_by_customer(days)["customer_list"]
        except Exception:
            pass

    slides_svc, drive_svc, _google_creds = _get_service()
    oai = llm_client()
    all_results: list[dict[str, Any]] = []
    report_cache: dict[str, dict] = {}
    # Aggregate cache stats across all decks in this run (printed at the end)
    run_cache_totals = {
        "class_slides": 0,
        "class_avoided": 0,
        "adapt_slides": 0,
        "adapt_served": 0,
    }

    for pres in presentations:
        source_id = pres["id"]
        pres_name = pres["name"]
        g = pres.get("group_email") or GOOGLE_HYDRATE_INTAKE_GROUP
        _print(f"{'─' * 60}")
        _print(f"Source: {pres_name}  (intake: group {g})")
        _print(f"{'─' * 60}")

        # Determine the customer for this deck
        customer = customer_override or _detect_customer(pres_name, known_customers)
        if not customer:
            _print(f"  Could not detect customer from title. "
                   f"Re-run with: decks --hydrate <customer>\n")
            all_results.append({"name": pres_name, "error": "customer not detected"})
            continue
        _print(f"  Customer: {customer}")

        full_pres = slides_svc.presentations().get(presentationId=source_id).execute()
        source_slides = full_pres.get("slides", [])
        original_slide_count = len(source_slides)
        if HYDRATE_MAX_SLIDES > 0 and original_slide_count > HYDRATE_MAX_SLIDES:
            source_slides = source_slides[:HYDRATE_MAX_SLIDES]
            _print(
                f"  {len(source_slides)} of {original_slide_count} slides to classify and rebuild "
                f"(HYDRATE_MAX_SLIDES={HYDRATE_MAX_SLIDES})\n"
            )
            logger.info(
                "hydrate: capping at %d slides (source had %d)",
                HYDRATE_MAX_SLIDES,
                original_slide_count,
            )
        else:
            _print(f"  {len(source_slides)} slides to classify and rebuild\n")

        class_cache = {
            "analysis_hit": 0, "legacy_classification_hit": 0,
            "fresh_analysis": 0, "no_cache_key_classify": 0,
        }
        # Phase 1: Classify every slide
        slide_plan: list[dict] = []
        for si, slide in enumerate(source_slides, 1):
            texts = []
            for el in slide.get("pageElements", []):
                texts.extend(_extract_text(el))
            slide_text = "\n".join(texts)
            elements = _describe_elements(slide)
            title_guess = texts[0][:60] if texts else "(no text)"

            logger.debug("hydrate: [%d/%d] fetching thumbnail — %s",
                         si, len(source_slides), title_guess)
            try:
                thumb_b64 = _get_slide_thumbnail_b64(slides_svc, source_id, slide["objectId"])
            except Exception:
                thumb_b64 = None

            cache_key = _slide_content_hash(
                thumb_b64, slide_text[:2000] if slide_text else "", page_id=slide["objectId"]
            )
            if cache_key:
                analysis = _get_cached_slide_analysis(cache_key)
                if analysis:
                    logger.debug("hydrate: [%d/%d] slide analysis cache hit", si, len(source_slides))
                    class_cache["analysis_hit"] += 1
                    classification = {
                        "slide_type": analysis.get("slide_type", "custom"),
                        "title": analysis.get("title", title_guess),
                        "reasoning": analysis.get("reasoning", ""),
                        "custom_sections": analysis.get("custom_sections"),
                    }
                else:
                    classification = _get_cached_classification(cache_key)
                    if classification:
                        logger.debug("hydrate: [%d/%d] classification cache hit", si, len(source_slides))
                        class_cache["legacy_classification_hit"] += 1
                    else:
                        logger.debug("hydrate: [%d/%d] analyzing slide (data ask + purpose)...",
                                     si, len(source_slides))
                        classification = _analyze_slide_broad(
                            oai, slide_text, elements, thumb_b64, si, len(source_slides), pres_name
                        )
                        _set_cached_slide_analysis(cache_key, classification)
                        class_cache["fresh_analysis"] += 1
            else:
                logger.debug("hydrate: [%d/%d] classifying slide (no cache key)...",
                             si, len(source_slides))
                classification = _classify_slide(
                    oai, slide_text, elements, thumb_b64, si, len(source_slides), pres_name
                )
                class_cache["no_cache_key_classify"] += 1
            slide_type = classification.get("slide_type", "custom")
            title = classification.get("title", title_guess)
            reasoning = classification.get("reasoning", "")

            _print(f"  [{si}/{len(source_slides)}] \"{title_guess}\"  → {slide_type}")
            if reasoning:
                _print(f"       {reasoning}")

            slide_plan.append({
                "slide_num": si,
                "slide_type": slide_type,
                "title": title,
                "text": slide_text,
                "elements": elements,
                "custom_sections": classification.get("custom_sections"),
            })

        n_cls = len(source_slides)
        cls_avoided = class_cache["analysis_hit"] + class_cache["legacy_classification_hit"]
        logger.info(
            "hydrate: classification cache summary — %s | analysis_hit=%d legacy_classification=%d fresh_analysis=%d no_key=%d",
            _cache_hit_rate_line("avoided_LLM", cls_avoided, n_cls),
            class_cache["analysis_hit"],
            class_cache["legacy_classification_hit"],
            class_cache["fresh_analysis"],
            class_cache["no_cache_key_classify"],
        )
        _print(f"\n  Classification cache: {cls_avoided}/{n_cls} slides avoided classification LLM "
               f"({100 * cls_avoided // n_cls if n_cls else 0}%) "
               f"[analysis={class_cache['analysis_hit']} legacy={class_cache['legacy_classification_hit']} "
               f"fresh={class_cache['fresh_analysis']} no_key={class_cache['no_cache_key_classify']}]")
        _print(f"  Classification complete. Building deck...\n")
        run_cache_totals["class_slides"] += n_cls
        run_cache_totals["class_avoided"] += cls_avoided

        # Load health report only after classification — Phase 1 uses slide text + vision, not Pendo/Jira/SF.
        # Report is required for Phase 2 (e.g. data_quality rebuild) and Phase 3 (text adaptation + notes).
        if customer in report_cache:
            report = report_cache[customer]
        else:
            _print(f"  Loading customer metrics (Pendo + integrations) for copy & adaptation...")
            report = pc.get_customer_health_report(customer, days=days)
            if "error" in report:
                _print(f"  Failed to load data: {report['error']}\n")
                all_results.append({"name": pres_name, "customer": customer, "error": report["error"]})
                continue
            report["quarter"] = qr.label
            report["quarter_start"] = qr.start.isoformat()
            report["quarter_end"] = qr.end.isoformat()
            report_cache[customer] = report
        _print(f"  Data ready ({days}d window, {qr.label})\n")

        # Phase 2: Copy the source presentation, then replace only data slides
        import datetime
        date_str = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')})"
        out_title = f"{customer} — {pres_name} ({date_str})"

        try:
            copy_body: dict[str, Any] = {"name": out_title}
            output_folder = get_deck_output_folder_id()
            if output_folder:
                copy_body["parents"] = [output_folder]
            copied = drive_svc.files().copy(
                fileId=source_id, body=copy_body, fields="id",
            ).execute()
            pres_id = copied["id"]
        except HttpError as e:
            status = getattr(e.resp, "status", None) or "?"
            reason = getattr(e.resp, "reason", "") or ""
            detail = ""
            try:
                body = json.loads(e.content.decode("utf-8")) if getattr(e, "content", None) else {}
                err = body.get("error", {})
                if isinstance(err, dict):
                    detail = err.get("message", "")
                    errs = err.get("errors", [])
                    if errs and isinstance(errs[0], dict) and errs[0].get("reason"):
                        detail = f"{detail} ({errs[0]['reason']})" if detail else errs[0]["reason"]
            except Exception:
                pass
            _print(f"  FAIL copying presentation: HTTP {status} {reason}")
            if detail:
                _print(f"    {detail}")
            _print(f"    Source file (group intake): {pres_name}")
            _print(f"    File ID: {source_id}")
            _print(f"    Open: https://docs.google.com/presentation/d/{source_id}/edit")
            if status == 403:
                _print("    Hint: Service account may lack access to the source file or output folder.")
            elif status == 404:
                _print("    Hint: Drive returned 404 (file not found for this request). Typical causes:")
                _print("      • Presentation was deleted, moved to trash, or the file id is stale/wrong.")
                _print("      • The integration user cannot see the file for copy (some setups surface this as 404).")
                _print("    Fix: Open the URL above; if it loads, share that file with the service account as Editor.")
                _print("    Fix: Restore from trash if applicable, or re-share the deck with the intake group.")
            all_results.append({
                "name": pres_name,
                "error": f"copy failed: HTTP {status} {detail or str(e)}",
                "source_id": source_id,
                "source_url": f"https://docs.google.com/presentation/d/{source_id}/edit",
            })
            continue

        # Read the slide object IDs from the copy
        copy_pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        copy_slides = copy_pres.get("slides", [])
        # Drop slides beyond the cap so output matches slide_plan (copy duplicates full source first).
        if HYDRATE_MAX_SLIDES > 0 and len(copy_slides) > HYDRATE_MAX_SLIDES:
            trim_reqs = [
                {"deleteObject": {"objectId": s["objectId"]}}
                for s in copy_slides[HYDRATE_MAX_SLIDES:]
            ]
            if trim_reqs:
                try:
                    slides_presentations_batch_update(slides_svc, pres_id, trim_reqs)
                except HttpError as e:
                    logger.warning("hydrate: failed to trim slides past cap: %s", e)
                else:
                    copy_pres = slides_svc.presentations().get(presentationId=pres_id).execute()
                    copy_slides = copy_pres.get("slides", [])
                    _print(
                        f"  Output trimmed to first {len(copy_slides)} slides "
                        f"(HYDRATE_MAX_SLIDES={HYDRATE_MAX_SLIDES}).\n"
                    )

        # Attach slide plan to report so builders like qbr_agenda can use it
        report["_slide_plan"] = [
            {"id": sp["slide_type"], "slide_type": sp["slide_type"], "title": sp["title"]}
            for sp in slide_plan if sp["slide_type"] != "skip"
        ]
        # Phase 2: Delete slides classified as "skip", and rebuild the one purely-mechanical
        # slide type (data_quality — color-coded boxes, no editorial content).
        # All other slides are kept exactly as-is; their data values will be updated
        # in-place during Phase 3.  Builder functions belong to the "build" path and
        # must NOT be called here, to avoid overwriting the customer's editorial content.
        reqs: list[dict] = []
        delete_ids: list[str] = []
        offset = 0
        built = 0
        kept = 0
        skipped = 0

        for i, sp in enumerate(slide_plan):
            st = sp["slide_type"]
            orig_oid = copy_slides[i]["objectId"] if i < len(copy_slides) else None

            if st == "skip":
                if orig_oid:
                    delete_ids.append(orig_oid)
                skipped += 1
                continue

            # data_quality is 100% mechanical (colored indicators, no editorial text);
            # rebuild it so current source health is always accurate.
            if st == "data_quality":
                builder = get_slide_builder("data_quality")
                if builder and orig_oid:
                    report["_current_slide"] = {"id": st, "slide_type": st, "title": sp["title"]}
                    insert_idx = i + 1 + offset
                    sid = f"s_dq_{i}"
                    try:
                        new_idx = builder(reqs, sid, report, insert_idx)
                        created = new_idx - insert_idx
                        if created > 0:
                            offset += created
                            delete_ids.append(orig_oid)
                            built += 1
                            continue
                    except Exception as e:
                        logger.warning("data_quality builder failed, keeping original: %s", e)
                kept += 1
                continue

            kept += 1

        # Delete the original slides that we replaced
        for oid in delete_ids:
            reqs.append({"deleteObject": {"objectId": oid}})

        # Execute
        url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
        logger.info("hydrate: phase 2 — built=%d rebuilt, kept=%d, skipped=%d",
                    built, kept, skipped)
        if reqs:
            try:
                slides_presentations_batch_update(slides_svc, pres_id, reqs)
            except HttpError as e:
                _print(f"  FAIL applying slide changes: {e}")
                all_results.append({"name": pres_name, "error": str(e)[:200]})
                continue

        _print(f"  Phase 2: {built} rebuilt, {kept} kept, {skipped} skipped")
        _print(f"  Output: {url}")

        # Phase 3: In-place data adaptation for kept slides — except title/cover/divider,
        # which stay as the customer authored them (classification gates this).
        # Re-fetch slide list after Phase 2 mutations so objectIds are current.
        adapted_pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        adapted_slides = adapted_pres.get("slides", [])
        filtered_plan = [sp for sp in slide_plan if sp.get("slide_type") != "skip"]
        adapt_page_ids: list[str] = []
        if len(adapted_slides) == len(filtered_plan):
            n_skip_adapt = 0
            for slide, sp in zip(adapted_slides, filtered_plan):
                st = (sp.get("slide_type") or "custom").strip()
                if st in _HYDRATE_SKIP_TEXT_ADAPT_TYPES:
                    n_skip_adapt += 1
                    logger.debug(
                        "hydrate: skipping text adaptation for slide_type=%s (%s)",
                        st,
                        (sp.get("title") or "")[:60],
                    )
                    continue
                adapt_page_ids.append(slide["objectId"])
            if n_skip_adapt:
                _print(
                    f"  Skipping in-place data swap on {n_skip_adapt} title/cover/divider slide(s).\n"
                )
        else:
            logger.warning(
                "hydrate: slide count mismatch after Phase 2 (%d slides vs %d non-skip in plan) — "
                "adapting all slides (cannot align types to skip title slides)",
                len(adapted_slides),
                len(filtered_plan),
            )
            adapt_page_ids = [s["objectId"] for s in adapted_slides]

        if adapt_page_ids:
            _print(f"\n  Adapting {len(adapt_page_ids)} slides with current data...")
            adapt_phase_started = datetime.datetime.now(datetime.timezone.utc)
            adapt_stats = adapt_custom_slides(
                slides_svc, pres_id, adapt_page_ids, report, oai,
                source_presentation_name=pres_name,
                run_started_at=adapt_phase_started,
                google_creds=_google_creds,
            )
            if adapt_stats.get("summary_slide_added"):
                _print(
                    "  Summary slide appended at end of deck (stats on slide; per-field matching in its notes)."
                )
            _print(f"  Adapted: {adapt_stats['adapted']} slides "
                   f"({adapt_stats['clean']} clean, {adapt_stats['incomplete']} incomplete, "
                   f"{adapt_stats['skipped']} unchanged, "
                   f"{adapt_stats.get('notes_only', 0)} notes-only)")
            if adapt_stats.get("adapted", 0) == 0:
                _print(
                    "  Note: The health report was used for this pass (data_quality rebuild if any, speaker "
                    "notes, and adaptation attempts), but no slide body text was replaced — often normal for "
                    "mostly static or custom slides."
                )
                logger.info(
                    "hydrate: Phase 3 applied 0 text replacements; report still used for notes / builders "
                    "where applicable (deck=%s)",
                    pres_name,
                )
            c = adapt_stats.get("cache") or {}
            tot = c.get("total_slides") or 0
            if tot:
                served = c.get("analysis_hit", 0) + c.get("adapt_hit", 0)
                _print(f"  Adapt cache: {served}/{tot} slides avoided adapt LLM "
                       f"({100 * served // tot if tot else 0}%) "
                       f"[analysis={c.get('analysis_hit', 0)} legacy_adapt={c.get('adapt_hit', 0)} "
                       f"llm={c.get('llm', 0)}]")
                run_cache_totals["adapt_slides"] += tot
                run_cache_totals["adapt_served"] += served
        else:
            _print(
                "\n  Phase 3 (adapt): skipped — no slides marked for in-place data swap "
                "(only title / qbr_cover / qbr_divider slides, or slide/plan length mismatch)."
            )
            logger.info("hydrate: Phase 3 skipped — adapt_page_ids empty")

        if HYDRATE_REMOVE_INTAKE_GROUP_PERMISSION and GOOGLE_HYDRATE_INTAKE_GROUP:
            n_perm = _remove_intake_group_permission_from_file(
                drive_svc, source_id, GOOGLE_HYDRATE_INTAKE_GROUP
            )
            if n_perm:
                _print(
                    f"  Removed intake group {GOOGLE_HYDRATE_INTAKE_GROUP!r} "
                    f"from source deck sharing ({n_perm} permission(s))."
                )

        result_entry: dict[str, Any] = {
            "name": pres_name, "customer": customer,
            "url": url, "built": built, "kept": kept, "skipped": skipped,
        }
        all_results.append(result_entry)
        _print("")

    _print(f"{'=' * 60}")
    _print(f"Replication complete: {len(all_results)} deck(s)")
    _print(f"{'=' * 60}")
    _print("")
    _print("CACHE HIT RATE (entire run)")
    cs, ca = run_cache_totals["class_slides"], run_cache_totals["class_avoided"]
    if cs:
        cp = 100 * ca // cs
        _print(f"  Classification LLM skipped: {ca}/{cs} slides ({cp}% hit rate)")
        logger.info("hydrate: run summary — classification cache %s", _cache_hit_rate_line("hit", ca, cs))
    else:
        _print("  Classification: no slides processed this run")
    ads, adh = run_cache_totals["adapt_slides"], run_cache_totals["adapt_served"]
    if ads:
        ap = 100 * adh // ads
        _print(f"  Adapt LLM skipped:       {adh}/{ads} slides ({ap}% hit rate)")
        logger.info("hydrate: run summary — adapt cache %s", _cache_hit_rate_line("hit", adh, ads))
    else:
        _print(
            "  Adapt: no adapt cache stats (Phase 3 did not run — e.g. copy failed, "
            "or every slide skipped adaptation, or no intake decks processed)."
        )
    _print(f"{'=' * 60}")
    return all_results
