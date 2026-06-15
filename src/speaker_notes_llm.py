"""LLM-generated VP management guidance appended to speaker notes."""

from __future__ import annotations

import json
import os
from typing import Any

from .config import LLM_MODEL_FAST, logger, llm_client
from .llm_utils import _llm_create_with_retry

_MAX_METRICS_JSON_CHARS = 6000
_MAX_SLIDE_PROMPT_CHARS = 2000
_MAX_USER_PROMPT_CHARS = 12_000

_MANAGEMENT_GUIDANCE_HEADER = "How to use this slide"

_SYSTEM_PROMPT = (
    "You write speaker notes for a VP of Engineering reviewing a slide deck.\n"
    "Output a single paragraph (4–6 sentences, plain text only).\n"
    "Explain specifically how the information ON THIS SLIDE helps the VP manage:\n"
    "- Engineering delivery and quality (when relevant)\n"
    "- Implementation / customer rollout (when relevant)\n"
    "- Support operations (when relevant)\n"
    "- QA and test coverage (when relevant)\n"
    "- DevOps / platform reliability (when relevant)\n"
    "- Compliance / governance (when relevant)\n"
    "- Business impact for the rest of the organization (when relevant)\n"
    "Pick only the management angles that genuinely apply; do not force every category.\n"
    "Be concrete: reference specific metrics, labels, or trends from the context when available.\n"
    "Name specific decisions, conversations, or interventions this slide enables—not generic advice.\n"
    "Do NOT repeat JQL/SOQL trace details or restate the slide title.\n"
    "No markdown, bullets, section headers, or a label like 'How to use:'—paragraph only.\n"
    "Tone: direct, analytical, executive; never salesy."
)

__all__ = [
    "enrich_speaker_notes_with_management_guidance",
    "generate_slide_management_guidance",
    "speaker_notes_llm_allow_fallback",
    "speaker_notes_llm_enabled",
]


def speaker_notes_llm_enabled() -> bool:
    v = (os.environ.get("BPO_SPEAKER_NOTES_LLM", "true") or "").strip().lower()
    return v not in ("0", "false", "no", "off")


def speaker_notes_llm_allow_fallback() -> bool:
    v = (os.environ.get("BPO_SPEAKER_NOTES_LLM_ALLOW_FALLBACK", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on", "allow")


def _fallback_paragraph(slide_title: str, slide_type: str) -> str:
    label = (slide_title or slide_type or "this slide").strip()
    return (
        f"Use {label} to anchor executive review on the metrics shown, validate against source "
        "systems, and decide whether current trends require staffing, process, or priority changes "
        "across Engineering, Support, or customer-facing teams."
    )


def _truncate_json(obj: Any, max_chars: int) -> str:
    raw = json.dumps(obj, default=str, ensure_ascii=False)
    if len(raw) <= max_chars:
        return raw
    return raw[: max_chars - 3] + "..."


def _slide_yaml_prompt(slide_id: str | None, slide_type: str | None) -> str:
    from .slide_loader import get_slide_definition

    for sid in (slide_id, slide_type):
        if not sid:
            continue
        defn = get_slide_definition(str(sid))
        if isinstance(defn, dict):
            prompt = (defn.get("prompt") or "").strip()
            if prompt:
                return prompt[:_MAX_SLIDE_PROMPT_CHARS]
    return ""


def _report_metrics_blob(report: dict[str, Any], data_keys: list[str]) -> str:
    if not report or not data_keys:
        return ""
    blob: dict[str, Any] = {}
    for key in data_keys:
        val = report.get(key)
        if val is None:
            continue
        blob[key] = val
    if not blob:
        return ""
    return _truncate_json(blob, _MAX_METRICS_JSON_CHARS)


def _hydrate_replacements_summary(replacements: list[dict[str, Any]] | None) -> str:
    if not replacements:
        return ""
    lines: list[str] = []
    for r in replacements[:24]:
        fld = str(r.get("field") or r.get("original") or "?").strip()
        nv = str(r.get("new_value") or "").strip()
        if not nv or nv in ("—", "-", "N/A"):
            continue
        if len(nv) > 120:
            nv = nv[:117] + "..."
        lines.append(f"{fld}: {nv}")
    return "\n".join(lines)


def _build_user_prompt(
    *,
    slide_title: str,
    slide_type: str,
    deck_id: str = "",
    customer: str = "",
    slide_yaml_prompt: str = "",
    metrics_blob: str = "",
    existing_notes_excerpt: str = "",
    hydrate_analysis: dict[str, Any] | None = None,
    hydrate_replacements_summary: str = "",
    slide_copy_excerpt: str = "",
) -> str:
    parts: list[str] = [
        f"Slide title: {slide_title or '(untitled)'}",
        f"Slide type: {slide_type or 'unknown'}",
    ]
    if deck_id:
        parts.append(f"Deck: {deck_id}")
    if customer:
        parts.append(f"Customer / scope: {customer}")
    if slide_yaml_prompt:
        parts.append(f"Slide design prompt (YAML):\n{slide_yaml_prompt}")
    if hydrate_analysis:
        purpose = (hydrate_analysis.get("purpose") or "").strip()
        if purpose:
            parts.append(f"Slide objective: {purpose}")
        interp = (hydrate_analysis.get("interpretation") or "").strip()
        if interp:
            parts.append(f"Visual interpretation: {interp[:800]}")
    if metrics_blob:
        parts.append(f"Key metrics from report slice (JSON):\n{metrics_blob}")
    if hydrate_replacements_summary:
        parts.append(f"Mapped values on slide:\n{hydrate_replacements_summary}")
    if slide_copy_excerpt:
        parts.append(f"Visible slide copy:\n{slide_copy_excerpt}")
    if existing_notes_excerpt:
        parts.append(
            "Existing speaker notes excerpt (for context; do not repeat trace queries):\n"
            + existing_notes_excerpt[:2500]
        )
    prompt = "\n\n".join(parts)
    if len(prompt) > _MAX_USER_PROMPT_CHARS:
        prompt = prompt[: _MAX_USER_PROMPT_CHARS - 3] + "..."
    return prompt


def generate_slide_management_guidance(
    *,
    slide_title: str = "",
    slide_type: str = "",
    deck_id: str = "",
    customer: str = "",
    slide_id: str = "",
    slide_yaml_hint: str = "",
    report: dict[str, Any] | None = None,
    entry: dict[str, Any] | None = None,
    data_keys: list[str] | None = None,
    existing_notes: str = "",
    hydrate_analysis: dict[str, Any] | None = None,
    hydrate_replacements: list[dict[str, Any]] | None = None,
    slide_copy_excerpt: str = "",
) -> str:
    """Return one VP-focused management paragraph, or ``""`` on failure (unless fallback enabled)."""
    if not speaker_notes_llm_enabled():
        return ""

    report = report or {}
    entry = entry or {}
    st = (slide_type or entry.get("slide_type") or entry.get("id") or "").strip()
    title = (slide_title or entry.get("title") or st.replace("_", " ").title()).strip()
    sid = (slide_id or entry.get("id") or "").strip()
    yaml_prompt = (
        (slide_yaml_hint or "").strip()
        or str(entry.get("prompt") or "").strip()
        or _slide_yaml_prompt(sid, st)
    )

    keys = list(data_keys or [])
    if not keys and st:
        from .slide_metadata import SLIDE_DATA_REQUIREMENTS

        keys = list(SLIDE_DATA_REQUIREMENTS.get(st, []))

    deck = (deck_id or report.get("_deck_id") or report.get("deck_id") or "").strip()
    cust = (
        customer
        or report.get("customer")
        or report.get("customer_name")
        or ("Portfolio" if report.get("type") == "portfolio" else "")
    )
    cust = str(cust or "").strip()

    metrics_blob = _report_metrics_blob(report, keys)
    repl_summary = _hydrate_replacements_summary(hydrate_replacements)
    notes_excerpt = (existing_notes or "").strip()

    user_prompt = _build_user_prompt(
        slide_title=title,
        slide_type=st,
        deck_id=deck,
        customer=str(cust),
        slide_yaml_prompt=yaml_prompt,
        metrics_blob=metrics_blob,
        existing_notes_excerpt=notes_excerpt,
        hydrate_analysis=hydrate_analysis,
        hydrate_replacements_summary=repl_summary,
        slide_copy_excerpt=slide_copy_excerpt,
    )

    try:
        client = llm_client()
        resp = _llm_create_with_retry(
            client,
            model=LLM_MODEL_FAST,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.35,
            max_tokens=1024,
        )
        text = " ".join((resp.choices[0].message.content or "").split()).strip()
        text = text.lstrip("•-–—* ").strip()
        if text.lower().startswith(_MANAGEMENT_GUIDANCE_HEADER.lower()):
            text = text[len(_MANAGEMENT_GUIDANCE_HEADER) :].lstrip(": ").strip()
        return text
    except Exception as e:
        logger.warning(
            "Speaker notes management guidance failed for slide_type=%r title=%r: %s",
            st,
            title,
            e,
        )
        if speaker_notes_llm_allow_fallback():
            return _fallback_paragraph(title, st)
        return ""


def enrich_speaker_notes_with_management_guidance(
    base_notes: str,
    *,
    report: dict[str, Any] | None = None,
    entry: dict[str, Any] | None = None,
    slide_title: str = "",
    slide_type: str = "",
    slide_yaml_hint: str = "",
    hydrate_analysis: dict[str, Any] | None = None,
    hydrate_replacements: list[dict[str, Any]] | None = None,
    slide_copy_excerpt: str = "",
) -> str:
    """Append LLM management guidance to existing speaker notes when enabled."""
    base = (base_notes or "").rstrip()
    if not speaker_notes_llm_enabled():
        return base

    paragraph = generate_slide_management_guidance(
        slide_title=slide_title,
        slide_type=slide_type,
        slide_yaml_hint=slide_yaml_hint,
        report=report,
        entry=entry,
        existing_notes=base,
        hydrate_analysis=hydrate_analysis,
        hydrate_replacements=hydrate_replacements,
        slide_copy_excerpt=slide_copy_excerpt,
    )
    if not paragraph:
        return base

    block = f"\n\n{_MANAGEMENT_GUIDANCE_HEADER}\n{paragraph}"
    combined = base + block
    if len(combined) > 14_000:
        combined = combined[:13_900] + "\n\n… (truncated)"
    return combined
