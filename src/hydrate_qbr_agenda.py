"""QBR agenda-specific hydrate replacement helpers."""

from __future__ import annotations

import re
from typing import Any

from .config import logger
from .slide_loader import get_slide_definition

TITLE_HASH_PLACEHOLDER_RE = re.compile(r"\bTitle\s*#\s*(\d+)\b", re.I)


def qbr_agenda_items_from_plan(slide_plan: list[dict[str, Any]]) -> list[str]:
    """Same section ordering as the QBR agenda slide builder."""
    divider_items = [
        str(entry.get("title", "")).strip()
        for entry in slide_plan
        if entry.get("slide_type", entry.get("id", "")) == "qbr_divider" and entry.get("title")
    ]
    divider_items = [title for title in divider_items if title]
    if divider_items:
        return divider_items

    skip_types = {"qbr_cover", "qbr_agenda", "title", "data_quality", "skip"}
    out: list[str] = []
    for entry in slide_plan:
        slide_type = entry.get("slide_type", entry.get("id", ""))
        if slide_type in skip_types:
            continue
        title = str(entry.get("title", "") or "").strip() or str(entry.get("id", "")).replace("_", " ").title()
        if title:
            out.append(title)
    return out


def qbr_agenda_hydrate_config(report: dict[str, Any]) -> dict[str, Any]:
    """``hydrate`` block from the report bundle or ``slides/qbr-02-agenda.yaml``."""
    hints = report.get("_hydrate_slide_hints")
    if isinstance(hints, dict) and isinstance(hints.get("qbr_agenda"), dict):
        return hints["qbr_agenda"]
    slide_def = get_slide_definition("qbr_agenda")
    if isinstance(slide_def, dict) and isinstance(slide_def.get("hydrate"), dict):
        return slide_def["hydrate"]
    return {}


def slide_looks_like_qbr_agenda_titles_legacy(text_elements: list[dict[str, Any]]) -> bool:
    blob = "\n".join((element.get("text") or "") for element in text_elements)
    if "agenda" in blob.lower():
        return True
    return bool(TITLE_HASH_PLACEHOLDER_RE.search(blob))


def slide_matches_qbr_agenda_hydrate(
    text_elements: list[dict[str, Any]], agenda_hydrate: dict[str, Any]
) -> bool:
    """Whether this slide is the QBR agenda for title merge."""
    template_detection = (agenda_hydrate.get("template") or {}).get("slide_detection")
    if template_detection is None or not isinstance(template_detection, dict):
        return slide_looks_like_qbr_agenda_titles_legacy(text_elements)
    words = template_detection.get("body_contains_word")
    if isinstance(words, str):
        words = [words]
    if not isinstance(words, list):
        words = []
    pattern = (template_detection.get("body_matches_regex") or "").strip()
    if not words and not pattern:
        return slide_looks_like_qbr_agenda_titles_legacy(text_elements)

    blob = "\n".join((element.get("text") or "") for element in text_elements)
    blob_lower = blob.lower()
    matched = any(str(word).lower() in blob_lower for word in words)
    if not matched and pattern:
        try:
            matched = bool(re.search(pattern, blob, re.I))
        except re.error as e:
            logger.warning("hydrate: qbr_agenda slide_detection.body_matches_regex invalid: %s", e)
    return matched


def compiled_title_slot_pattern(section_titles: dict[str, Any]) -> re.Pattern[str]:
    pattern = (section_titles.get("title_slot_regex") or "").strip()
    if pattern:
        try:
            return re.compile(pattern, re.I)
        except re.error as e:
            logger.warning("hydrate: qbr_agenda title_slot_regex invalid: %s — using default", e)
    return TITLE_HASH_PLACEHOLDER_RE


def truncate_agenda_line(value: str, max_chars: int) -> str:
    """Trim to ``max_chars`` with a trailing ellipsis when shortened."""
    text = re.sub(r"\s+", " ", (value or "").strip())
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    if max_chars == 1:
        return text[0]
    return text[: max_chars - 1] + "…"


def shorten_agenda_label(raw: str, mode: str, max_title_chars: int | None) -> str:
    """Shorten a deck section title for narrow agenda rows."""
    text = re.sub(r"\s+", " ", (raw or "").strip())
    if not text:
        return ""
    mode = (mode or "none").strip().lower()
    if mode in ("", "none"):
        if max_title_chars is not None:
            try:
                max_chars = int(max_title_chars)
                if max_chars > 0:
                    return truncate_agenda_line(text, max_chars)
            except (TypeError, ValueError):
                pass
        return text[:200] if len(text) > 200 else text

    if mode == "acronym":
        words = re.findall(r"[A-Za-z0-9]+", text)
        if not words:
            return truncate_agenda_line(text, max_title_chars or 12)
        out = "".join(word[0].upper() for word in words[:12])
        if len(out) > 8:
            out = out[:8]
    elif mode == "first_word":
        out = _pick_first_non_stopword(text, max_words=1)
    elif mode == "first_two_words":
        out = _pick_first_non_stopword(text, max_words=2)
    else:
        out = text

    if max_title_chars is not None:
        try:
            max_chars = int(max_title_chars)
            if max_chars > 0:
                return truncate_agenda_line(out, max_chars)
        except (TypeError, ValueError):
            pass
    return out[:200] if len(out) > 200 else out


def _pick_first_non_stopword(text: str, *, max_words: int) -> str:
    skip = {"the", "a", "an", "and", "or", "of", "for", "in", "on", "at", "to"}
    picked: list[str] = []
    fallback: list[str] = []
    for token in text.split():
        core = token.strip(".,!?;:\"'()[]")
        if not core:
            continue
        fallback.append(core)
        if core.lower() in skip and not picked:
            continue
        picked.append(core)
        if len(picked) >= max_words:
            break
    if picked:
        return " ".join(picked[:max_words])
    return " ".join(fallback[:max_words]) if fallback else text


def qbr_agenda_label_shortening_mode(report: dict[str, Any], section_titles: dict[str, Any]) -> str:
    override = report.get("_qbr_agenda_shorten_mode_override")
    if isinstance(override, str) and override.strip():
        return override.strip().lower()
    label_shortening = section_titles.get("label_shortening") if isinstance(section_titles, dict) else None
    if isinstance(label_shortening, dict):
        mode = label_shortening.get("mode")
        if isinstance(mode, str) and mode.strip():
            return mode.strip().lower()
    return "none"


def qbr_agenda_effective_max_chars(report: dict[str, Any], section_titles: dict[str, Any]) -> int | None:
    """``max_chars_per_section_title`` with optional vision/YAML scale or override."""
    base = section_titles.get("max_chars_per_section_title")
    try:
        base_int = int(base) if base is not None else None
    except (TypeError, ValueError):
        base_int = None
    override = report.get("_qbr_agenda_max_chars_override")
    if override is not None:
        try:
            override_int = int(override)
            if override_int > 0:
                return override_int
        except (TypeError, ValueError):
            pass
    scale = report.get("_qbr_agenda_max_chars_scale")
    if base_int is not None and scale is not None:
        try:
            scale_float = float(scale)
            if 0 < scale_float <= 2:
                return max(1, int(round(base_int * scale_float)))
        except (TypeError, ValueError):
            pass
    return base_int


def build_qbr_title_hash_replacements(
    text_elements: list[dict[str, Any]],
    items: list[str],
    *,
    pattern: re.Pattern[str] | None = None,
    max_title_chars: int | None = None,
    shorten_mode: str = "none",
) -> list[dict[str, Any]]:
    """One replaceAllText row per distinct title-slot substring on the slide."""
    rx = pattern or TITLE_HASH_PLACEHOLDER_RE
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for element in text_elements:
        text = element.get("text") or ""
        for match in rx.finditer(text):
            exact = match.group(0)
            number = int(match.group(1))
            if number < 1 or number > len(items) or exact in seen:
                continue
            seen.add(exact)
            rows.append({
                "original": exact,
                "new_value": shorten_agenda_label(items[number - 1], shorten_mode, max_title_chars),
                "mapped": True,
                "field": "qbr_agenda_section",
            })
    return rows


def qbr_agenda_title_bar_shape_object_ids(
    text_elements: list[dict[str, Any]], report: dict[str, Any]
) -> list[str]:
    """Object IDs of text shapes that hold ``Title #N`` slots."""
    saved = report.get("_qbr_agenda_title_shape_ids")
    if isinstance(saved, list) and saved:
        out = [str(item) for item in saved if item]
        if out:
            return out
    agenda_hydrate = qbr_agenda_hydrate_config(report)
    if not slide_matches_qbr_agenda_hydrate(text_elements, agenda_hydrate):
        return []
    section_titles = (agenda_hydrate.get("template") or {}).get("section_titles") or {}
    if not isinstance(section_titles, dict) or section_titles.get("from_deck_plan") is False:
        return []
    pattern = compiled_title_slot_pattern(section_titles)
    out: list[str] = []
    seen: set[str] = set()
    for element in text_elements:
        if element.get("type") != "shape":
            continue
        if not pattern.search(element.get("text") or ""):
            continue
        object_id = element.get("element_id")
        if not object_id or object_id in seen:
            continue
        seen.add(object_id)
        out.append(str(object_id))
    return out


def build_qbr_agenda_reshorten_replacements(
    text_elements: list[dict[str, Any]], report: dict[str, Any]
) -> list[dict[str, Any]]:
    """After placeholders are gone, shorten full section titles already on the slide."""
    plan = report.get("_slide_plan")
    if not isinstance(plan, list) or not plan:
        return []
    agenda_hydrate = qbr_agenda_hydrate_config(report)
    if not slide_matches_qbr_agenda_hydrate(text_elements, agenda_hydrate):
        return []
    section_titles = (agenda_hydrate.get("template") or {}).get("section_titles") or {}
    if not isinstance(section_titles, dict) or section_titles.get("from_deck_plan") is False:
        return []
    mode = qbr_agenda_label_shortening_mode(report, section_titles)
    if mode in ("", "none"):
        return []
    items = qbr_agenda_items_from_plan(plan)
    if not items:
        return []
    blob = "\n".join((element.get("text") or "") for element in text_elements if element.get("type") == "shape")
    max_title = qbr_agenda_effective_max_chars(report, section_titles)
    rows: list[dict[str, Any]] = []
    seen_originals: set[str] = set()
    for item in items:
        raw = (item or "").strip()
        if not raw or raw not in blob:
            continue
        short = shorten_agenda_label(raw, mode, max_title)
        if short == raw or raw in seen_originals:
            continue
        seen_originals.add(raw)
        rows.append({
            "original": raw,
            "new_value": short,
            "mapped": True,
            "field": "qbr_agenda_section_reshorten",
        })
    return rows


def shape_autofit_none_requests(object_ids: list[str]) -> list[dict[str, Any]]:
    """Slides API requests to disable text autofit on shapes."""
    reqs: list[dict[str, Any]] = []
    for object_id in object_ids:
        if not object_id:
            continue
        reqs.append({
            "updateShapeProperties": {
                "objectId": object_id,
                "shapeProperties": {"autofit": {"autofitType": "NONE"}},
                "fields": "autofit",
            }
        })
    return reqs


def qbr_agenda_adapt_extra_rules(report: dict[str, Any], text_elements: list[dict[str, Any]]) -> str:
    """Extra adapt system rules from ``slides/qbr-02-agenda.yaml``."""
    agenda_hydrate = qbr_agenda_hydrate_config(report)
    if not isinstance(agenda_hydrate, dict):
        return ""
    if not slide_matches_qbr_agenda_hydrate(text_elements, agenda_hydrate):
        return ""
    template = agenda_hydrate.get("template")
    if not isinstance(template, dict):
        template = {}
    raw = template.get("adapt_instructions")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    section_titles = template.get("section_titles") or {}
    if not isinstance(section_titles, dict):
        return ""
    max_title = section_titles.get("max_chars_per_section_title")
    max_description = section_titles.get("max_chars_per_description")
    if max_title is None and max_description is None:
        return ""
    lines = [
        "QBR AGENDA SLIDE (this slide only) — strict character limits so rows fit the template layout.",
    ]
    if max_title is not None:
        try:
            lines.append(
                f"Each section title line must be at most {int(max_title)} characters "
                "(truncate with … if needed; do not shrink font size to cheat)."
            )
        except (TypeError, ValueError):
            pass
    if max_description is not None:
        try:
            lines.append(
                f"Each description line under a section title must be at most {int(max_description)} characters "
                "(truncate with … if needed; do not shrink font size to cheat)."
            )
        except (TypeError, ValueError):
            pass
    lines.append("Treat every distinct short line under a numbered agenda row as a “description” for this limit.")
    return "\n".join(lines)


def merge_qbr_agenda_title_replacements(
    text_elements: list[dict[str, Any]],
    base_replacements: list[dict[str, Any]],
    report: dict[str, Any],
) -> list[dict[str, Any]]:
    """Replace template title slots with QBR section titles; drop conflicting base rows."""
    plan = report.get("_slide_plan")
    if not isinstance(plan, list) or not plan:
        return base_replacements

    agenda_hydrate = qbr_agenda_hydrate_config(report)
    if not slide_matches_qbr_agenda_hydrate(text_elements, agenda_hydrate):
        return base_replacements

    section_titles = (agenda_hydrate.get("template") or {}).get("section_titles") or {}
    if section_titles.get("from_deck_plan") is False:
        return base_replacements
    slot_labels = (section_titles.get("slot_labels") or "title_number_hash").strip()
    if slot_labels != "title_number_hash":
        logger.warning(
            "hydrate: qbr_agenda slot_labels=%r not supported — skipping section title merge",
            slot_labels,
        )
        return base_replacements

    items = qbr_agenda_items_from_plan(plan)
    if not items:
        return base_replacements
    pattern = compiled_title_slot_pattern(section_titles)
    title_rows = build_qbr_title_hash_replacements(
        text_elements,
        items,
        pattern=pattern,
        max_title_chars=qbr_agenda_effective_max_chars(report, section_titles),
        shorten_mode=qbr_agenda_label_shortening_mode(report, section_titles),
    )
    if not title_rows:
        return base_replacements

    object_ids: list[str] = []
    for element in text_elements:
        if element.get("type") != "shape":
            continue
        if not pattern.search(element.get("text") or ""):
            continue
        object_id = element.get("element_id")
        if object_id:
            object_ids.append(str(object_id))
    if object_ids:
        report["_qbr_agenda_title_shape_ids"] = object_ids

    originals = {str(row["original"]).strip() for row in title_rows}
    filtered = [
        row for row in base_replacements
        if str(row.get("original", "")).strip() not in originals
    ]
    return filtered + title_rows
