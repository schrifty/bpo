"""Pure replacement filtering and sanitizer helpers for hydrate/adapt."""

from __future__ import annotations

import re
from typing import Any

from . import matching_log
from .config import logger

ADAPT_SPELLED_NUMBER_RE = re.compile(
    r"\b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
    r"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|"
    r"thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|million|billion)\b",
    re.I,
)
ADAPT_MONTH_NAME_RE = re.compile(
    r"\b(?:january|february|march|april|june|july|august|september|october|november|december)\b",
    re.I,
)
ADAPT_QUARTER_OR_PERCENT_RE = re.compile(r"\b(?:q[1-4]|quarter|percent(?:age)?)\b", re.I)
ADAPT_PERCENT_PLACEHOLDER_RE = re.compile(r"\[00%\]|\[00\s*%\]", re.I)
ADAPT_YEARS_CONTEXT_RE = re.compile(r"\b(?:years?|lifetime|tenure|since\s+join)\b", re.I)
ADAPT_WRONG_TIME_UNIT_FIELD_RE = re.compile(
    r"\b(?:total_)?minutes?\b|\bhours?\b|weekly\s*hours|avg[_\s]*hours|account_total_minutes",
    re.I,
)


def element_may_contain_data(element: dict[str, Any]) -> bool:
    """Return True if this element is worth sending to GPT for data replacement."""
    text = element.get("text", "")
    if text.startswith("(embedded") or text.startswith("(image"):
        return True
    if len(text) <= 2:
        return False
    if re.search(r"[\d%$€£¥#]", text):
        return True
    return bool(
        ADAPT_SPELLED_NUMBER_RE.search(text)
        or ADAPT_MONTH_NAME_RE.search(text)
        or ADAPT_QUARTER_OR_PERCENT_RE.search(text)
    )


def text_has_percentage_semantics(value: str) -> bool:
    if not (value or "").strip():
        return False
    if "%" in value or "\uFF05" in value or "\u2030" in value:
        return True
    if ADAPT_PERCENT_PLACEHOLDER_RE.search(value):
        return True
    return bool(re.search(r"\b(?:percent|per\s+cent|pct)\b", value, re.I))


def original_reads_as_percent_on_slide(original: str, text_elements: list[dict[str, Any]]) -> bool:
    """True when *original* appears immediately before a percent marker on the slide."""
    needle = (original or "").strip()
    if not needle:
        return False
    if text_has_percentage_semantics(needle):
        return True
    if not re.match(r"^[\d.,]+\s*$", needle):
        return False
    try:
        float(needle.replace(",", ""))
    except ValueError:
        return False
    for element in text_elements:
        text = element.get("text") or ""
        if needle not in text:
            continue
        pos = 0
        while True:
            idx = text.find(needle, pos)
            if idx < 0:
                break
            end = idx + len(needle)
            if end < len(text) and text[end] == "%":
                return True
            pos = idx + 1
    return False


def normalize_adapt_replacements(replacements: list[Any]) -> list[dict[str, Any]]:
    """Keep only well-formed replacement dicts; coerce types; drop rows missing original."""
    out: list[dict[str, Any]] = []
    if not isinstance(replacements, list):
        return []
    for index, row in enumerate(replacements):
        if not isinstance(row, dict):
            logger.warning("hydrate: adapt replacement[%d] skipped (not a dict)", index)
            continue
        original = row.get("original")
        if original is None:
            logger.warning("hydrate: adapt replacement[%d] skipped (missing original)", index)
            continue
        original_s = str(original).strip()
        if not original_s:
            continue
        if len(original_s) == 1 and original_s.isdigit():
            logger.warning(
                "hydrate: adapt replacement[%d] skipped (single-digit original would match "
                "every occurrence on the page, e.g. P1/P2 priority labels): original=%r",
                index,
                original_s,
            )
            continue
        new_value = row.get("new_value", "")
        if new_value is None:
            new_value = ""
        elif not isinstance(new_value, str):
            new_value = str(new_value)
        if text_has_percentage_semantics(original_s) and not text_has_percentage_semantics(new_value.strip()):
            logger.warning(
                "hydrate: adapt replacement[%d] skipped (percentage original requires "
                "percentage new_value): original=%r new_value=%r",
                index,
                original_s[:120],
                (new_value or "")[:120],
            )
            continue
        field = row.get("field", "")
        if field is not None and not isinstance(field, str):
            field = str(field)
        out.append({
            "original": original_s,
            "new_value": new_value,
            "mapped": bool(row.get("mapped", True)),
            "field": (field or "").strip(),
        })
    return out


def placeholder_for_percent_mismatch(original: str) -> str:
    """Placeholder when a percent slot was filled with a non-percentage value."""
    original = (original or "").strip()
    match = re.match(r"^[\d.,\s$€£%]+", original)
    if not match:
        return "[00%]"
    suffix = (original[match.end():].strip() if match else "").strip()
    return f"[00%] {suffix}" if suffix else "[00%]"


def sanitize_adapt_replacements_percent_semantics(
    replacements: list[dict[str, Any]],
    text_elements: list[dict[str, Any]] | None,
    *,
    slide_ref: str = "",
) -> list[dict[str, Any]]:
    """Demote mapped rows where the slide shows a percent but new_value lost %."""
    out: list[dict[str, Any]] = []
    for row in replacements:
        if not bool(row.get("mapped", True)):
            out.append(row)
            continue
        original = str(row.get("original") or "")
        new_value = str(row.get("new_value") or "")
        percent_context = (
            original_reads_as_percent_on_slide(original, text_elements)
            if text_elements
            else text_has_percentage_semantics(original)
        )
        if not percent_context or text_has_percentage_semantics(new_value.strip()):
            out.append(row)
            continue
        logger.debug(
            "hydrate: demoting replacement (percent context requires %% in new_value): "
            "original=%r new_value=%r",
            original[:120],
            new_value[:120],
        )
        field = str(row.get("field") or "")
        if matching_log.enabled():
            matching_log.emit(
                "sanitize_percent_demote",
                slide_ref=slide_ref,
                original=original[:200],
                new_value=new_value[:200],
                field=field[:200],
                reason="percent_context_but_new_value_missing_percent",
            )
        out.append({
            "original": original,
            "new_value": placeholder_for_percent_mismatch(original),
            "mapped": False,
            "field": (field + " (percent slot; verify manually)").strip(),
        })
    return out


def dedupe_replacements_by_original(replacements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Later rows win."""
    by_original: dict[str, dict[str, Any]] = {}
    for row in replacements:
        by_original[row["original"]] = row
    return list(by_original.values())


def first_number_in_new_value(value: str) -> float | None:
    """Leading numeric token from new_value."""
    if not (value or "").strip():
        return None
    match = re.match(r"^\s*\$?\s*([\d,]+(?:\.\d+)?)", value.strip())
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def placeholder_for_years_context(original: str) -> str:
    """Placeholder that preserves a trailing 'years' label when the slide had one."""
    if ADAPT_YEARS_CONTEXT_RE.search(original or ""):
        return "[000] years"
    return "[000]"


def sanitize_adapt_replacements_plausible_years(
    replacements: list[dict[str, Any]], *, slide_ref: str = ""
) -> list[dict[str, Any]]:
    """Demote absurd years values such as minutes/hours mistaken for years."""
    out: list[dict[str, Any]] = []
    for row in replacements:
        original = str(row.get("original") or "")
        new_value = str(row.get("new_value") or "")
        field = str(row.get("field") or "")
        if not bool(row.get("mapped", True)) or not ADAPT_YEARS_CONTEXT_RE.search(original):
            out.append(row)
            continue
        number = first_number_in_new_value(new_value)
        if number is None:
            out.append(row)
            continue
        abs_number = abs(number)
        wrong_unit = bool(ADAPT_WRONG_TIME_UNIT_FIELD_RE.search(field))
        if abs_number > 150 or (wrong_unit and abs_number > 50):
            logger.warning(
                "hydrate: demoting implausible years replacement: original=%r new_value=%r field=%r",
                original[:120],
                new_value[:120],
                field[:120],
            )
            if matching_log.enabled():
                matching_log.emit(
                    "sanitize_years_demote",
                    slide_ref=slide_ref,
                    original=original[:200],
                    new_value=new_value[:200],
                    field=field[:200],
                    parsed_number=number,
                    reason="abs_gt_150_or_time_unit_mismatch" if abs_number > 150 else "wrong_time_unit",
                )
            out.append({
                "original": original,
                "new_value": placeholder_for_years_context(original),
                "mapped": False,
                "field": (field + " (implausible years; verify manually)").strip(),
            })
            continue
        out.append(row)
    return out
