"""Explicit QBR hydrate mappings from ``config/qbr_mappings.yaml``.

When ``report[REPORT_KEY_EXPLICIT_QBR_MAPPINGS]`` is true, :func:`adapt_custom_slides` uses
:func:`apply_explicit_qbr_mappings` instead of synonym-phrase resolution from ``data_field_synonyms``.
"""

from __future__ import annotations

import re
import threading
from pathlib import Path
from typing import Any

import yaml

from .config import logger

# Report dict flag set by ``qbr_template`` before ``adapt_custom_slides`` (template QBR path only).
REPORT_KEY_EXPLICIT_QBR_MAPPINGS = "_hydrate_explicit_qbr_mappings"

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_PATH = _REPO_ROOT / "config" / "qbr_mappings.yaml"

_LOAD_LOCK = threading.Lock()
_cached: dict[str, Any] | None = None
_cached_mtime: float | None = None

_SYNONYM_TRIGGER_PLACEHOLDERS = frozenset(
    ("[000]", "[$000]", "[00/00/00]", "[00%]", "[???]")
)


def load_qbr_mappings(*, path: Path | None = None) -> dict[str, Any]:
    """Load ``qbr_mappings.yaml`` (cached by mtime). Returns empty mappings if missing."""
    global _cached, _cached_mtime
    p = path or _DEFAULT_PATH
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return {"version": 2, "slides": [], "global_elements": []}
    with _LOAD_LOCK:
        if _cached is not None and _cached_mtime == mtime:
            return _cached
        try:
            raw = yaml.safe_load(p.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as e:
            logger.warning("qbr_mappings: could not load %s — %s", p, e)
            raw = {}
        if not isinstance(raw, dict):
            raw = {}
        _cached = raw
        _cached_mtime = mtime
        return _cached


def build_adapt_page_slide_type_by_page_id(
    report: dict[str, Any],
    adapt_page_ids: list[str],
) -> dict[str, str]:
    """Align ``adapt_page_ids`` order to ``report['_slide_plan']`` rows that receive text adapt.

    Uses :data:`evaluate._HYDRATE_SKIP_TEXT_ADAPT_TYPES` (lazy import to avoid cycles at import time).
    """
    from .evaluate import _HYDRATE_SKIP_TEXT_ADAPT_TYPES

    skip = _HYDRATE_SKIP_TEXT_ADAPT_TYPES
    plan = report.get("_slide_plan") or []
    seq: list[str] = []
    for sp in plan:
        if not isinstance(sp, dict):
            continue
        st = (sp.get("slide_type") or sp.get("id") or "").strip()
        if st in skip:
            continue
        seq.append(st)
    out: dict[str, str] = {}
    for i, pid in enumerate(adapt_page_ids):
        if i < len(seq):
            out[str(pid)] = seq[i]
        else:
            out[str(pid)] = ""
    return out


def _normalize_slide_id(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in ("null", "none", "~"):
        return None
    return s


def _coerce_slide_number(raw: Any) -> int | None:
    """YAML may use int or string; null means rule applies on any slide (subject to slide_id)."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw > 0 else None
    s = str(raw).strip()
    if not s or s.lower() in ("null", "none", "~"):
        return None
    try:
        n = int(s)
    except ValueError:
        return None
    return n if n > 0 else None


def _row_from_element(
    ent: dict[str, Any],
    *,
    slide_number: int | None,
    parent_slide_id: str | None,
) -> dict[str, Any]:
    sid = _normalize_slide_id(ent.get("slide_id"))
    if sid is None:
        sid = parent_slide_id
    name = ent.get("name") if ent.get("name") is not None else ent.get("data_element_name")
    return {
        "slide_number": slide_number,
        "slide_id": sid,
        "data_element_name": str(name).strip() if name is not None else "",
        "source": str(ent.get("source") or "").strip(),
        "target": str(ent.get("target") or "").strip(),
    }


def expand_mapping_rules(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten YAML into rule dicts: slide_number, slide_id, data_element_name, source, target.

    Supports:

    * **version >= 2** (or presence of ``slides`` / ``global_elements``): structured
      ``slides: [{ slide_number, slide_id?, elements: [{ name, source, target, slide_id? }] }]``
      and ``global_elements: [{ name, source, target, slide_id? }]``.
    * **version 1** (legacy): ``mappings`` + ``bracket_placeholder_sources`` flat lists with
      optional ``slide_id``; no per-slide numbering.
    """
    rows: list[dict[str, Any]] = []
    ver = cfg.get("version", 1)
    try:
        ver_int = int(ver)
    except (TypeError, ValueError):
        ver_int = 1
    use_v2 = ver_int >= 2 or "slides" in cfg or "global_elements" in cfg

    if use_v2:
        for block in cfg.get("slides") or []:
            if not isinstance(block, dict):
                continue
            block_sn = _coerce_slide_number(block.get("slide_number"))
            block_sid = _normalize_slide_id(block.get("slide_id"))
            for ent in block.get("elements") or []:
                if isinstance(ent, dict):
                    rows.append(_row_from_element(ent, slide_number=block_sn, parent_slide_id=block_sid))
        for ent in cfg.get("global_elements") or []:
            if isinstance(ent, dict):
                rows.append(_row_from_element(ent, slide_number=None, parent_slide_id=None))
        if rows or ver_int >= 2:
            return rows

    for ent in cfg.get("mappings") or []:
        if isinstance(ent, dict):
            rows.append(
                {
                    "slide_number": _coerce_slide_number(ent.get("slide_number")),
                    "slide_id": _normalize_slide_id(ent.get("slide_id")),
                    "data_element_name": str(
                        ent.get("data_element_name") or ent.get("name") or ""
                    ).strip(),
                    "source": str(ent.get("source") or "").strip(),
                    "target": str(ent.get("target") or "").strip(),
                }
            )
    for ent in cfg.get("bracket_placeholder_sources") or []:
        if isinstance(ent, dict):
            rows.append(
                {
                    "slide_number": _coerce_slide_number(ent.get("slide_number")),
                    "slide_id": _normalize_slide_id(ent.get("slide_id")),
                    "data_element_name": str(
                        ent.get("data_element_name") or ent.get("name") or ""
                    ).strip(),
                    "source": str(ent.get("source") or "").strip(),
                    "target": str(ent.get("target") or "").strip(),
                }
            )
    return rows


def _normalize_context(s: str) -> str:
    t = (s or "").replace("\u00a0", " ").lower().strip()
    return re.sub(r"\s+", " ", t)


def apply_explicit_qbr_mappings(
    replacements: list[dict],
    text_elements: list[dict],
    data_summary: dict[str, Any],
    *,
    slide_type: str | None,
    slide_ref: str = "",
    slide_number: int | None = None,
) -> list[dict]:
    """Apply ``config/qbr_mappings.yaml`` rules (phrase or exact placeholder → dotted path)."""
    from .data_field_synonyms import (
        _format_scalar_for_slide,
        _narrow_synonym_haystack,
        _value_present,
        data_summary_lookup,
    )
    from .evaluate import (
        _adapt_original_reads_as_percent_on_slide,
        _adapt_text_has_percentage_semantics,
    )

    cfg = load_qbr_mappings()
    rows = expand_mapping_rules(cfg)
    st_filter = (slide_type or "").strip()
    out: list[dict] = []
    for r in replacements:
        r = dict(r)
        fld = (r.get("field") or "").strip().lower()
        if fld in ("chart", "image"):
            out.append(r)
            continue
        mapped = bool(r.get("mapped", True))
        nv = str(r.get("new_value") or "").strip()
        try_explicit = (not mapped) or (nv in _SYNONYM_TRIGGER_PLACEHOLDERS)
        if not try_explicit:
            out.append(r)
            continue
        orig = str(r.get("original") or "")
        applied = False
        for ent in rows:
            src = str(ent.get("source") or "").strip()
            tgt = str(ent.get("target") or "").strip()
            rule_sn = ent.get("slide_number")
            if rule_sn is not None:
                if slide_number is None:
                    continue
                if int(rule_sn) != int(slide_number):
                    continue
            sid_raw = ent.get("slide_id")
            if sid_raw is not None and str(sid_raw).strip() not in ("", "null"):
                if st_filter and str(sid_raw).strip() != st_filter:
                    continue
            if not src or not tgt:
                continue
            is_bracket = src.startswith("[") and src.endswith("]")
            if is_bracket:
                if orig.strip() != src:
                    continue
            else:
                hay = _narrow_synonym_haystack(orig, text_elements)
                h = _normalize_context(hay)
                if len(h) < 4 or _normalize_context(src) not in h:
                    continue
            raw = data_summary_lookup(data_summary, tgt)
            if not _value_present(raw):
                continue
            if isinstance(raw, (dict, list)):
                continue
            fv = _float_scalar(raw)
            if fv is not None and abs(fv) > 150:
                if _adapt_text_has_percentage_semantics(orig) or _adapt_original_reads_as_percent_on_slide(
                    orig, text_elements
                ):
                    continue

            raw_s = _format_scalar_for_slide(raw, path=tgt)
            m = re.match(r"^[\d.,\s$€£%]+", orig)
            suffix = (orig[m.end() :].strip() if m else "").strip()
            pct_in_prefix = bool(m and "%" in m.group())
            percent_slot = (
                pct_in_prefix
                or _adapt_text_has_percentage_semantics(orig)
                or _adapt_original_reads_as_percent_on_slide(orig, text_elements)
            )
            if percent_slot and "%" not in raw_s and not raw_s.endswith("%"):
                raw_s = f"{raw_s}%"
            new_val = f"{raw_s} {suffix}".strip() if suffix else raw_s

            r["mapped"] = True
            r["field"] = tgt
            r["new_value"] = new_val
            r["synonym_phrase"] = src
            r["synonym_path"] = tgt
            elem_label = ent.get("data_element_name") or ""
            if elem_label:
                r["qbr_mapping_element"] = elem_label
            applied = True
            logger.debug(
                "qbr_mappings: slide %s%s applied %r → %s (element=%r)",
                slide_ref,
                f" n={slide_number}" if slide_number is not None else "",
                src,
                tgt,
                elem_label or None,
            )
            break
        out.append(r)
    return out


def _float_scalar(val: Any) -> float | None:
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val.replace(",", "").replace("$", "").strip())
        except ValueError:
            return None
    return None
