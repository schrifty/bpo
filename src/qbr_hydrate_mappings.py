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
        return {"version": 1, "mappings": [], "bracket_placeholder_sources": []}
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
        return raw


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
    rows: list[dict[str, Any]] = []
    for ent in cfg.get("mappings") or []:
        if isinstance(ent, dict):
            rows.append(ent)
    for ent in cfg.get("bracket_placeholder_sources") or []:
        if isinstance(ent, dict):
            rows.append(ent)

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
            slide_id = ent.get("slide_id")
            if not src or not tgt:
                continue
            sid_raw = slide_id
            if sid_raw is not None and str(sid_raw).strip() not in ("", "null"):
                if st_filter and str(sid_raw).strip() != st_filter:
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
            applied = True
            logger.debug(
                "qbr_mappings: slide %s applied %r → %s",
                slide_ref,
                src,
                tgt,
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
