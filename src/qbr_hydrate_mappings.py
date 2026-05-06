"""Explicit QBR hydrate mappings from ``config/qbr_mappings.yaml``.

When ``report[REPORT_KEY_EXPLICIT_QBR_MAPPINGS]`` is true, :func:`adapt_custom_slides` runs a
**mapping-first** path: replacements are built only from YAML rules (no adapt LLM). Phase B still
uses :func:`~hydrate_slide_mutation.apply_adaptations` (page-scoped ``replaceAllText``).

Legacy post-LLM behaviour remains available behind the same flag only for callers that still run the
LLM adapt path; template QBR uses mapping-first exclusively.

Each rule's ``target`` is resolved via :func:`data_field_synonyms.resolve_data_summary_target_path`
(``comprehensive_data_element_list.json`` ``terms``).

**Disk writes are opt-in.** Hydrate **always reads** the YAML when present; it does **not** rewrite the
file unless :func:`qbr_mappings_disk_write_enabled` is true (see ``BPO_QBR_MAPPINGS_WRITE``).

**Bootstrap / auto-append:** When writes are enabled and ``config/qbr_mappings.yaml`` is absent,
:func:`bootstrap_qbr_mappings_from_slides` can walk template slides and create stub rows
(``target: ""``). With writes disabled and no file, add ``config/qbr_mappings.yaml`` manually
(version 2 schema: ``slides`` / ``global_elements``; see :func:`bootstrap_qbr_mappings_from_slides`)
or enable ``BPO_QBR_MAPPINGS_WRITE``.
"""

from __future__ import annotations

import hashlib
import os
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
# Public path for callers (e.g. ``adapt_custom_slides`` checks existence before bootstrap).
QBR_MAPPINGS_DEFAULT_PATH = _DEFAULT_PATH

_LOAD_LOCK = threading.Lock()
_cached: dict[str, Any] | None = None
_cached_mtime: float | None = None

_SYNONYM_TRIGGER_PLACEHOLDERS = frozenset(
    ("[000]", "[$000]", "[00/00/00]", "[00%]", "[???]")
)


def qbr_mappings_disk_write_enabled() -> bool:
    """Return True when bootstrap / merge may write ``config/qbr_mappings.yaml``.

    Default is **false** (read-only): manual edits are never overwritten during QBR.

    * Set ``BPO_QBR_MAPPINGS_WRITE`` to ``1``, ``true``, ``yes``, or ``on`` to allow writes.
    * Set to ``0``, ``false``, ``no``, or ``off`` to disallow (explicit override).
    * If ``BPO_QBR_MAPPINGS_WRITE`` is unset, ``BPO_QBR_MAPPINGS_AUTOWRITE=true`` still enables
      writes (legacy; previously defaulted on in :mod:`evaluate`).
    """
    raw = os.environ.get("BPO_QBR_MAPPINGS_WRITE")
    if raw is not None and str(raw).strip() != "":
        s = str(raw).strip().lower()
        if s in ("1", "true", "yes", "on"):
            return True
        if s in ("0", "false", "no", "off"):
            return False
        return False
    aw = (os.environ.get("BPO_QBR_MAPPINGS_AUTOWRITE") or "").strip().lower()
    return aw in ("1", "true", "yes", "on")


def load_qbr_mappings(*, path: Path | None = None) -> dict[str, Any]:
    """Load ``qbr_mappings.yaml`` (cached by mtime).

    If the file is missing, returns an in-memory empty v2 shape (no disk write). Call
    :func:`bootstrap_qbr_mappings_from_slides` before adapt to create the file from a slide walk.
    """
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
        # Optional legacy lists alongside v2 layout (e.g. transitional configs).
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


def _norm_source_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def existing_mapping_source_keys(cfg: dict[str, Any]) -> set[tuple[int | None, str]]:
    """Keys (slide_number | None, normalized source) already present in config (any target)."""
    keys: set[tuple[int | None, str]] = set()
    for row in expand_mapping_rules(cfg):
        sn = row.get("slide_number")
        if sn is not None:
            try:
                sn = int(sn)
            except (TypeError, ValueError):
                sn = None
        src = _norm_source_key(str(row.get("source") or ""))
        if not src:
            continue
        keys.add((sn, src))
    return keys


def _auto_element_name(slide_number: int, source: str) -> str:
    h = hashlib.sha256(f"{slide_number}:{source}".encode("utf-8")).hexdigest()[:10]
    return f"auto_s{slide_number}_{h}"


def _ensure_slide_block(slides: list[Any], slide_number: int, slide_id: str | None) -> dict[str, Any]:
    for b in slides:
        if isinstance(b, dict) and _coerce_slide_number(b.get("slide_number")) == slide_number:
            if slide_id and not _normalize_slide_id(b.get("slide_id")):
                b["slide_id"] = slide_id
            return b
    block: dict[str, Any] = {"slide_number": slide_number, "elements": []}
    if slide_id:
        block["slide_id"] = slide_id
    slides.append(block)
    return block


# Slide extract types for raster / chart objects — never rows in qbr_mappings.
_VISUAL_ELEMENT_TYPES = frozenset(("image", "chart"))


def mapping_source_is_visual_only(source: str | None, field: str | None = None) -> bool:
    """True if this source is an image/chart pipeline slot, not a text data element for YAML mapping."""
    f = (field or "").strip().lower()
    if f in ("chart", "image"):
        return True
    t = (source or "").strip()
    if not t:
        return True
    if t in ("(embedded image)", "(image in shape)"):
        return True
    if t == "(embedded chart — contains data that cannot be auto-updated)":
        return True
    if t.startswith("[STATIC IMAGE"):
        return True
    if "CHART — data cannot be auto-updated" in t:
        return True
    return False


# Bracketed placeholders: digits, currency, percent, dates, or unknown-slot markers.
_RE_BRACKET_DATA = re.compile(
    r"\[[^\]]*[\d$€£¥%/?][^\]]*\]|\[[^\]]*\d{4}[^\]]*\]",
    re.I,
)
# Slash or ISO dates; month + year style fragments.
_RE_DATE_LIKE = re.compile(
    r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b\d{4}-\d{2}-\d{2}\b|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,4}\b",
    re.I,
)
_RE_PERCENT = re.compile(r"[\d,.\s]+\s*%|%\s*[\d,.\s]+|\[\s*00\s*%\s*\]", re.I)
_RE_CURRENCY = re.compile(r"[\$€£¥]\s*[\d,.]+|[\d,.]+\s*[kmb]\b", re.I)
# Phrase-style sources for synonym rules (not single-word section labels).
_RE_LETTER_WORDS = re.compile(r"\b[^\W\d_]{2,}\b", re.UNICODE)


def mapping_source_is_recognizable_data(source: str | None, field: str | None = None) -> bool:
    """True if ``source`` looks like mappable slide data (bootstrap / merge / autowrite).

    Includes: bracketed numbers or placeholders, dates, percents, currency, the same
    letter/number heuristics as :func:`~hydrate_replacements.element_may_contain_data`,
    and multiword strings (three or more letter-words) for explicit phrase mappings.
    """
    if mapping_source_is_visual_only(source, field):
        return False
    t = (source or "").strip()
    if len(t) <= 2 or len(t) > 2000:
        return False
    if _RE_BRACKET_DATA.search(t) or _RE_DATE_LIKE.search(t):
        return True
    if _RE_PERCENT.search(t) or _RE_CURRENCY.search(t):
        return True
    from .hydrate_replacements import element_may_contain_data as _may_data

    if _may_data({"text": t}):
        return True
    letter_words = _RE_LETTER_WORDS.findall(t)
    if len(letter_words) >= 3:
        return True
    return False


# Each non-empty line of a shape starts with its own metric placeholder (not one blob with prose).
_RE_LINE_LEADING_METRIC = re.compile(
    r"(?is)^\s*(?:\[[^\]\n]{1,120}\]|[a-z]{2,4}\s*%|[$€£]\s*[\d,.]|[\d.,]+\s*%)",
)


def _line_is_own_standalone_metric_placeholder(line: str) -> bool:
    """True when this single line is a template metric row (leading bracket, xx%%, digits%%, or currency)."""
    s = (line or "").strip()
    if len(s) < 4 or len(s) > 220:
        return False
    return bool(_RE_LINE_LEADING_METRIC.match(s))


def _split_multiline_metric_placeholder_lines(text: str) -> list[str]:
    """If every non-empty line is its own metric row, return those lines; else the whole string."""
    t = (text or "").strip()
    if "\n" not in t:
        return [t]
    segments = [s.strip() for s in re.split(r"\n+", t) if s.strip()]
    if len(segments) < 2:
        return [t]
    if all(_line_is_own_standalone_metric_placeholder(s) for s in segments):
        return segments
    return [t]


def expand_qbr_mapping_source_candidates(raw: str, *, field: str | None = None) -> list[str]:
    """Expand one extracted/adapt ``original`` into 1+ YAML sources when one shape holds multiple metric lines."""
    raw = (raw or "").strip()
    if not raw:
        return []
    out: list[str] = []
    for piece in _split_multiline_metric_placeholder_lines(raw):
        p = piece.strip()
        if not p or len(p) > 2000:
            continue
        if mapping_source_is_visual_only(p, field):
            continue
        if not mapping_source_is_recognizable_data(p, field):
            continue
        if not mapping_source_suitable_for_qbr_yaml_autowrite(p):
            continue
        out.append(p)
    return out


def mapping_source_suitable_for_qbr_yaml_autowrite(source: str | None) -> bool:
    """False for long multi-paragraph shapes; YAML ``source`` must match a replace key (e.g. ``XX%``), not whole coach copy.

    Unmapped adapt rows still use verbatim ``original``; when the model leaves an entire instruction
    block unmapped, appending it as ``source`` produces unusable rules. Concise placeholders and
    short phrases still pass.
    """
    t = (source or "").strip()
    if len(t) > 500:
        return False
    if t.count("\n") >= 2 and len(t) > 100:
        return False
    return True


def bootstrap_qbr_mappings_from_slides(
    slides_by_id: dict[str, Any],
    page_ids: list[str],
    ordered_ids: list[str],
    explicit_slide_type_by_page: dict[str, str],
    *,
    path: Path | None = None,
) -> int:
    """If ``path`` (default ``config/qbr_mappings.yaml``) does not exist, walk ``page_ids`` and write rules.

    Uses the same data-element heuristic as adapt (:func:`~hydrate_replacements.element_may_contain_data`).
    Returns number of new elements written, or 0 if the file already exists or nothing matched.
    """
    from .hydrate_extract import extract_slide_text_elements as _extract_te
    from .hydrate_replacements import element_may_contain_data as _element_may_contain_data

    p = path or _DEFAULT_PATH
    if p.exists():
        return 0
    if not qbr_mappings_disk_write_enabled():
        logger.info(
            "qbr_mappings: bootstrap skipped — %s missing and disk writes are disabled "
            "(set BPO_QBR_MAPPINGS_WRITE=1 to auto-create, or add config/qbr_mappings.yaml manually)",
            p,
        )
        return 0
    discoveries: list[dict[str, Any]] = []
    seen: set[tuple[int, str]] = set()
    for page_id in page_ids:
        slide = slides_by_id.get(page_id)
        if not slide or page_id not in ordered_ids:
            continue
        sn = ordered_ids.index(page_id) + 1
        sid_raw = (explicit_slide_type_by_page.get(page_id) or "").strip() or None
        for el in _extract_te(slide.get("pageElements") or []):
            if el.get("type") in _VISUAL_ELEMENT_TYPES:
                continue
            if not _element_may_contain_data(el):
                continue
            raw = str(el.get("text") or "").strip()
            for piece in expand_qbr_mapping_source_candidates(raw):
                key = (int(sn), _norm_source_key(piece))
                if key in seen:
                    continue
                seen.add(key)
                discoveries.append({"slide_number": int(sn), "slide_id": sid_raw, "source": piece})
    if not discoveries:
        return 0
    n = merge_discovered_sources_into_qbr_mappings(discoveries, path=p)
    if n:
        logger.info(
            "qbr_mappings: bootstrap from slide walk wrote %d element(s) to %s (fill targets and re-run)",
            n,
            p,
        )
    return n


def invalidate_qbr_mappings_cache() -> None:
    """Force reload of ``qbr_mappings.yaml`` on next access (call after disk merge)."""
    global _cached, _cached_mtime
    with _LOAD_LOCK:
        _cached = None
        _cached_mtime = None


def merge_discovered_sources_into_qbr_mappings(
    discoveries: list[dict[str, Any]],
    *,
    path: Path | None = None,
) -> int:
    """Append ``slides[].elements`` rows (``target: ""``) for unmapped sources not already in the file.

    Each discovery dict: ``slide_number`` (int), ``slide_id`` (optional str), ``source`` (str, verbatim).
    Deduplicates by ``(slide_number, normalized source)``. Returns count of new elements appended.
    """
    p = path or _DEFAULT_PATH
    if not qbr_mappings_disk_write_enabled():
        logger.info(
            "qbr_mappings: skip merge — disk writes disabled (set BPO_QBR_MAPPINGS_WRITE=1 to append "
            "discovered sources to %s)",
            p,
        )
        return 0
    seen: set[tuple[int, str]] = set()
    uniq: list[dict[str, Any]] = []
    for d in discoveries:
        sn = d.get("slide_number")
        raw = str(d.get("source") or "").strip()
        if not isinstance(sn, int) or sn < 1 or not raw or len(raw) > 2000:
            continue
        pieces = expand_qbr_mapping_source_candidates(raw, field=d.get("field"))
        if not pieces:
            continue
        for piece in pieces:
            if len(piece) > 2000:
                continue
            key = (sn, _norm_source_key(piece))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(
                {
                    "slide_number": sn,
                    "slide_id": _normalize_slide_id(d.get("slide_id")),
                    "source": piece,
                }
            )
    if not uniq:
        return 0

    try:
        txt = p.read_text(encoding="utf-8")
        cfg = yaml.safe_load(txt)
    except (OSError, yaml.YAMLError):
        cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}

    keys = existing_mapping_source_keys(cfg)
    slides = cfg.setdefault("slides", [])
    if not isinstance(slides, list):
        cfg["slides"] = []
        slides = cfg["slides"]
    cfg.setdefault("global_elements", [])
    if not isinstance(cfg["global_elements"], list):
        cfg["global_elements"] = []

    added = 0
    for d in uniq:
        sn = d["slide_number"]
        raw = d["source"]
        k = (sn, _norm_source_key(raw))
        if k in keys:
            continue
        keys.add(k)
        sid = d.get("slide_id")
        block = _ensure_slide_block(slides, sn, sid)
        els = block.setdefault("elements", [])
        if not isinstance(els, list):
            block["elements"] = []
            els = block["elements"]
        els.append(
            {
                "name": _auto_element_name(sn, raw),
                "source": raw,
                "target": "",
            }
        )
        added += 1

    if not added:
        return 0

    slides.sort(
        key=lambda x: _coerce_slide_number(x.get("slide_number")) if isinstance(x, dict) else 10**9
    )

    try:
        ver = int(cfg.get("version", 2))
    except (TypeError, ValueError):
        ver = 2
    cfg["version"] = max(ver, 2)

    p.parent.mkdir(parents=True, exist_ok=True)
    out = yaml.dump(
        cfg,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )
    p.write_text(out, encoding="utf-8")
    logger.info("qbr_mappings: wrote %d new element(s) with empty target to %s", added, p)
    return added


def _normalize_context(s: str) -> str:
    t = (s or "").replace("\u00a0", " ").lower().strip()
    return re.sub(r"\s+", " ", t)


def _slide_matches_explicit_qbr_rule(
    ent: dict[str, Any],
    *,
    slide_number: int | None,
    slide_type: str | None,
) -> bool:
    """Same slide_number / slide_id gating as :func:`apply_explicit_qbr_mappings`."""
    rule_sn = ent.get("slide_number")
    if rule_sn is not None:
        if slide_number is None:
            return False
        if int(rule_sn) != int(slide_number):
            return False
    st_filter = (slide_type or "").strip()
    sid_raw = ent.get("slide_id")
    if sid_raw is not None and str(sid_raw).strip() not in ("", "null"):
        if st_filter and str(sid_raw).strip() != st_filter:
            return False
    return True


def _slide_has_explicit_qbr_source(
    src: str,
    *,
    is_bracket: bool,
    text_elements: list[dict],
) -> bool:
    """True when slide text contains ``src`` per bracket vs phrase rules (aligned with explicit mapping)."""
    from .data_field_synonyms import _narrow_synonym_haystack

    src = (src or "").strip()
    if not src:
        return False
    if is_bracket:
        for el in text_elements:
            full = el.get("text") or ""
            if full.strip() == src:
                return True
            for line in full.splitlines():
                if line.strip() == src:
                    return True
        return False
    hay = _narrow_synonym_haystack(src, text_elements)
    h = _normalize_context(hay)
    if len(h) < 4:
        return False
    return _normalize_context(src) in h


def _format_explicit_qbr_new_value(
    orig: str,
    raw: Any,
    path_resolved: str,
    text_elements: list[dict],
) -> str:
    """Format scalar for slide with suffix and percent semantics (shared explicit-mapping logic)."""
    from .data_field_synonyms import _format_scalar_for_slide
    from .evaluate import (
        _adapt_original_reads_as_percent_on_slide,
        _adapt_text_has_percentage_semantics,
    )

    raw_s = _format_scalar_for_slide(raw, path=path_resolved)
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
    return f"{raw_s} {suffix}".strip() if suffix else raw_s


def build_mapping_first_qbr_replacements(
    text_elements: list[dict],
    data_summary: dict[str, Any],
    *,
    slide_type: str | None,
    slide_ref: str = "",
    slide_number: int | None,
) -> list[dict]:
    """Build adapt replacement rows from ``qbr_mappings.yaml`` only (no LLM).

    Warns when a rule's ``source`` is absent from slide text or the resolved value cannot be used.
    When multiple rules share the same ``source`` string, the **last** matching rule in flattened
    YAML order wins (one ``replaceAllText`` per distinct ``original``).
    """
    from .data_field_synonyms import (
        _value_present,
        data_summary_lookup,
        resolve_data_summary_target_path,
    )
    from .evaluate import (
        _adapt_original_reads_as_percent_on_slide,
        _adapt_text_has_percentage_semantics,
    )

    cfg = load_qbr_mappings()
    rows = expand_mapping_rules(cfg)
    out_by_src: dict[str, dict[str, Any]] = {}
    for ent in rows:
        if not _slide_matches_explicit_qbr_rule(
            ent, slide_number=slide_number, slide_type=slide_type
        ):
            continue
        src = str(ent.get("source") or "").strip()
        tgt = str(ent.get("target") or "").strip()
        if not src or not tgt:
            continue
        is_bracket = src.startswith("[") and src.endswith("]")
        if not _slide_has_explicit_qbr_source(src, is_bracket=is_bracket, text_elements=text_elements):
            logger.warning(
                "qbr_mappings: slide %s — mapping-first: source not found on slide: %r (target=%r)",
                slide_ref,
                src[:200],
                tgt[:120],
            )
            continue
        path_resolved = resolve_data_summary_target_path(tgt)
        raw = data_summary_lookup(data_summary, path_resolved)
        if not _value_present(raw):
            logger.warning(
                "qbr_mappings: slide %s — mapping-first: no data for target %r → %s",
                slide_ref,
                tgt,
                path_resolved,
            )
            continue
        if isinstance(raw, (dict, list)):
            logger.warning(
                "qbr_mappings: slide %s — mapping-first: target %r resolves to non-scalar (%s)",
                slide_ref,
                tgt,
                path_resolved,
            )
            continue
        fv = _float_scalar(raw)
        if fv is not None and abs(fv) > 150:
            if _adapt_text_has_percentage_semantics(src) or _adapt_original_reads_as_percent_on_slide(
                src, text_elements
            ):
                logger.warning(
                    "qbr_mappings: slide %s — mapping-first: skipped large scalar for percent-like slot "
                    "(source=%r path=%s)",
                    slide_ref,
                    src[:120],
                    path_resolved,
                )
                continue

        new_val = _format_explicit_qbr_new_value(src, raw, path_resolved, text_elements)
        elem_label = str(ent.get("data_element_name") or "").strip()
        row: dict[str, Any] = {
            "original": src,
            "new_value": new_val,
            "mapped": True,
            "field": path_resolved,
            "synonym_phrase": src,
            "synonym_path": path_resolved,
        }
        if elem_label:
            row["qbr_mapping_element"] = elem_label
        if src in out_by_src:
            logger.warning(
                "qbr_mappings: slide %s — mapping-first: duplicate source %r; later rule wins",
                slide_ref,
                src[:120],
            )
        out_by_src[src] = row
        logger.debug(
            "qbr_mappings: slide %s mapping-first applied %r → %s",
            slide_ref,
            src,
            path_resolved,
        )
    return list(out_by_src.values())


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
        _narrow_synonym_haystack,
        _value_present,
        data_summary_lookup,
        resolve_data_summary_target_path,
    )
    from .evaluate import (
        _adapt_original_reads_as_percent_on_slide,
        _adapt_text_has_percentage_semantics,
    )

    cfg = load_qbr_mappings()
    rows = expand_mapping_rules(cfg)
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
            if not _slide_matches_explicit_qbr_rule(
                ent, slide_number=slide_number, slide_type=slide_type
            ):
                continue
            src = str(ent.get("source") or "").strip()
            tgt = str(ent.get("target") or "").strip()
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
            path_resolved = resolve_data_summary_target_path(tgt)
            raw = data_summary_lookup(data_summary, path_resolved)
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

            new_val = _format_explicit_qbr_new_value(orig, raw, path_resolved, text_elements)

            r["mapped"] = True
            r["field"] = path_resolved
            r["new_value"] = new_val
            r["synonym_phrase"] = src
            r["synonym_path"] = path_resolved
            elem_label = ent.get("data_element_name") or ""
            if elem_label:
                r["qbr_mapping_element"] = elem_label
            applied = True
            logger.debug(
                "qbr_mappings: slide %s%s applied %r → %s (element=%r)",
                slide_ref,
                f" n={slide_number}" if slide_number is not None else "",
                src,
                path_resolved,
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
