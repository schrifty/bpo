"""Curated phrase → data_summary path mapping for hydrate synonym resolution.

Template QBR (``qbr_template`` → ``adapt_custom_slides`` with explicit flag) uses
``config/qbr_mappings.yaml`` via ``qbr_hydrate_mappings`` instead of scanning slide
copy for **phrase** matches — but ``qbr_mappings`` **target** strings still resolve
through :func:`resolve_data_summary_target_path`, which reads
``config/comprehensive_data_element_list.json`` (``entries[].path`` + ``entries[].terms``
or legacy ``phrases``). Later catalog rows win on duplicate normalized phrases.
Portfolio snapshot JSON caches on Drive are unrelated.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import threading
from pathlib import Path
from typing import Any

from . import matching_log
from .config import logger

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG = _REPO_ROOT / "config" / "comprehensive_data_element_list.json"

_WS_RE = re.compile(r"\s+")

# Keep in sync with evaluate._PLACEHOLDER_MARKERS (avoid import cycle).
_SYNONYM_TRIGGER_PLACEHOLDERS = frozenset(
    ("[000]", "[$000]", "[00/00/00]", "[00%]", "[???]")
)

_cache_rows: list[tuple[int, str, str, str]] | None = None
_cache_key: object | None = None
_default_synonym_load_lock = threading.Lock()

# QBR-only overlay: repo ``comprehensive_data_element_list.json`` + champion-derived synonym rows.
_qbr_catalog_session_lock = threading.Lock()
_qbr_catalog_synonym_rows: list[tuple[int, str, str, str]] | None = None
_qbr_catalog_alias_map: dict[str, str] | None = None

_target_path_alias_map: dict[str, str] | None = None
_target_path_alias_ck: tuple[str, str] | None = None
_target_path_alias_lock = threading.Lock()


def _clip_for_log(s: str, n: int = 400) -> str:
    t = (s or "").replace("\n", " ")
    return t if len(t) <= n else t[: n - 1] + "…"


def _normalize_context(s: str) -> str:
    t = (s or "").replace("\u00a0", " ").lower().strip()
    t = _WS_RE.sub(" ", t)
    return t


def _rows_from_synonyms_data(data: dict[str, Any]) -> list[tuple[int, str, str, str]]:
    rows: list[tuple[int, str, str, str]] = []
    for ent in data.get("entries") or []:
        if not isinstance(ent, dict):
            continue
        path_str = str(ent.get("path") or "").strip().replace(" ", "_").replace("-", "_").lower()
        if not path_str:
            continue
        phrases = ent.get("phrases") or ent.get("terms") or []
        if isinstance(phrases, str):
            phrases = [phrases]
        canonical = ""
        for ph in phrases:
            if not isinstance(ph, str):
                continue
            raw = ph.strip()
            if len(raw) < 4:
                continue
            norm = _normalize_context(raw)
            if len(norm) < 4:
                continue
            if not canonical:
                canonical = raw
            rows.append((-len(norm), norm, path_str, raw))
    rows.sort(key=lambda x: (x[0], x[1]))
    return rows


def _champion_synonym_catalog_entries(champions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Synthetic catalog rows so phrase resolution / target aliases can bind champion fields."""
    out: list[dict[str, Any]] = []
    if not champions:
        return out
    for i, ch in enumerate(champions):
        if not isinstance(ch, dict):
            continue
        n = i + 1
        base = [
            f"Pendo champion {n}",
            f"champion {n}",
            f"pendo champion #{n}",
        ]
        email = str(ch.get("email") or "").strip()
        role = str(ch.get("role") or "").strip()
        lang = str(ch.get("language") or "").strip()
        lv = str(ch.get("last_visit") or "").strip()
        terms_email = [*base, f"Pendo champion {n} email", f"champion {n} email"]
        if len(email) >= 4:
            terms_email.append(email)
        out.append({"path": f"pendo_champion_{n}.email", "terms": terms_email})
        terms_role = [*base, f"Pendo champion {n} role", f"champion {n} role"]
        if len(role) >= 4:
            terms_role.append(role)
        out.append({"path": f"pendo_champion_{n}.role", "terms": terms_role})
        terms_lang = [*base, f"Pendo champion {n} language", f"champion {n} language"]
        if len(lang) >= 4:
            terms_lang.append(lang)
        out.append({"path": f"pendo_champion_{n}.language", "terms": terms_lang})
        terms_lv = [*base, f"Pendo champion {n} last visit", f"champion {n} last visit"]
        if len(lv) >= 4:
            terms_lv.append(lv)
        out.append({"path": f"pendo_champion_{n}.last_visit", "terms": terms_lv})
        out.append({
            "path": f"pendo_champion_{n}.days_inactive",
            "terms": [
                *base,
                f"Pendo champion {n} days inactive",
                f"champion {n} days inactive",
            ],
        })
    return out


def begin_qbr_comprehensive_catalog_session(champions: list[dict[str, Any]] | None) -> None:
    """Load ``comprehensive_data_element_list.json`` from disk into memory and merge champion synonyms.

    Active for :func:`_load_synonym_rows` (``config_path is None``) and
    :func:`resolve_data_summary_target_path` until :func:`end_qbr_comprehensive_catalog_session`
    (typically the whole ``run_qbr_from_template`` tail: adapt, agenda refinement).
    Uses the repo JSON only (not Drive) for this overlay.
    """
    global _qbr_catalog_synonym_rows, _qbr_catalog_alias_map
    try:
        raw_txt = _DEFAULT_CONFIG.read_text(encoding="utf-8")
        data = json.loads(raw_txt)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(
            "hydrate extra | qbr catalog | failed to load %s: %s — using empty base entries",
            _DEFAULT_CONFIG,
            e,
        )
        data = {}
    if not isinstance(data, dict):
        data = {}
    data = copy.deepcopy(data)
    base_entries = list(data.get("entries") or [])
    champ_entries = _champion_synonym_catalog_entries(champions)
    data["entries"] = base_entries + champ_entries
    rows = _rows_from_synonyms_data(data)
    alias = _alias_map_from_synonym_like_entries(data.get("entries") or [], min_phrase_len=4)
    with _qbr_catalog_session_lock:
        _qbr_catalog_synonym_rows = rows
        _qbr_catalog_alias_map = alias
    n_ch = len(champions) if isinstance(champions, list) else 0
    logger.info(
        "hydrate extra | qbr catalog | loaded %s base_entries=%d champion_synonym_entries=%d "
        "synonym_rows=%d alias_keys=%d champions=%d",
        _DEFAULT_CONFIG.name,
        len(base_entries),
        len(champ_entries),
        len(rows),
        len(alias),
        n_ch,
    )


def end_qbr_comprehensive_catalog_session() -> None:
    """Clear QBR comprehensive-catalog overlay (call from ``finally`` at end of QBR run)."""
    global _qbr_catalog_synonym_rows, _qbr_catalog_alias_map
    had_active = False
    with _qbr_catalog_session_lock:
        had_active = (
            _qbr_catalog_synonym_rows is not None or _qbr_catalog_alias_map is not None
        )
        _qbr_catalog_synonym_rows = None
        _qbr_catalog_alias_map = None
    if had_active:
        logger.info("hydrate extra | qbr catalog | session ended")


def _load_synonym_rows(config_path: Path | None = None) -> list[tuple[int, str, str, str]]:
    """Rows sorted for scan: (unused, normalized_phrase, path, canonical_label_for_notes)."""
    global _cache_rows, _cache_key
    if config_path is None:
        with _qbr_catalog_session_lock:
            sess_rows = _qbr_catalog_synonym_rows
        if sess_rows is not None:
            return sess_rows
    if config_path is not None:
        path = config_path
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return []
        ck: object = (str(path), mtime)
        if _cache_rows is not None and _cache_key == ck:
            return _cache_rows
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            _cache_rows, _cache_key = [], ck
            return _cache_rows
        if not isinstance(data, dict):
            _cache_rows, _cache_key = [], ck
            return _cache_rows
        rows = _rows_from_synonyms_data(data)
        _cache_rows = rows
        _cache_key = ck
        return rows

    from .pendo_portfolio_snapshot_drive import load_data_field_synonyms_document

    # Serialize Drive + parse: parallel hydrate threads used to hammer Drive concurrently and
    # trigger flaky TLS (SSL record layer) and possible native heap issues in OpenSSL.
    if (
        _cache_rows is not None
        and isinstance(_cache_key, tuple)
        and len(_cache_key) == 3
        and _cache_key[0] == "synonyms"
    ):
        return _cache_rows

    with _default_synonym_load_lock:
        if (
            _cache_rows is not None
            and isinstance(_cache_key, tuple)
            and len(_cache_key) == 3
            and _cache_key[0] == "synonyms"
        ):
            return _cache_rows
        data, source = load_data_field_synonyms_document(allow_drive=True)
        if not isinstance(data, dict):
            data = {}
        h = hashlib.sha256(
            json.dumps(data, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:32]
        ck: object = ("synonyms", source, h)
        rows = _rows_from_synonyms_data(data)
        _cache_rows = rows
        _cache_key = ck
        return rows


def data_summary_lookup(data_summary: dict[str, Any], path: str) -> Any:
    """Walk ``data_summary`` by dotted path (segments lower snake_case)."""
    if not data_summary or not path:
        return None
    path = path.strip().replace(" ", "_").replace("-", "_").lower()
    parts = path.split(".")
    cur: Any = data_summary
    for p in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def data_summary_path_exists(data_summary: dict[str, Any], path: str) -> bool:
    """True if ``path`` resolves through dict keys (terminal value may be None)."""
    if not data_summary or not path:
        return False
    path = path.strip().replace(" ", "_").replace("-", "_").lower()
    parts = path.split(".")
    cur: Any = data_summary
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            return False
        cur = cur[p]
    return True


def _normalize_target_path_key(s: str) -> str:
    """Same segment normalization as :func:`data_summary_lookup` applies to the full path string."""
    return s.strip().replace(" ", "_").replace("-", "_").lower()


def _alias_map_from_synonym_like_entries(
    entries: Any,
    *,
    min_phrase_len: int,
) -> dict[str, str]:
    """``phrase``/``term`` (normalized) → canonical dotted path key (normalized)."""
    out: dict[str, str] = {}
    if not isinstance(entries, list):
        return out
    for ent in entries:
        if not isinstance(ent, dict):
            continue
        path_raw = str(ent.get("path") or "").strip()
        if not path_raw:
            continue
        path_key = _normalize_target_path_key(path_raw)
        phrases = ent.get("phrases")
        if phrases is None:
            phrases = ent.get("terms") or []
        if isinstance(phrases, str):
            phrases = [phrases]
        out[path_key] = path_key
        for ph in phrases:
            if not isinstance(ph, str):
                continue
            raw = ph.strip()
            if len(raw) < min_phrase_len:
                continue
            k = _normalize_target_path_key(raw)
            if k:
                out[k] = path_key
    return out


def _target_path_alias_signature() -> str:
    from .pendo_portfolio_snapshot_drive import local_data_field_synonyms_path

    p = local_data_field_synonyms_path()
    try:
        return str(p.stat().st_mtime_ns)
    except OSError:
        return "0"


def _build_target_path_alias_map() -> dict[str, str]:
    from .pendo_portfolio_snapshot_drive import load_data_field_synonyms_document

    catalog, _ = load_data_field_synonyms_document(allow_drive=True)
    if not isinstance(catalog, dict):
        return {}
    return _alias_map_from_synonym_like_entries(
        catalog.get("entries"),
        min_phrase_len=4,
    )


def invalidate_target_path_alias_cache() -> None:
    """Drop cached target-alias map (tests or after editing JSON)."""
    global _target_path_alias_map, _target_path_alias_ck
    with _target_path_alias_lock:
        _target_path_alias_map = None
        _target_path_alias_ck = None


def resolve_data_summary_target_path(target: str) -> str:
    """Turn a ``qbr_mappings`` / human **target** string into a dotted ``data_summary`` path.

    Keys are normalized like :func:`data_summary_lookup` (spaces and hyphens → ``_``,
    lowercased). Built from ``entries[].path`` + ``entries[].terms`` (or legacy
    ``phrases``) in ``config/comprehensive_data_element_list.json``; strings shorter than
    4 characters
    are skipped. **Later** rows in ``entries`` win when two rows share the same
    normalized phrase.

    If nothing matches, returns ``target`` stripped (existing direct-path behavior).
    """
    t = (target or "").strip()
    if not t:
        return target or ""
    with _qbr_catalog_session_lock:
        sess_alias = _qbr_catalog_alias_map
    if sess_alias is not None:
        key = _normalize_target_path_key(t)
        return sess_alias.get(key, t)
    global _target_path_alias_map, _target_path_alias_ck
    sig = _target_path_alias_signature()
    with _target_path_alias_lock:
        if _target_path_alias_map is None or _target_path_alias_ck != sig:
            _target_path_alias_map = _build_target_path_alias_map()
            _target_path_alias_ck = sig
        m = _target_path_alias_map
    key = _normalize_target_path_key(t)
    return m.get(key, t)


def _value_present(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, (list, dict)) and len(val) == 0:
        return False
    if val == "":
        return False
    return True


def _format_scalar_for_slide(val: Any, *, path: str) -> str:
    del path  # reserved for currency heuristics
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, int):
        return f"{val:,}"
    if isinstance(val, float):
        if abs(val - round(val)) < 1e-6:
            return f"{int(round(val)):,}"
        return str(round(val, 1))
    return str(val)


def _narrow_synonym_haystack(orig: str, text_elements: list[dict]) -> str:
    """Use only lines (or a short window) around ``orig`` so unrelated copy in the same shape
    cannot trigger phrase matches for a different placeholder (e.g. ``[4 BU]``,
    ``[8 Differents ERP]`` vs ``weekly on leandna`` on another line).
    """
    if not orig:
        return ""
    parts: list[str] = []
    for el in text_elements:
        t = el.get("text") or ""
        if orig not in t:
            continue
        lines = t.splitlines()
        hit_lines = [ln for ln in lines if orig in ln]
        if hit_lines:
            parts.append("\n".join(hit_lines))
            continue
        # Rare: orig spans lines — fall back to a window around the first occurrence
        i = t.index(orig)
        lo = max(0, i - 160)
        hi = min(len(t), i + len(orig) + 160)
        parts.append(t[lo:hi])
    return "\n".join(parts) if parts else orig


def try_resolve_phrase_in_text(
    haystack: str,
    data_summary: dict[str, Any],
    *,
    config_path: Path | None = None,
) -> tuple[str, str, str, Any] | None:
    """Return ``(matched_phrase, path, display_field, raw_value)`` or None."""
    if not haystack or not data_summary:
        return None
    h = _normalize_context(haystack)
    if len(h) < 4:
        return None
    for _neg_len, norm_phrase, path, canonical_label in _load_synonym_rows(config_path):
        if norm_phrase not in h:
            continue
        raw = data_summary_lookup(data_summary, path)
        if not _value_present(raw):
            continue
        if isinstance(raw, (dict, list)):
            continue
        return (canonical_label, path, path, raw)
    return None


def synonym_scan_diagnostics(
    haystack: str,
    data_summary: dict[str, Any],
    *,
    config_path: Path | None = None,
) -> dict[str, Any]:
    """Explain why :func:`try_resolve_phrase_in_text` may return None: phrase overlap vs empty values.

    For every synonym config row whose phrase appears in the normalized haystack, records path and
    whether a scalar value was available (non-falsy, not dict/list).
    """
    h = _normalize_context(haystack)
    if len(h) < 4:
        return {
            "haystack_ok": False,
            "reason": "haystack_too_short_after_normalization",
            "normalized_len": len(h),
            "candidates": [],
        }
    rows = _load_synonym_rows(config_path)
    n_scanned = 0
    in_text: list[dict[str, Any]] = []
    for _neg, norm_phrase, path, raw_label in rows:
        n_scanned += 1
        if norm_phrase not in h:
            continue
        raw = data_summary_lookup(data_summary, path)
        if not _value_present(raw):
            in_text.append(
                {
                    "phrase": raw_label,
                    "path": path,
                    "outcome": "value_empty_or_missing",
                }
            )
        elif isinstance(raw, (dict, list)):
            in_text.append(
                {
                    "phrase": raw_label,
                    "path": path,
                    "outcome": "value_not_scalar_skipped",
                }
            )
        else:
            in_text.append(
                {
                    "phrase": raw_label,
                    "path": path,
                    "outcome": "would_resolve",
                }
            )
    return {
        "haystack_ok": True,
        "normalized_haystack_len": len(h),
        "config_rows_scanned": n_scanned,
        "phrases_matched_in_haystack": len(in_text),
        "candidates": in_text,
    }


def apply_synonym_resolution_to_replacements(
    replacements: list[dict],
    text_elements: list[dict],
    data_summary: dict[str, Any],
    *,
    config_path: Path | None = None,
    slide_ref: str = "",
) -> list[dict]:
    """Fill unmapped / generic-placeholder rows when slide context matches a synonym phrase."""
    import re as _re

    out: list[dict] = []
    for r in replacements:
        r = dict(r)
        fld = (r.get("field") or "").strip().lower()
        if fld in ("chart", "image"):
            out.append(r)
            continue
        mapped = bool(r.get("mapped", True))
        nv = str(r.get("new_value") or "").strip()
        try_synonym = (not mapped) or (nv in _SYNONYM_TRIGGER_PLACEHOLDERS)
        if not try_synonym:
            out.append(r)
            continue
        orig = str(r.get("original") or "")
        haystack = _narrow_synonym_haystack(orig, text_elements)
        if matching_log.enabled():
            _trig: list[str] = []
            if not mapped:
                _trig.append("unmapped")
            if nv in _SYNONYM_TRIGGER_PLACEHOLDERS:
                _trig.append("placeholder_token")
            matching_log.emit(
                "synonym_attempt",
                slide_ref=slide_ref or "",
                triggers=_trig,
                original=_clip_for_log(orig),
                field_before=fld,
                mapped_before=mapped,
                new_value_before=_clip_for_log(nv),
                haystack_narrowed=_clip_for_log(haystack, 500),
            )
        hit = try_resolve_phrase_in_text(haystack, data_summary, config_path=config_path)
        if not hit:
            if matching_log.enabled():
                diag = synonym_scan_diagnostics(haystack, data_summary, config_path=config_path)
                matching_log.emit(
                    "synonym_no_match",
                    slide_ref=slide_ref or "",
                    original=_clip_for_log(orig),
                    field=fld,
                    **diag,
                )
            out.append(r)
            continue
        matched_phrase, path, _display_field, raw_val = hit
        # Lazy import: evaluate imports this module at load time.
        from .evaluate import (
            _adapt_original_reads_as_percent_on_slide,
            _adapt_text_has_percentage_semantics,
        )

        def _float_scalar(val: Any) -> float | None:
            if isinstance(val, (int, float)):
                return float(val)
            if isinstance(val, str):
                try:
                    return float(val.replace(",", "").replace("$", "").strip())
                except ValueError:
                    return None
            return None

        fv = _float_scalar(raw_val)
        if fv is not None and abs(fv) > 150:
            if _adapt_text_has_percentage_semantics(orig) or _adapt_original_reads_as_percent_on_slide(
                orig, text_elements
            ):
                if matching_log.enabled():
                    matching_log.emit(
                        "synonym_skipped_implausible_magnitude",
                        slide_ref=slide_ref or "",
                        original=_clip_for_log(orig),
                        path=path,
                        raw_value_sample=_clip_for_log(str(raw_val), 80),
                        reason="value_abs>150_in_percent_context_left_unresolved",
                    )
                out.append(r)
                continue

        raw_s = _format_scalar_for_slide(raw_val, path=path)
        m = _re.match(r"^[\d.,\s$€£%]+", orig)
        suffix = (orig[m.end():].strip() if m else "").strip()
        pct_in_prefix = bool(m and "%" in m.group())
        percent_slot = (
            pct_in_prefix
            or _adapt_text_has_percentage_semantics(orig)
            or _adapt_original_reads_as_percent_on_slide(orig, text_elements)
        )
        if percent_slot:
            if suffix.startswith("%"):
                suffix = suffix[1:].strip()
            new_value = f"{raw_s}% {suffix}".strip() if suffix else f"{raw_s}%"
        else:
            new_value = f"{raw_s} {suffix}".strip() if suffix else raw_s
        r["mapped"] = True
        r["field"] = path
        r["new_value"] = new_value
        r["synonym_phrase"] = matched_phrase
        r["synonym_path"] = path
        if matching_log.enabled():
            matching_log.emit(
                "synonym_resolved",
                slide_ref=slide_ref or "",
                original=_clip_for_log(orig),
                matched_phrase=matched_phrase,
                path=path,
                new_value=_clip_for_log(new_value, 200),
            )
        out.append(r)
    return out
