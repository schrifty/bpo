"""Curated phrase → data_summary path mapping for QBR hydrate synonym resolution.

When the portfolio snapshot Drive folder is configured, ``data_field_synonyms.json`` is
synced there from the repo on first use (repo wins), then loaded from Drive with local
fallback. Otherwise only the repo file is used.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG = _REPO_ROOT / "config" / "data_field_synonyms.json"

_WS_RE = re.compile(r"\s+")

# Keep in sync with evaluate._PLACEHOLDER_MARKERS (avoid import cycle).
_SYNONYM_TRIGGER_PLACEHOLDERS = frozenset(
    ("[000]", "[$000]", "[00/00/00]", "[00%]", "[???]")
)

_cache_rows: list[tuple[int, str, str, str]] | None = None
_cache_key: object | None = None
_default_synonym_load_lock = threading.Lock()


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
        phrases = ent.get("phrases") or []
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


def _load_synonym_rows(config_path: Path | None = None) -> list[tuple[int, str, str, str]]:
    """Rows sorted for scan: (unused, normalized_phrase, path, canonical_label_for_notes)."""
    global _cache_rows, _cache_key
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


def apply_synonym_resolution_to_replacements(
    replacements: list[dict],
    text_elements: list[dict],
    data_summary: dict[str, Any],
    *,
    config_path: Path | None = None,
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
        hay_parts: list[str] = []
        for el in text_elements:
            t = el.get("text") or ""
            if orig and orig in t:
                hay_parts.append(t)
        haystack = "\n".join(hay_parts) if hay_parts else orig
        hit = try_resolve_phrase_in_text(haystack, data_summary, config_path=config_path)
        if not hit:
            out.append(r)
            continue
        matched_phrase, path, _display_field, raw_val = hit
        raw_s = _format_scalar_for_slide(raw_val, path=path)
        m = _re.match(r"^[\d.,\s$€£%]+", orig)
        suffix = (orig[m.end():].strip() if m else "").strip()
        new_value = f"{raw_s} {suffix}".strip() if suffix else raw_s
        r["mapped"] = True
        r["field"] = path
        r["new_value"] = new_value
        r["synonym_phrase"] = matched_phrase
        r["synonym_path"] = path
        out.append(r)
    return out
