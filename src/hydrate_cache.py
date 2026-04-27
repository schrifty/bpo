"""Persistent cache helpers for evaluate and hydrate slide analysis."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any


SLIDE_CACHE_VERSION = 2  # v2: classify title/cover/divider for hydrate skip
SLIDE_ANALYSIS_CACHE_VERSION = 7  # v7: charts[].interpretation + visual_kind + explicit pipeline gaps for visuals


def default_slide_cache_dir() -> Path:
    root = Path(__file__).resolve().parent.parent
    return root / ".slide_cache"


def slide_content_hash(thumb_b64: str | None, text_snapshot: str = "", page_id: str = "") -> str | None:
    """Stable slide cache key from thumbnail bytes or fallback text."""
    prefix = (page_id or "").encode("utf-8")
    if thumb_b64:
        raw = base64.b64decode(thumb_b64, validate=True)
        return hashlib.sha256(prefix + raw).hexdigest()
    if text_snapshot:
        return hashlib.sha256(prefix + text_snapshot.encode("utf-8")).hexdigest()
    return None


def data_summary_fingerprint(data_summary: dict[str, Any]) -> str:
    """Stable hash of the full data summary so adapt cache invalidates when report data changes."""
    canonical = json.dumps(data_summary, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def adapt_cache_key(thumb_b64: str | None, page_id: str, data_summary: dict[str, Any]) -> str | None:
    """Cache key for adapt replacements: slide pixels plus current data fingerprint."""
    base = slide_content_hash(thumb_b64, page_id=page_id)
    if not base:
        return None
    fp = data_summary_fingerprint(data_summary)
    return hashlib.sha256(f"{base}:{fp}".encode("utf-8")).hexdigest()


def get_cached_classification(cache_dir: Path, cache_key: str) -> dict[str, Any] | None:
    path = cache_dir / "classification" / f"{cache_key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("_version") != SLIDE_CACHE_VERSION:
            return None
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception:
        return None


def set_cached_classification(cache_dir: Path, cache_key: str, result: dict[str, Any]) -> None:
    out_dir = cache_dir / "classification"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = {"_version": SLIDE_CACHE_VERSION, **result}
    (out_dir / f"{cache_key}.json").write_text(json.dumps(out, indent=0), encoding="utf-8")


def get_cached_adapt(cache_dir: Path, cache_key: str) -> list[dict[str, Any]] | None:
    path = cache_dir / "adapt" / f"{cache_key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("_version") != SLIDE_CACHE_VERSION:
            return None
        return data.get("replacements", [])
    except Exception:
        return None


def set_cached_adapt(cache_dir: Path, cache_key: str, replacements: list[dict[str, Any]]) -> None:
    out_dir = cache_dir / "adapt"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = {"_version": SLIDE_CACHE_VERSION, "replacements": replacements}
    (out_dir / f"{cache_key}.json").write_text(json.dumps(out, indent=0, default=str), encoding="utf-8")


def get_cached_slide_analysis(cache_dir: Path, cache_key: str) -> dict[str, Any] | None:
    path = cache_dir / "analysis" / f"{cache_key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("_version") != SLIDE_ANALYSIS_CACHE_VERSION:
            return None
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception:
        return None


def set_cached_slide_analysis(cache_dir: Path, cache_key: str, analysis: dict[str, Any]) -> None:
    out_dir = cache_dir / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = {"_version": SLIDE_ANALYSIS_CACHE_VERSION, **analysis}
    (out_dir / f"{cache_key}.json").write_text(json.dumps(out, indent=0, default=str), encoding="utf-8")
