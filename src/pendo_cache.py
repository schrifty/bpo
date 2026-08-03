"""On-disk cache for heavy Pendo ``PendoClient.preload`` slices (under ``CORTEX_CACHE_DIR/pendo/``)."""

from __future__ import annotations

from typing import Any

from . import config as _config
from . import disk_cache as _dc

_NAMESPACE = "pendo"

PRELOAD_KIND_VISITORS = "visitors"
PRELOAD_KIND_FEATURE_EVENTS = "feature_events"
PRELOAD_KIND_PAGE_EVENTS = "page_events"
PRELOAD_KIND_TRACK_EVENTS = "track_events"
PRELOAD_KIND_GUIDE_EVENTS = "guide_events"
PRELOAD_KIND_PAGE_CATALOG = "page_catalog"
PRELOAD_KIND_FEATURE_CATALOG = "feature_catalog"
PRELOAD_KIND_GUIDE_CATALOG = "guide_catalog"
PRELOAD_KIND_USAGE_BY_SITE = "usage_by_site"
PRELOAD_KIND_USAGE_BY_SITE_ENTITY = "usage_by_site_entity"

_CATALOG_KINDS = frozenset(
    {
        PRELOAD_KIND_PAGE_CATALOG,
        PRELOAD_KIND_FEATURE_CATALOG,
        PRELOAD_KIND_GUIDE_CATALOG,
    }
)


def preload_cache_key(kind: str, days: int | None) -> str:
    if kind in _CATALOG_KINDS or days is None:
        return kind
    return f"{kind}_days{int(days)}"


def disk_cache_enabled() -> bool:
    return _config.CORTEX_PENDO_DISK_CACHE_TTL_SECONDS > 0


def _ttl_seconds() -> int:
    return max(0, int(_config.CORTEX_PENDO_DISK_CACHE_TTL_SECONDS))


def try_load_preload_payload(kind: str, days: int | None) -> Any | None:
    """Return a cached preload slice when present and younger than the disk TTL."""
    if not disk_cache_enabled():
        return None
    key = preload_cache_key(kind, days)
    hit = _dc.cache_get(_NAMESPACE, key, _ttl_seconds())
    if hit is not None:
        from .config import logger

        logger.debug("Pendo disk cache: hit %r", key)
    return hit


def save_preload_payload(kind: str, days: int | None, payload: Any) -> None:
    """Persist a preload slice to disk (best-effort)."""
    if not disk_cache_enabled():
        return
    key = preload_cache_key(kind, days)
    _dc.cache_set(_NAMESPACE, key, payload, _ttl_seconds())
    from .config import logger

    logger.debug("Pendo disk cache: wrote %r", key)


def clear_pendo_cache_for_tests() -> None:
    _dc.clear_namespace_for_tests(_NAMESPACE)
