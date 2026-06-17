"""On-disk cache for GitHub productivity API rollups."""

from __future__ import annotations

from typing import Any

from .config import BPO_GITHUB_CACHE_TTL_SECONDS
from . import disk_cache as _dc

_NAMESPACE = "github"


def cache_key(name: str, params: dict[str, Any]) -> str:
    return _dc.cache_key(name, params)


def cache_get(key: str, ttl_seconds: int | None = None) -> Any | None:
    ttl = BPO_GITHUB_CACHE_TTL_SECONDS if ttl_seconds is None else max(0, int(ttl_seconds))
    return _dc.cache_get(_NAMESPACE, key, ttl)


def cache_set(key: str, data: Any, ttl_seconds: int | None = None) -> None:
    ttl = BPO_GITHUB_CACHE_TTL_SECONDS if ttl_seconds is None else max(0, int(ttl_seconds))
    _dc.cache_set(_NAMESPACE, key, data, ttl)


def clear_github_cache_for_tests() -> None:
    _dc.clear_namespace_for_tests(_NAMESPACE)
