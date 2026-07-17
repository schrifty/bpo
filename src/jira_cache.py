"""On-disk cache for read-only Jira REST API responses."""

from __future__ import annotations

from typing import Any

from . import config as _config
from . import disk_cache as _dc

_NAMESPACE = "jira"


def cache_enabled() -> bool:
    return _config.CORTEX_JIRA_CACHE_TTL_SECONDS > 0


def _ttl_seconds() -> int:
    return max(0, int(_config.CORTEX_JIRA_CACHE_TTL_SECONDS))


def cache_get(tenant: str, op: str, params: dict[str, Any]) -> Any | None:
    if not cache_enabled():
        return None
    key = _dc.cache_key(f"{tenant}:{op}", params)
    return _dc.cache_get(_NAMESPACE, key, _ttl_seconds())


def cache_set(tenant: str, op: str, params: dict[str, Any], data: Any) -> None:
    if not cache_enabled():
        return
    key = _dc.cache_key(f"{tenant}:{op}", params)
    _dc.cache_set(_NAMESPACE, key, data, _ttl_seconds())


def clear_jira_cache_for_tests() -> None:
    _dc.clear_namespace_for_tests(_NAMESPACE)
