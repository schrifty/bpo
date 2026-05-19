"""Pendo orphan prefixes (config/pendo_orphans.yaml) — skip portfolio rollup and coarse deck lists."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

from .config import logger

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CFG_PATH = _REPO_ROOT / "config" / "pendo_orphans.yaml"


def _parse_env_extra() -> frozenset[str]:
    raw = os.environ.get("BPO_PORTFOLIO_EXCLUDE_CUSTOMERS", "")
    return frozenset(x.strip() for x in raw.split(",") if x.strip())


@lru_cache(maxsize=1)
def _yaml_prefix_set() -> frozenset[str]:
    if not _CFG_PATH.exists():
        raise RuntimeError(
            "Missing Pendo orphans list; restore config/pendo_orphans.yaml "
            f"(expected at {_CFG_PATH})"
        )
    data = yaml.safe_load(_CFG_PATH.read_text(encoding="utf-8")) or {}
    rows = data.get("prefixes")
    if rows is None:
        raise RuntimeError(
            f"Invalid {_CFG_PATH}: expected top-level 'prefixes' list (got {rows!r})"
        )
    if not isinstance(rows, list):
        raise RuntimeError(
            f"Invalid {_CFG_PATH}: 'prefixes' must be a list (got {type(rows).__name__})"
        )
    out = {str(p).strip() for p in rows if isinstance(p, str) and p.strip()}
    if not out:
        raise RuntimeError(f"Invalid {_CFG_PATH}: 'prefixes' is empty")
    return frozenset(out)


def invalidate_portfolio_prefix_cache_for_tests() -> None:
    _yaml_prefix_set.cache_clear()


def reset_for_tests() -> None:
    """Clear cached YAML load so tests can mutate config in isolation."""
    invalidate_portfolio_prefix_cache_for_tests()


def is_skipped_customer_prefix(prefix: str) -> bool:
    """Skip portfolio rollup / junk filtering for this exact Pendo-derived customer prefix."""
    if prefix in _parse_env_extra():
        return True
    try:
        return prefix in _yaml_prefix_set()
    except Exception as exc:
        logger.exception("pendo_orphans config failed: %s", exc)
        raise
