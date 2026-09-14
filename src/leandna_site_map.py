"""Map Cortex customer names to LeanDNA site IDs."""

from __future__ import annotations

import os
import re

from .config import logger
from .slide_team import load_teams


def _env_sites_key(customer: str) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", customer.strip().upper()).strip("_")
    return f"LEANDNA_SITES_{slug}" if slug else "LEANDNA_SITES"


def _sites_from_raw(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        parts = [str(x).strip() for x in raw if str(x).strip()]
        return ",".join(parts) if parts else None
    text = str(raw).strip()
    return text or None


def resolve_customer_sites(customer: str) -> str | None:
    """Return comma-separated LeanDNA site IDs, or None to use all authorized sites.

    Resolution order:
    1. ``LEANDNA_SITES_{CUSTOMER}`` (non-alphanumerics become ``_``)
    2. ``leandna_site_ids`` on the matching ``config/teams.yaml`` customer key
    """
    name = (customer or "").strip()
    if not name:
        logger.warning("LeanDNA site map: empty customer; using all authorized sites")
        return None
    env_key = _env_sites_key(name)
    env_val = _sites_from_raw(os.environ.get(env_key))
    if env_val:
        return env_val
    teams = load_teams()
    entry: object | None = None
    if isinstance(teams, dict):
        want = name.casefold()
        for key, val in teams.items():
            if str(key).strip().casefold() == want:
                entry = val
                break
    if isinstance(entry, dict):
        mapped = _sites_from_raw(entry.get("leandna_site_ids"))
        if mapped:
            return mapped
    logger.warning(
        "LeanDNA site map: no %s and no teams.yaml leandna_site_ids for %r; using all authorized sites",
        env_key,
        name,
    )
    return None
