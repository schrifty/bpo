"""Portfolio customer list driven by Salesforce Customer Entity accounts."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, AbstractSet

from .config import logger


def _name_matches_word_boundary(query: str, text: str) -> bool:
    """Same rule as ``pendo_client._name_matches``: whole-word match only."""
    if not query or not text:
        return False
    return bool(re.search(rf"\b{re.escape(query)}\b", text, re.IGNORECASE))


def portfolio_labels_from_entity_accounts(rows: list[dict[str, Any]]) -> list[str]:
    """Distinct portfolio labels from Customer Entity rows (rollup: ultimate → parent → Name)."""
    seen: set[str] = set()
    out: list[str] = []
    for a in rows:
        if not isinstance(a, dict):
            continue
        label = _row_portfolio_label(a)
        if not label:
            continue
        low = label.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(label)
    out.sort(key=str.lower)
    return out


def _row_portfolio_label(a: dict[str, Any]) -> str:
    up = (a.get("ultimate_parent_name") or "").strip()
    if up:
        return up
    pn = (a.get("parent_name") or "").strip()
    if pn:
        return pn
    return (a.get("Name") or "").strip()


def resolve_sf_label_to_pendo_prefix(sf_label: str, pendo_prefixes: AbstractSet[str]) -> str | None:
    """Map a Salesforce-derived label to a Pendo sitename first-token / visitor bucket.

    Uses the same word-boundary rule as visitor matching so multi-word SF names can still
    resolve to a short Pendo prefix (e.g. ``Spirit`` inside ``Spirit AeroSystems``).
    """
    if not pendo_prefixes:
        return None
    canon = {p.lower(): p for p in pendo_prefixes}
    sl = (sf_label or "").strip()
    if not sl:
        return None
    low = sl.lower()
    if low in canon:
        return canon[low]

    candidates: list[str] = []
    for p in pendo_prefixes:
        if _name_matches_word_boundary(p, sl):
            candidates.append(canon[p.lower()])
    if candidates:
        return max(candidates, key=len)

    parts = sl.split()
    if parts:
        first = parts[0]
        if first.lower() in canon:
            return canon[first.lower()]
        first_hits: list[str] = []
        for p in pendo_prefixes:
            if _name_matches_word_boundary(p, first):
                first_hits.append(canon[p.lower()])
        if first_hits:
            return max(first_hits, key=len)

    return None


def salesforce_allowlist_pendo_keys(
    *,
    entity_accounts: list[dict[str, Any]],
    pendo_prefixes: AbstractSet[str],
    is_excluded: Callable[[str], bool],
) -> tuple[list[str], dict[str, Any]]:
    """Build ordered Pendo customer keys from Salesforce entities.

    Returns ``(pendo_keys, meta)``. Unmatched Salesforce labels emit WARNING logs and appear in
    ``meta["salesforce_labels_unmatched"]``. Excluded prefixes are omitted from *pendo_keys* but
    recorded under ``meta["salesforce_labels_excluded_after_resolve"]``.
    """
    sf_labels = portfolio_labels_from_entity_accounts(entity_accounts)
    meta: dict[str, Any] = {
        "salesforce_entity_row_count": len(entity_accounts),
        "salesforce_portfolio_labels": list(sf_labels),
        "salesforce_labels_unmatched": [],
        "salesforce_labels_excluded_after_resolve": [],
        "pendo_key_to_salesforce_label": {},
    }
    ordered: list[str] = []
    seen_lower: set[str] = set()

    for label in sf_labels:
        key = resolve_sf_label_to_pendo_prefix(label, pendo_prefixes)
        if key is None:
            meta["salesforce_labels_unmatched"].append(label)
            logger.warning(
                "Portfolio (Salesforce allowlist): no Pendo customer prefix matches Salesforce label %r "
                "(visitor / sitename token) — skipping",
                label,
            )
            continue
        if is_excluded(key):
            meta["salesforce_labels_excluded_after_resolve"].append(
                {"salesforce_label": label, "pendo_prefix": key},
            )
            continue
        kl = key.lower()
        if kl in seen_lower:
            continue
        seen_lower.add(kl)
        ordered.append(key)
        meta["pendo_key_to_salesforce_label"][key] = label

    return ordered, meta
