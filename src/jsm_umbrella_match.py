"""JSM organization match expansion for umbrella ultimate-parent Salesforce rollups.

When an ultimate parent (e.g. ``Safran``) has no single JSM directory label, constituent
division names and ``config/cs_report_customer_aliases.yaml`` entries resolve to multiple
``Organizations`` literals that are OR'd together in HELP JQL.
"""

from __future__ import annotations

import re
from typing import Any


def division_names_without_parenthetical(labels: list[Any]) -> list[str]:
    """JSM org labels often omit the parenthetical parent (e.g. ``Commercial HVAC`` not ``… (Carrier)``)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in labels:
        label = str(raw or "").strip()
        if not label:
            continue
        base = re.sub(r"\s*\([^)]+\)\s*$", "", label).strip()
        if not base or base == label:
            continue
        key = base.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(base)
    return out


def _load_cs_report_alias_map() -> dict[str, list[str]]:
    from .cs_report_client import _load_cs_report_alias_map as _csr_map

    return _load_cs_report_alias_map()


def cs_report_alias_terms_for_label(label: str) -> list[str]:
    """Division / CS Report alias strings for *label* (empty when unconfigured)."""
    seed = (label or "").strip()
    if not seed:
        return []
    return list(_load_cs_report_alias_map().get(seed.lower()) or [])


def jsm_directory_prefix_organizations(primary: str, candidates: list[str]) -> list[str]:
    """JSM org names that start with ``{primary} `` (umbrella family), excluding exact *primary*."""
    seed = (primary or "").strip()
    if not seed or not candidates:
        return []
    prefix = seed.lower() + " "
    out: list[str] = []
    seen: set[str] = set()
    for org in candidates:
        name = (org or "").strip()
        if not name:
            continue
        low = name.lower()
        if low == seed.lower():
            continue
        if not low.startswith(prefix):
            continue
        if low in seen:
            continue
        seen.add(low)
        out.append(name)
    return sorted(out, key=str.lower)


def expand_umbrella_jsm_match_terms(
    primary: str,
    *,
    match_terms: list[str] | None = None,
    salesforce_labels: list[str] | None = None,
) -> list[str]:
    """Ordered extra match terms for umbrella ultimate-parent JSM resolution (excludes *primary*)."""
    primary_clean = (primary or "").strip()
    out: list[str] = []
    seen: set[str] = set()

    def add(term: str) -> None:
        s = (term or "").strip()
        if not s:
            return
        if primary_clean and s.lower() == primary_clean.lower():
            return
        if s.lower() in seen:
            return
        seen.add(s.lower())
        out.append(s)

    for raw in list(match_terms or []):
        add(str(raw or ""))
    for raw in list(salesforce_labels or []):
        label = str(raw or "").strip()
        add(label)
        for division in division_names_without_parenthetical([label]):
            add(division)

    seeds = [primary_clean] if primary_clean else []
    seeds.extend(str(x or "").strip() for x in (salesforce_labels or []) if str(x or "").strip())
    for seed in seeds:
        for alias in cs_report_alias_terms_for_label(seed):
            add(alias)

    return out
