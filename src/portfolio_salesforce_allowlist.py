"""Portfolio customer list driven by Salesforce Customer Entity accounts."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, AbstractSet

from .config import logger
from .salesforce_client import (
    _CHURNED_CONTRACT_STATUS_LOWER,
    _customer_name_matches_entity_account,
    _renewal_roll_up_fields,
)


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


def _entity_rows_for_portfolio_label(
    sf_label: str,
    entity_accounts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Customer Entity rows whose portfolio rollup label equals *sf_label*."""
    want = (sf_label or "").strip()
    if not want:
        return []
    return [
        a
        for a in entity_accounts
        if isinstance(a, dict) and _row_portfolio_label(a) == want
    ]


def _entity_rows_matching_customer_query(
    customer_query: str,
    entity_accounts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Customer Entity rows matching *customer_query* (Name, entity, Parent, Ultimate Parent)."""
    upper = (customer_query or "").strip().upper()
    if not upper:
        return []
    return [
        a
        for a in entity_accounts
        if isinstance(a, dict) and _customer_name_matches_entity_account(upper, a)
    ]


def _summarize_entity_rows_activity(
    display_label: str,
    matching: list[dict[str, Any]],
) -> dict[str, Any]:
    """Salesforce active/churn rollup for a set of Customer Entity rows."""
    if not matching:
        return {
            "salesforce_label": (display_label or "").strip(),
            "entity_row_count": 0,
            "active_in_salesforce": None,
            "all_entities_churned": None,
            "arr_total": 0.0,
            "arr_active": 0.0,
            "entity_names_sample": [],
            "contract_statuses_distinct": [],
            "portfolio_labels_matched": [],
        }

    statuses: set[str] = set()
    arr_total = 0.0
    arr_active = 0.0
    has_active_contract = False
    entity_names: list[str] = []
    for a in matching:
        name = (a.get("Name") or "").strip()
        if name:
            entity_names.append(name)
        st = (a.get("Contract_Status__c") or "").strip()
        if st:
            statuses.add(st)
        st_low = st.lower()
        is_churned = bool(st and st_low in _CHURNED_CONTRACT_STATUS_LOWER)
        if st and not is_churned:
            has_active_contract = True
        try:
            arr = float(a.get("ARR__c") or 0)
        except (TypeError, ValueError):
            arr = 0.0
        arr_total += arr
        if not is_churned:
            arr_active += arr

    all_churned = bool(statuses) and not has_active_contract
    portfolio_labels = sorted(
        {_row_portfolio_label(a) for a in matching if _row_portfolio_label(a)},
        key=str.lower,
    )
    out: dict[str, Any] = {
        "salesforce_label": (display_label or "").strip(),
        "entity_row_count": len(matching),
        "active_in_salesforce": has_active_contract,
        "all_entities_churned": all_churned if statuses else None,
        "arr_total": round(arr_total, 2),
        "arr_active": round(arr_active, 2),
        "entity_names_sample": entity_names[:8],
        "contract_statuses_distinct": sorted(statuses, key=str.lower)[:16],
        "portfolio_labels_matched": portfolio_labels[:12],
    }
    out.update(_renewal_roll_up_fields(matching))
    return out


def summarize_salesforce_label_activity(
    sf_label: str,
    entity_accounts: list[dict[str, Any]],
) -> dict[str, Any]:
    """Salesforce facts for a portfolio label (active vs churned contract signals)."""
    matching = _entity_rows_for_portfolio_label(sf_label, entity_accounts)
    return _summarize_entity_rows_activity((sf_label or "").strip(), matching)


def summarize_salesforce_customer_query_activity(
    customer_query: str,
    entity_accounts: list[dict[str, Any]],
) -> dict[str, Any]:
    """Salesforce facts for HELP/Jira customer scope (fuzzy entity name match)."""
    matching = _entity_rows_matching_customer_query(customer_query, entity_accounts)
    return _summarize_entity_rows_activity((customer_query or "").strip(), matching)


def format_salesforce_label_activity_hint(activity: dict[str, Any]) -> str:
    """One-line hint for logs / export warnings."""
    n = activity.get("entity_row_count") or 0
    if n == 0:
        return "Salesforce: no Customer Entity rows rolled up to this label"

    statuses = activity.get("contract_statuses_distinct") or []
    st_s = ", ".join(statuses[:6]) + ("…" if len(statuses) > 6 else "") if statuses else "(none recorded)"
    arr_total = activity.get("arr_total")
    arr_active = activity.get("arr_active")
    names = activity.get("entity_names_sample") or []
    names_s = ", ".join(names[:4]) + ("…" if len(names) > 4 else "") if names else ""
    pl = activity.get("portfolio_labels_matched") or []
    pl_s = ""
    if pl:
        pl_s = f"; SF portfolio label(s): {', '.join(pl[:4])}" + ("…" if len(pl) > 4 else "")

    if activity.get("active_in_salesforce"):
        end = activity.get("contract_end_date_nearest")
        end_bit = f", nearest contract end {end}" if end else ""
        return (
            f"Salesforce: active/non-churned ({n} entity row(s), ARR ${arr_active:,.0f} active "
            f"of ${arr_total:,.0f} total, statuses: {st_s}{end_bit}"
            + (f"; entities: {names_s}" if names_s else "")
            + pl_s
            + ")"
        )
    if activity.get("all_entities_churned"):
        return (
            f"Salesforce: churned only ({n} entity row(s), ARR ${arr_total:,.0f}, statuses: {st_s}"
            + (f"; entities: {names_s}" if names_s else "")
            + pl_s
            + ")"
        )
    return (
        f"Salesforce: {n} entity row(s), ARR ${arr_total:,.0f}, statuses: {st_s}"
        + (f"; entities: {names_s}" if names_s else "")
        + pl_s
        + " (active/churn not classified — missing Contract_Status__c)"
    )


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
            activity = summarize_salesforce_label_activity(label, entity_accounts)
            meta["salesforce_labels_unmatched"].append(
                {"salesforce_label": label, "salesforce_activity": activity},
            )
            sf_hint = format_salesforce_label_activity_hint(activity)
            warn_msg = (
                "Portfolio (Salesforce allowlist): no Pendo customer prefix matches Salesforce label "
                f"{label!r} (visitor / sitename token) — skipping. {sf_hint}"
            )
            logger.warning("%s", warn_msg)
            try:
                from .data_governance_warnings import record_data_governance_warning

                record_data_governance_warning(
                    "portfolio_sf_pendo_prefix_unmatched",
                    warn_msg,
                    salesforce_activity=activity,
                    context={"salesforce_label": label},
                )
            except Exception:
                pass
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
