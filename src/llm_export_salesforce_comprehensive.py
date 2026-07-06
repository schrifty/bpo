"""Attach per-customer Salesforce comprehensive payloads to the all-customers LLM export."""

from __future__ import annotations

import os
from typing import Any

from .config import logger
from .data_source_health import _salesforce_configured


def _env_truthy(name: str, *, default: bool) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _entity_accounts_with_grouping(
    entity_accounts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return copies of entity rows enriched with SF-first grouping labels.

    Adds ``division_group``, ``corporate_group``, and ``ultimate_parent_group`` so the
    export can be grouped by Ultimate Parent even when ``ultimate_parent_name`` is blank
    (no ``SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP``). See ``src/salesforce_reporting``.
    """
    from .salesforce_reporting import (
        entity_account_corporate_group,
        entity_account_division_group,
        entity_account_ultimate_parent_group,
    )

    enriched: list[dict[str, Any]] = []
    for account in entity_accounts:
        if not isinstance(account, dict):
            continue
        row = dict(account)
        row["division_group"] = entity_account_division_group(account)
        row["corporate_group"] = entity_account_corporate_group(account)
        row["ultimate_parent_group"] = entity_account_ultimate_parent_group(account)
        enriched.append(row)
    return enriched


def _entity_names_sample(rows_in_group: list[dict[str, Any]], *, limit: int = 12) -> list[str]:
    names: list[str] = []
    for account in rows_in_group:
        name = (account.get("Name") or "").strip()
        if name and name not in names:
            names.append(name)
        if len(names) >= limit:
            break
    return names


def _build_arr_by_ultimate_parent(
    entity_accounts: list[dict[str, Any]],
    *,
    contract_rollups: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Full-book ARR rollup keyed by ultimate parent, sorted by ``current_arr`` descending.

    Uses portfolio **contract rollups** (corporate reporting groups) — the same source as
    ``selection_ranked`` — so divisions like Carrier HVAC roll up correctly. Includes all
    ``commercial_status`` values (not only the current book). Entity rows enrich name samples
    and supply fallback buckets when the revenue book has no matching rollup label.
    """
    from .llm_export_csr import group_contract_rollups_by_ultimate_parent
    from .salesforce_commercial_status import (
        derive_commercial_status,
        is_current_book_commercial_status,
        rollup_arr_fields,
    )
    from .salesforce_reporting import aggregate_accounts_by_ultimate_parent

    rows_by_parent: dict[str, dict[str, Any]] = {}
    rollups = contract_rollups or []
    if rollups:
        for row in group_contract_rollups_by_ultimate_parent(rollups, current_book_only=False):
            rows_by_parent[str(row["ultimate_parent"])] = dict(row)

    entity_groups = aggregate_accounts_by_ultimate_parent(entity_accounts) if entity_accounts else {}
    for parent, rows_in_group in entity_groups.items():
        if parent in rows_by_parent:
            row = rows_by_parent[parent]
            row["entity_names_sample"] = _entity_names_sample(rows_in_group)
            if not int(row.get("entity_count") or 0):
                row["entity_count"] = len(rows_in_group)
            continue
        if rollups:
            continue
        commercial_status = derive_commercial_status(rows_in_group, renewal_in_flight=False)
        arr_fields = rollup_arr_fields(rows_in_group, commercial_status=commercial_status)
        rows_by_parent[parent] = {
            "ultimate_parent": parent,
            "salesforce_labels": [],
            "arr": arr_fields["historical_arr"],
            "historical_arr": arr_fields["historical_arr"],
            "active_arr": arr_fields["active_arr"],
            "renewal_arr": arr_fields["renewal_arr"],
            "current_arr": arr_fields["current_arr"],
            "entity_count": len(rows_in_group),
            "commercial_status": commercial_status,
            "active": is_current_book_commercial_status(commercial_status),
            "entity_names_sample": _entity_names_sample(rows_in_group),
        }

    rows = list(rows_by_parent.values())
    rows.sort(
        key=lambda r: (
            -float(r.get("current_arr") or 0),
            -float(r.get("arr") or 0),
            str(r.get("ultimate_parent") or "").lower(),
        )
    )
    return rows


def llm_export_sf_comprehensive_enabled() -> bool:
    """When false, skip per-customer comprehensive SOQL (``CORTEX_LLM_EXPORT_SF_COMPREHENSIVE``)."""
    return _env_truthy("CORTEX_LLM_EXPORT_SF_COMPREHENSIVE", default=True)


def llm_export_sf_comprehensive_row_limit() -> int:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_SF_COMPREHENSIVE_ROW_LIMIT") or "").strip()
    if not raw:
        return 8
    try:
        return max(1, min(int(raw), 500))
    except ValueError:
        return 8


def llm_export_sf_comprehensive_customer_cap() -> int | None:
    """Max customers to fetch. Default 12 (top active by ARR). ``0``/``all`` = no cap."""
    raw = (os.environ.get("CORTEX_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP") or "").strip()
    if not raw:
        return 12
    if raw.lower() in ("0", "all", "none", "unlimited"):
        return None
    try:
        return max(1, int(raw))
    except ValueError:
        return 12


def _labels_for_comprehensive_fetch(
    report: dict[str, Any],
    *,
    cap: int | None,
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    """Choose Customer Entity labels for §3c fetch (CSM: top active book by ARR when capped)."""
    if cap is None:
        labels = _rollup_labels_with_segment(report)
        return labels, {"selection": "all_portfolio_labels", "top_n": None}

    from .llm_export_csr import top_active_ultimate_parents_by_arr_for_llm_export

    ranked = top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=cap)
    labels: list[tuple[str, str]] = []
    seen: set[str] = set()
    for row in ranked:
        for label in row.get("salesforce_labels") or [row.get("ultimate_parent")]:
            s = str(label or "").strip()
            if not s:
                continue
            key = s.lower()
            if key in seen:
                continue
            seen.add(key)
            labels.append((s, "active"))
    return labels, {
        "selection": "top_active_ultimate_parents_by_arr",
        "top_n": cap,
        "selection_ranked": ranked,
    }


def _rollup_labels_with_segment(report: dict[str, Any]) -> list[tuple[str, str]]:
    """Unique portfolio labels as ``(label, 'active'|'churned')``."""
    ordered: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(rows: list[Any], segment: str) -> None:
        for r in rows:
            if not isinstance(r, dict):
                continue
            label = str(r.get("customer") or "").strip()
            if not label:
                continue
            key = label.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append((label, segment))

    book = report.get("_llm_export_salesforce_revenue_book")
    if not isinstance(book, dict):
        book = report.get("portfolio_revenue_book")
    if isinstance(book, dict):
        for r in book.get("matched_customer_contract_rollups") or []:
            if not isinstance(r, dict):
                continue
            label = str(r.get("customer") or "").strip()
            if not label:
                continue
            if r.get("commercial_status") == "ACTIVE":
                seg = "active"
            elif r.get("commercial_status") == "OUT_OF_CONTRACT_RENEWING":
                seg = "renewal_negotiation"
            elif r.get("commercial_status") == "FUTURE":
                seg = "future_contract"
            elif r.get("commercial_status") == "CHURNED":
                seg = "churned"
            elif r.get("active") is not False:
                seg = "active"
            elif r.get("renewal_in_flight") is True:
                seg = "renewal_negotiation"
            else:
                seg = "churned"
            key = label.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append((label, seg))

    churn_seg = report.get("salesforce_churned_segment")
    if isinstance(churn_seg, dict):
        sf = churn_seg.get("salesforce")
        if isinstance(sf, dict):
            _add(sf.get("matched_customer_contract_rollups") or [], "churned")
    renewal_seg = report.get("salesforce_renewal_negotiation_segment")
    if isinstance(renewal_seg, dict):
        sf = renewal_seg.get("salesforce")
        if isinstance(sf, dict):
            _add(sf.get("matched_customer_contract_rollups") or [], "renewal_negotiation")

    return ordered


def attach_salesforce_comprehensive_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Populate ``report['salesforce_comprehensive_portfolio']`` for LLM export §3c.

    Fetches the same multi-object slice as the ``salesforce_comprehensive`` deck
    (:meth:`SalesforceClient.get_customer_salesforce_comprehensive`) for each portfolio
    Customer Entity labels (by default top active accounts by ARR; churned only when uncapped).
    Also attaches all Customer Entity account
    rows and portfolio expansion KPIs when available.
    """
    summary: dict[str, Any] = {
        "enabled": llm_export_sf_comprehensive_enabled(),
        "salesforce_configured": _salesforce_configured(),
        "customers_requested": 0,
        "customers_fetched": 0,
        "customers_matched": 0,
        "customers_errors": 0,
        "customers_drive_cache_hit": 0,
        "customers_salesforce_fetch": 0,
        "row_limit": llm_export_sf_comprehensive_row_limit(),
        "customer_cap": llm_export_sf_comprehensive_customer_cap(),
        "integration_cache_kind": "salesforce_comprehensive",
    }
    if not summary["enabled"]:
        report["salesforce_comprehensive_portfolio"] = {
            "configured": False,
            "skipped": "disabled_via_CORTEX_LLM_EXPORT_SF_COMPREHENSIVE",
        }
        report["_llm_export_salesforce_comprehensive"] = summary
        return summary

    if not summary["salesforce_configured"]:
        report["salesforce_comprehensive_portfolio"] = {
            "configured": False,
            "skipped": "salesforce_not_configured",
        }
        report["_llm_export_salesforce_comprehensive"] = summary
        return summary

    cap = summary["customer_cap"]
    labels, selection_meta = _labels_for_comprehensive_fetch(report, cap=cap)
    summary.update(selection_meta)
    summary["customers_requested"] = len(labels)

    from src.customer_identity import lookup_salesforce_identity
    from src.salesforce_client import SalesforceClient
    from src.salesforce_comprehensive_cache import load_or_fetch_salesforce_comprehensive

    sf = SalesforceClient()
    row_limit = int(summary["row_limit"])
    by_customer: dict[str, Any] = {}
    total = len(labels)

    for idx, (label, segment) in enumerate(labels, 1):
        logger.info(
            "LLM export: SF comprehensive %d/%d — %s (%s)",
            idx,
            total,
            label,
            segment,
        )
        sf_ids, sf_prim = lookup_salesforce_identity(label)
        sf_kwargs: dict[str, Any] = {}
        if sf_ids:
            sf_kwargs["preferred_account_ids"] = sf_ids
            sf_kwargs["primary_account_id"] = sf_prim
        payload, source = load_or_fetch_salesforce_comprehensive(
            label,
            row_limit=row_limit,
            **sf_kwargs,
        )
        payload = dict(payload)
        payload["customer_segment"] = segment
        by_customer[label] = payload
        summary["customers_fetched"] += 1
        if source == "drive_cache":
            summary["customers_drive_cache_hit"] += 1
        else:
            summary["customers_salesforce_fetch"] += 1
        if payload.get("matched"):
            summary["customers_matched"] += 1
        if payload.get("error"):
            summary["customers_errors"] += 1

    entity_accounts: list[dict[str, Any]] = []
    try:
        entity_accounts = sf.get_entity_accounts()
    except Exception as e:
        logger.warning("LLM export: get_entity_accounts failed: %s", e)
        summary["entity_accounts_error"] = str(e)[:500]

    entity_accounts = _entity_accounts_with_grouping(entity_accounts)

    book = report.get("_llm_export_salesforce_revenue_book")
    if not isinstance(book, dict):
        book = report.get("portfolio_revenue_book")

    contract_rollups: list[dict[str, Any]] = []
    if isinstance(book, dict):
        contract_rollups = [
            r
            for r in (book.get("matched_customer_contract_rollups") or [])
            if isinstance(r, dict)
        ]
    arr_by_ultimate_parent = _build_arr_by_ultimate_parent(
        entity_accounts,
        contract_rollups=contract_rollups,
    )

    expansion = None
    if isinstance(book, dict):
        expansion = book.get("expansion_kpis")
    if expansion is None:
        expansion = report.get("portfolio_expansion_book")

    report["salesforce_comprehensive_portfolio"] = {
        "configured": True,
        "row_limit": row_limit,
        "customer_count": len(labels),
        "by_customer": by_customer,
        "entity_accounts": entity_accounts,
        "entity_accounts_count": len(entity_accounts),
        "arr_by_ultimate_parent": arr_by_ultimate_parent,
        "portfolio_expansion_book": expansion,
        "note": (
            "Per-customer payloads mirror the salesforce_comprehensive deck (mainstream object "
            "categories scoped to matched Customer Entity accounts). "
            "``arr_by_ultimate_parent`` sums portfolio contract rollups by ultimate parent (same grouping as "
            "``selection_ranked``) and includes all commercial_status segments; sort by ``current_arr``. "
            "When CORTEX_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP is set (default 12), only the top "
            "current-book ultimate parents by ARR are fetched for ``by_customer`` — same ranking as §2 Jira and "
            "§4 CS Report top-N. Set CUSTOMER_CAP=0 or all to fetch every portfolio label."
        ),
    }
    report["_llm_export_salesforce_comprehensive"] = summary
    logger.info(
        "LLM export: attached Salesforce comprehensive for %d customer label(s) "
        "(%d matched, %d Drive cache hit(s), %d Salesforce fetch(es), row_limit=%d)",
        len(labels),
        summary["customers_matched"],
        summary["customers_drive_cache_hit"],
        summary["customers_salesforce_fetch"],
        row_limit,
    )
    return summary
