"""Portfolio Salesforce rollup for all-customers reports (revenue book → export shape)."""

from __future__ import annotations

from typing import Any

_SF_NOT_CONFIGURED_MSG = (
    "Salesforce not configured: set SF_LOGIN_URL, SF_CONSUMER_KEY, SF_USERNAME, "
    "and SF_PRIVATE_KEY or SF_PRIVATE_KEY_PATH (JWT integration)."
)


def salesforce_portfolio_aggregate_for_report(report: dict[str, Any]) -> dict[str, Any]:
    """Attach ``portfolio_revenue_book`` via :func:`enrich_portfolio_report_with_revenue_book` and map to ``salesforce`` shape."""
    from src.data_source_health import _salesforce_configured
    from src.deck_variants import enrich_portfolio_report_with_revenue_book

    if not _salesforce_configured():
        return {
            "error": _SF_NOT_CONFIGURED_MSG,
            "matched": False,
            "resolution": "none",
            "source": "salesforce",
        }
    enrich_portfolio_report_with_revenue_book(report)
    prb = report.get("portfolio_revenue_book") or {}
    if prb.get("error"):
        return {
            "error": str(prb.get("error")),
            "matched": False,
            "resolution": "none",
            "source": "salesforce",
        }
    if prb.get("configured") is False:
        return {
            "error": _SF_NOT_CONFIGURED_MSG,
            "matched": False,
            "resolution": "none",
            "source": "salesforce",
        }

    matched_n = int(prb.get("salesforce_matched_customers") or 0)
    accounts: list[dict[str, Any]] = []
    for row in prb.get("top_customers_by_arr") or []:
        cust = row.get("customer")
        if not cust:
            continue
        accounts.append({"Name": cust, "ARR__c": row.get("arr"), "Type": "Customer Entity"})

    return {
        "customer": "All Customers",
        "matched": matched_n > 0,
        "resolution": "portfolio_aggregate",
        "primary_account_id": None,
        "accounts": accounts,
        "account_ids": [],
        "pipeline_arr": float(prb.get("pipeline_arr") or 0),
        "opportunity_count_this_year": int(prb.get("opportunity_count_this_year") or 0),
        "total_arr": prb.get("total_arr"),
        "active_installed_base_arr": prb.get("active_installed_base_arr"),
        "churned_contract_arr": prb.get("churned_contract_arr"),
        "pendo_customers": prb.get("pendo_customers"),
        "salesforce_matched_customers": matched_n,
        "salesforce_unmatched_customers": prb.get("salesforce_unmatched_customers"),
        "active_customer_count": prb.get("active_customer_count"),
        "churned_customer_count": prb.get("churned_customer_count"),
    }
