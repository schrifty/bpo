"""Portfolio Salesforce rollup for all-customers reports (revenue book → export shape)."""

from __future__ import annotations

from typing import Any

_SF_NOT_CONFIGURED_MSG = (
    "Salesforce not configured: set SF_LOGIN_URL, SF_CONSUMER_KEY, SF_USERNAME, "
    "and SF_PRIVATE_KEY or SF_PRIVATE_KEY_PATH (JWT integration)."
)


def salesforce_aggregate_from_rollups(
    rollups: list[dict[str, Any]],
    *,
    book: dict[str, Any] | None = None,
    segment: str = "active",
) -> dict[str, Any]:
    """Map pre-filtered contract rollups to the export ``salesforce`` JSON shape."""
    book = book if isinstance(book, dict) else {}
    accounts: list[dict[str, Any]] = []
    for row in rollups:
        cust = row.get("customer")
        if not cust:
            continue
        accounts.append(
            {
                "Name": cust,
                "ARR__c": row.get("historical_arr", row.get("arr")),
                "Type": "Customer Entity",
                "commercial_status": row.get("commercial_status"),
                "active_arr": row.get("active_arr"),
                "renewal_arr": row.get("renewal_arr"),
                "current_arr": row.get("current_arr"),
                "historical_arr": row.get("historical_arr", row.get("arr")),
                "contract_statuses_distinct": row.get("contract_statuses_distinct"),
                "contract_end_date_nearest": row.get("contract_end_date_nearest"),
                "contract_end_date_farthest": row.get("contract_end_date_farthest"),
                "days_until_contract_end_nearest": row.get("days_until_contract_end_nearest"),
                "contract_start_date_earliest_active": row.get("contract_start_date_earliest_active"),
                "contract_start_date_latest_active": row.get("contract_start_date_latest_active"),
                "entity_row_count": row.get("entity_row_count"),
                "renewal_in_flight": row.get("renewal_in_flight"),
                "pipeline_arr_including_parent_accounts": row.get(
                    "pipeline_arr_including_parent_accounts"
                ),
                "open_pipeline_opportunities_sample": row.get("open_pipeline_opportunities_sample"),
            }
        )
    arr_sum = 0.0
    current_sum = 0.0
    for row in rollups:
        try:
            arr_sum += float(row.get("historical_arr") or row.get("arr") or 0)
        except (TypeError, ValueError):
            pass
        try:
            current_sum += float(row.get("current_arr") or 0)
        except (TypeError, ValueError):
            pass
    out: dict[str, Any] = {
        "customer": "All Customers",
        "matched": bool(rollups),
        "resolution": "portfolio_aggregate",
        "primary_account_id": None,
        "accounts": accounts,
        "account_ids": [],
        "matched_customer_contract_rollups": list(rollups),
        "customer_segment": segment,
        "segment_customer_count": len(rollups),
        "segment_contract_arr": round(arr_sum, 2),
        "segment_current_arr": round(current_sum, 2),
    }
    if segment == "active":
        out["pipeline_arr"] = float(book.get("pipeline_arr") or 0)
        out["opportunity_count_this_year"] = int(book.get("opportunity_count_this_year") or 0)
        out["total_arr"] = book.get("total_arr")
        out["historical_arr"] = book.get("historical_arr", book.get("total_arr"))
        out["active_installed_base_arr"] = book.get("active_installed_base_arr")
        out["active_arr"] = book.get("active_arr", book.get("active_installed_base_arr"))
        out["renewal_arr"] = book.get("renewal_arr", book.get("renewal_in_flight_contract_arr"))
        out["current_arr"] = book.get("current_arr")
        out["churned_contract_arr"] = book.get("churned_contract_arr")
        out["future_contract_arr"] = book.get("future_contract_arr")
        out["pendo_customers"] = book.get("pendo_customers")
        out["salesforce_matched_customers"] = book.get("salesforce_matched_customers")
        out["salesforce_unmatched_customers"] = book.get("salesforce_unmatched_customers")
        out["active_customer_count"] = book.get("active_customer_count")
        out["churned_customer_count"] = book.get("churned_customer_count")
        out["future_customer_count"] = book.get("future_customer_count")
        out["renewal_in_flight_customer_count"] = book.get("renewal_in_flight_customer_count")
        out["expansion_kpis"] = book.get("expansion_kpis")
    else:
        out["portfolio_book_note"] = (
            "Portfolio-wide pipeline totals are omitted here so inactive-contract rows are not "
            "mixed with installed-base totals. Per-row pipeline_arr_including_parent_accounts "
            "reflects open Opportunities on parent accounts."
        )
        if segment == "renewal_negotiation":
            out["segment_note"] = (
                "``commercial_status = OUT_OF_CONTRACT_RENEWING``: expired entity contracts with "
                "open parent-account renewal pipeline (stages 3–5). Not churned-lost; see §3b-renewal."
            )
        elif segment == "future_contract":
            out["segment_note"] = (
                "``commercial_status = FUTURE``: won/signed contracts with a future start date. "
                "Not active installed base and not churn."
            )
        elif segment == "churned":
            out["segment_note"] = (
                "``commercial_status = CHURNED``: inactive contracts with no qualifying open "
                "parent-account pipeline (true churn / lost)."
            )
    return out


def salesforce_portfolio_aggregate_for_report(report: dict[str, Any]) -> dict[str, Any]:
    """Attach ``portfolio_revenue_book`` via :func:`enrich_portfolio_report_with_revenue_book` and map to ``salesforce`` shape."""
    from src.data_source_health import _salesforce_configured
    from src.deck_variants import enrich_portfolio_report_with_revenue_book
    from src.salesforce_commercial_status import rollup_in_current_book

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

    rollups: list[dict[str, Any]] = list(prb.get("matched_customer_contract_rollups") or [])
    if not rollups:
        for row in prb.get("top_customers_by_arr") or []:
            cust = row.get("customer")
            if not cust:
                continue
            rollups.append(dict(row))
    current_rollups = [
        r
        for r in rollups
        if isinstance(r, dict) and rollup_in_current_book(r)
    ]
    out = salesforce_aggregate_from_rollups(current_rollups, book=prb, segment="active")
    matched_n = int(prb.get("salesforce_matched_customers") or 0)
    out["matched"] = matched_n > 0
    out["portfolio_expansion_book"] = report.get("portfolio_expansion_book")
    return out
