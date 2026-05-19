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
                "ARR__c": row.get("arr"),
                "Type": "Customer Entity",
                "active_in_salesforce": row.get("active"),
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
    for row in rollups:
        try:
            arr_sum += float(row.get("arr") or 0)
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
    }
    if segment == "active":
        out["pipeline_arr"] = float(book.get("pipeline_arr") or 0)
        out["opportunity_count_this_year"] = int(book.get("opportunity_count_this_year") or 0)
        out["total_arr"] = book.get("total_arr")
        out["active_installed_base_arr"] = book.get("active_installed_base_arr")
        out["churned_contract_arr"] = book.get("churned_contract_arr")
        out["pendo_customers"] = book.get("pendo_customers")
        out["salesforce_matched_customers"] = book.get("salesforce_matched_customers")
        out["salesforce_unmatched_customers"] = book.get("salesforce_unmatched_customers")
        out["active_customer_count"] = book.get("active_customer_count")
        out["churned_customer_count"] = book.get("churned_customer_count")
        out["expansion_kpis"] = book.get("expansion_kpis")
    else:
        renewal_n = sum(1 for r in rollups if r.get("renewal_in_flight") is True)
        out["portfolio_book_note"] = (
            "Portfolio-wide pipeline totals are omitted here so churn rows are not mixed with "
            "installed-base totals. Per-row pipeline_arr_including_parent_accounts and "
            "renewal_in_flight reflect open Opportunities on parent accounts when entity "
            "contracts are churned/expired."
        )
        if renewal_n:
            out["renewal_in_flight_customer_count"] = renewal_n
    return out


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

    rollups: list[dict[str, Any]] = list(prb.get("matched_customer_contract_rollups") or [])
    if not rollups:
        for row in prb.get("top_customers_by_arr") or []:
            cust = row.get("customer")
            if not cust:
                continue
            rollups.append(
                {
                    "customer": cust,
                    "arr": row.get("arr"),
                    "active": row.get("active"),
                    "contract_statuses_distinct": row.get("contract_statuses_distinct"),
                    "contract_end_date_nearest": row.get("contract_end_date_nearest"),
                    "contract_end_date_farthest": row.get("contract_end_date_farthest"),
                    "days_until_contract_end_nearest": row.get("days_until_contract_end_nearest"),
                    "contract_start_date_earliest_active": row.get("contract_start_date_earliest_active"),
                    "contract_start_date_latest_active": row.get("contract_start_date_latest_active"),
                    "entity_row_count": row.get("entity_row_count"),
                }
            )
    active_rollups = [r for r in rollups if isinstance(r, dict) and r.get("active") is not False]
    out = salesforce_aggregate_from_rollups(active_rollups, book=prb, segment="active")
    matched_n = int(prb.get("salesforce_matched_customers") or 0)
    out["matched"] = matched_n > 0
    out["portfolio_expansion_book"] = report.get("portfolio_expansion_book")
    return out
