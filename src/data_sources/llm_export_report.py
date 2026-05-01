"""Build the all-customers LLM export report from an explicit datasource profile."""

from __future__ import annotations

from typing import Any

from .profiles import PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS, PROFILE_LLM_EXPORT_ALL_CUSTOMERS
from .registry import SourceId
from .loaders.salesforce_portfolio_aggregate import salesforce_portfolio_aggregate_for_report


def _provenance_row(source: SourceId, *, status: str, detail: str | None = None) -> dict[str, Any]:
    row: dict[str, Any] = {"source": str(source), "status": status}
    if detail:
        row["detail"] = detail[:500]
    return row


def build_llm_export_snapshot_report(pc: Any, *, days: int) -> dict[str, Any]:
    """Run :data:`PROFILE_LLM_EXPORT_ALL_CUSTOMERS` and attach ``_data_source_provenance``.

    ``pc`` is a :class:`pendo_client.PendoClient` (typed as Any to avoid import cycles in callers).
    """
    # Profile is fixed for this builder; kept in sync with PROFILE_LLM_EXPORT_ALL_CUSTOMERS.
    _ = PROFILE_LLM_EXPORT_ALL_CUSTOMERS
    provenance: list[dict[str, Any]] = []

    portfolio = pc.get_portfolio_report(days=days, cohort_rollup_from_slide_yaml=False)
    if not isinstance(portfolio, dict):
        provenance.append(
            _provenance_row(SourceId.PENDO_PORTFOLIO_ROLLUP, status="error", detail="non-dict response")
        )
        return {
            "error": "portfolio report returned non-dict",
            "_data_source_provenance": {
                "profile_id": PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
                "sources": provenance,
            },
        }
    if portfolio.get("error"):
        provenance.append(
            _provenance_row(
                SourceId.PENDO_PORTFOLIO_ROLLUP,
                status="error",
                detail=str(portfolio.get("error")),
            )
        )
        out = dict(portfolio)
        out["_data_source_provenance"] = {
            "profile_id": PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
            "sources": provenance,
        }
        return out

    provenance.append(_provenance_row(SourceId.PENDO_PORTFOLIO_ROLLUP, status="ok"))
    report = dict(portfolio)
    report["customer"] = "All Customers"

    try:
        from src.cs_report_client import load_csr_all_customers_week

        report["csr"] = load_csr_all_customers_week()
        provenance.append(_provenance_row(SourceId.CS_REPORT_ALL_CUSTOMERS_WEEK, status="ok"))
    except Exception as e:
        err = {"error": str(e), "source": "cs_report"}
        report["csr"] = {
            "platform_health": dict(err),
            "supply_chain": dict(err),
            "platform_value": dict(err),
        }
        provenance.append(
            _provenance_row(SourceId.CS_REPORT_ALL_CUSTOMERS_WEEK, status="error", detail=str(e))
        )

    report["salesforce"] = salesforce_portfolio_aggregate_for_report(report)
    if report["salesforce"].get("error"):
        provenance.append(
            _provenance_row(
                SourceId.SALESFORCE_PORTFOLIO_AGGREGATE,
                status="error",
                detail=str(report["salesforce"].get("error")),
            )
        )
    else:
        provenance.append(_provenance_row(SourceId.SALESFORCE_PORTFOLIO_AGGREGATE, status="ok"))

    report["signals"] = []
    try:
        from src.jira_client import get_shared_jira_client

        report["jira"] = get_shared_jira_client().get_customer_jira(None, days=min(int(days), 365))
        if report["jira"].get("error"):
            provenance.append(
                _provenance_row(
                    SourceId.JIRA_HELP_PORTFOLIO,
                    status="error",
                    detail=str(report["jira"].get("error")),
                )
            )
        else:
            provenance.append(_provenance_row(SourceId.JIRA_HELP_PORTFOLIO, status="ok"))
    except Exception as e:
        report["jira"] = {"error": str(e)}
        provenance.append(_provenance_row(SourceId.JIRA_HELP_PORTFOLIO, status="error", detail=str(e)))

    report["_data_source_provenance"] = {
        "profile_id": PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
        "sources": provenance,
    }
    return report
