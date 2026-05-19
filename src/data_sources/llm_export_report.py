"""Build the all-customers LLM export report from an explicit datasource profile."""

from __future__ import annotations

import os
from typing import Any

from .profiles import PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS, PROFILE_LLM_EXPORT_ALL_CUSTOMERS
from .registry import SourceId
from .loaders.salesforce_portfolio_aggregate import salesforce_portfolio_aggregate_for_report

from ..llm_export_csr import attach_csr_top_customers_for_llm_export
from ..llm_export_slack import attach_slack_top_customers_for_llm_export
from ..llm_export_salesforce_comprehensive import attach_salesforce_comprehensive_for_llm_export
from ..llm_export_salesforce_universe import merge_salesforce_universe_for_llm_export


# Deck ``get_portfolio_report`` defaults to 20 lines / 4 read-heavy in ``pendo_client``; LLM export
# raises both so ``portfolio_signals`` is not slide-limited (still ranked / de-duped by that layer).
_LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_LINES = 50_000
_LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_READ_HEAVY = 50_000


def _llm_export_portfolio_signal_caps() -> tuple[int, int]:
    """Optional env: ``BPO_LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_LINES`` (applies to both caps if set)."""
    raw = (os.environ.get("BPO_LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_LINES") or "").strip()
    if not raw:
        return (_LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_LINES, _LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_READ_HEAVY)
    try:
        n = max(100, int(raw))
    except ValueError:
        return (_LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_LINES, _LLM_EXPORT_PORTFOLIO_SIGNAL_MAX_READ_HEAVY)
    return (n, n)


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

    _sig_lines, _sig_rh = _llm_export_portfolio_signal_caps()
    portfolio = pc.get_portfolio_report(
        days=days,
        cohort_rollup_from_slide_yaml=False,
        portfolio_signals_max_lines=_sig_lines,
        portfolio_signals_max_read_heavy=_sig_rh,
    )
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

    merge_salesforce_universe_for_llm_export(report)
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

    try:
        csr_summary = attach_csr_top_customers_for_llm_export(report)
        n_sel = int(csr_summary.get("customers_selected") or 0)
        n_ok = int(csr_summary.get("customers_with_csr_data") or 0)
        if n_sel == 0:
            provenance.append(
                _provenance_row(
                    SourceId.CS_REPORT_ALL_CUSTOMERS_WEEK,
                    status="skipped",
                    detail="no_salesforce_rollups_for_top_arr_selection",
                )
            )
        elif n_ok < n_sel:
            provenance.append(
                _provenance_row(
                    SourceId.CS_REPORT_ALL_CUSTOMERS_WEEK,
                    status="partial",
                    detail=f"top_{n_sel}_by_arr with_data={n_ok}",
                )
            )
        else:
            provenance.append(
                _provenance_row(
                    SourceId.CS_REPORT_ALL_CUSTOMERS_WEEK,
                    status="ok",
                    detail=f"top_{n_sel}_by_arr",
                )
            )
    except Exception as e:
        err = {"error": str(e), "source": "cs_report"}
        report["csr"] = {
            "scope": "top_customers_by_arr",
            "customers": {},
            "platform_health": dict(err),
            "supply_chain": dict(err),
            "platform_value": dict(err),
        }
        provenance.append(
            _provenance_row(SourceId.CS_REPORT_ALL_CUSTOMERS_WEEK, status="error", detail=str(e))
        )

    try:
        slack_summary = attach_slack_top_customers_for_llm_export(report)
        if not slack_summary.get("enabled"):
            provenance.append(
                _provenance_row(
                    SourceId.SLACK_CUSTOMER_CONVERSATIONS,
                    status="skipped",
                    detail="BPO_LLM_EXPORT_SLACK disabled",
                )
            )
        elif not slack_summary.get("slack_configured"):
            provenance.append(
                _provenance_row(
                    SourceId.SLACK_CUSTOMER_CONVERSATIONS,
                    status="skipped",
                    detail="slack_not_configured",
                )
            )
        elif int(slack_summary.get("customers_selected") or 0) == 0:
            provenance.append(
                _provenance_row(
                    SourceId.SLACK_CUSTOMER_CONVERSATIONS,
                    status="skipped",
                    detail="no_salesforce_rollups_for_top_arr_selection",
                )
            )
        elif int(slack_summary.get("customers_slack_errors") or 0) > 0:
            provenance.append(
                _provenance_row(
                    SourceId.SLACK_CUSTOMER_CONVERSATIONS,
                    status="partial",
                    detail=(
                        f"top_{slack_summary.get('customers_selected')} "
                        f"with_data={slack_summary.get('customers_with_slack_data')} "
                        f"errors={slack_summary.get('customers_slack_errors')}"
                    ),
                )
            )
        else:
            provenance.append(
                _provenance_row(
                    SourceId.SLACK_CUSTOMER_CONVERSATIONS,
                    status="ok",
                    detail=f"top_{slack_summary.get('customers_selected')}_by_arr",
                )
            )
    except Exception as e:
        report["slack"] = {"scope": "top_customers_by_arr", "error": str(e)[:500], "customers": {}}
        provenance.append(
            _provenance_row(SourceId.SLACK_CUSTOMER_CONVERSATIONS, status="error", detail=str(e))
        )

    try:
        sf_comp_summary = attach_salesforce_comprehensive_for_llm_export(report)
        if sf_comp_summary.get("enabled") is False:
            provenance.append(
                _provenance_row(
                    SourceId.SALESFORCE_COMPREHENSIVE_PORTFOLIO,
                    status="skipped",
                    detail="BPO_LLM_EXPORT_SF_COMPREHENSIVE disabled",
                )
            )
        elif not sf_comp_summary.get("salesforce_configured"):
            provenance.append(
                _provenance_row(
                    SourceId.SALESFORCE_COMPREHENSIVE_PORTFOLIO,
                    status="skipped",
                    detail="salesforce_not_configured",
                )
            )
        elif sf_comp_summary.get("customers_errors"):
            provenance.append(
                _provenance_row(
                    SourceId.SALESFORCE_COMPREHENSIVE_PORTFOLIO,
                    status="partial",
                    detail=(
                        f"matched={sf_comp_summary.get('customers_matched')}/"
                        f"{sf_comp_summary.get('customers_requested')} "
                        f"errors={sf_comp_summary.get('customers_errors')}"
                    ),
                )
            )
        else:
            provenance.append(
                _provenance_row(SourceId.SALESFORCE_COMPREHENSIVE_PORTFOLIO, status="ok")
            )
    except Exception as e:
        report["salesforce_comprehensive_portfolio"] = {
            "configured": False,
            "error": str(e)[:500],
        }
        provenance.append(
            _provenance_row(SourceId.SALESFORCE_COMPREHENSIVE_PORTFOLIO, status="error", detail=str(e))
        )

    report["signals"] = []
    try:
        from src.llm_export_jira import attach_jira_top_customers_for_llm_export

        jira_summary = attach_jira_top_customers_for_llm_export(report)
        n_sel = int(jira_summary.get("customers_selected") or 0)
        n_ok = int(jira_summary.get("customers_with_jira_data") or 0)
        if n_sel == 0:
            provenance.append(
                _provenance_row(
                    SourceId.JIRA_HELP_PORTFOLIO,
                    status="skipped",
                    detail="no_salesforce_rollups_for_top_arr_selection",
                )
            )
        elif int(jira_summary.get("customers_jira_errors") or 0) > 0:
            provenance.append(
                _provenance_row(
                    SourceId.JIRA_HELP_PORTFOLIO,
                    status="partial",
                    detail=f"top_{n_sel}_by_arr with_data={n_ok}",
                )
            )
        else:
            provenance.append(
                _provenance_row(
                    SourceId.JIRA_HELP_PORTFOLIO,
                    status="ok",
                    detail=f"top_{n_sel}_by_arr",
                )
            )
    except Exception as e:
        report["jira"] = {
            "scope": "top_customers_by_arr",
            "error": str(e)[:500],
            "customers": {},
        }
        provenance.append(_provenance_row(SourceId.JIRA_HELP_PORTFOLIO, status="error", detail=str(e)))

    report["_data_source_provenance"] = {
        "profile_id": PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
        "sources": provenance,
    }
    return report
