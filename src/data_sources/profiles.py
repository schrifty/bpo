"""Named datasource bundles requested by flows — not tied to loader implementation."""

from __future__ import annotations

from .registry import SourceId

# All-customers markdown export (`scripts/export_llm_context_snapshot.py`).
PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS = "llm_export_all_customers"

PROFILE_LLM_EXPORT_ALL_CUSTOMERS: frozenset[SourceId] = frozenset(
    (
        SourceId.PENDO_PORTFOLIO_ROLLUP,
        SourceId.CS_REPORT_ALL_CUSTOMERS_WEEK,
        SourceId.SALESFORCE_PORTFOLIO_AGGREGATE,
        SourceId.JIRA_HELP_PORTFOLIO,
    )
)
