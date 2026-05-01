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

# QBR LeanDNA Data API enrichments (``qbr_template.py`` → ``enrich_qbr_with_*``).
PROFILE_ID_LEANDNA_QBR_ENRICHMENTS = "leandna_qbr_enrichments"

PROFILE_LEANDNA_QBR_ENRICHMENTS: frozenset[SourceId] = frozenset(
    (
        SourceId.LEANDNA_ITEM_MASTER,
        SourceId.LEANDNA_SHORTAGE_TRENDS,
        SourceId.LEANDNA_LEAN_PROJECTS,
    )
)
