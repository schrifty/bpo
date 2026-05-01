"""Canonical datasource identifiers.

Flows (QBR, exports, decks) select subsets of these ids via profiles; loaders live alongside
this package and must not import deck or QBR modules.
"""

from __future__ import annotations

from enum import Enum


class SourceId(str, Enum):
    """Stable ids for orchestration, logging, and provenance."""

    PENDO_PORTFOLIO_ROLLUP = "pendo_portfolio_rollup"
    CS_REPORT_ALL_CUSTOMERS_WEEK = "cs_report_all_customers_week"
    SALESFORCE_PORTFOLIO_AGGREGATE = "salesforce_portfolio_aggregate"
    JIRA_HELP_PORTFOLIO = "jira_help_portfolio"
