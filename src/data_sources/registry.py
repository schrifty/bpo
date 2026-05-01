"""Canonical datasource identifiers.

Flows (QBR, exports, decks) select subsets of these ids via profiles; loaders live alongside
this package and must not import deck or QBR modules.
"""

from __future__ import annotations

from enum import Enum


class SourceId(str, Enum):
    """Stable ids for orchestration, logging, and provenance.

    LeanDNA Data API: implementations live under ``src/leandna_*_client.py`` and
    ``*_enrich.py``. Report dict keys match the value strings below.

    HTTP surfaces (OpenAPI: fetch with ``scripts/fetch_leandna_data_api_swagger.py``):

    - **Item master:** ``GET /data/ItemMasterData``
    - **Shortages:** ``.../MaterialShortages/ShortagesByItem/Weekly``,
      ``.../Daily``, ``.../ShortagesByOrder``,
      ``.../ShortagesByItemWithScheduledDeliveries/Weekly`` (enrichment uses Weekly
      + scheduled-deliveries weekly in practice; Daily/ByOrder exist on the client)
    - **Lean projects:** ``GET /data/LeanProject``,
      ``GET /data/LeanProject/{{ids}}/Savings``
    """

    PENDO_PORTFOLIO_ROLLUP = "pendo_portfolio_rollup"
    CS_REPORT_ALL_CUSTOMERS_WEEK = "cs_report_all_customers_week"
    SALESFORCE_PORTFOLIO_AGGREGATE = "salesforce_portfolio_aggregate"
    JIRA_HELP_PORTFOLIO = "jira_help_portfolio"

    LEANDNA_ITEM_MASTER = "leandna_item_master"
    LEANDNA_SHORTAGE_TRENDS = "leandna_shortage_trends"
    LEANDNA_LEAN_PROJECTS = "leandna_lean_projects"
