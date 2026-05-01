"""Canonical datasource identifiers.

Flows (QBR, exports, decks) select subsets of these ids via profiles; loaders live alongside
this package and must not import deck or QBR modules.
"""

from __future__ import annotations

from enum import Enum


class SourceId(str, Enum):
    """Stable ids for orchestration, logging, and provenance.

    **Pendo (single customer):** Built through :meth:`pendo_client.PendoClient.get_customer_health_report`
    (engagement, sites, features, guides, Jira/Salesforce attachments inside that pipeline, etc.).
    Distinct from :attr:`PENDO_PORTFOLIO_ROLLUP`, which is all-customers portfolio rollup.

    **CS Report (per customer):** Week delta rows for one customer via
    ``cs_report_client.get_customer_platform_health``,
    ``get_customer_supply_chain``, ``get_customer_platform_value`` — typically merged into
    ``report["csr"]`` on health reports.

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
    PENDO_CUSTOMER_HEALTH = "pendo_customer_health"
    CS_REPORT_ALL_CUSTOMERS_WEEK = "cs_report_all_customers_week"
    CS_REPORT_CUSTOMER_WEEK = "cs_report_customer_week"
    SALESFORCE_PORTFOLIO_AGGREGATE = "salesforce_portfolio_aggregate"
    JIRA_HELP_PORTFOLIO = "jira_help_portfolio"

    LEANDNA_ITEM_MASTER = "leandna_item_master"
    LEANDNA_SHORTAGE_TRENDS = "leandna_shortage_trends"
    LEANDNA_LEAN_PROJECTS = "leandna_lean_projects"
