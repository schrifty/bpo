#!/usr/bin/env python3
"""Emit ``config/comprehensive_data_element_list.json`` — registry of data elements BPO can surface.

Paths are logical dotted paths on the **single-customer health report** from
``PendoClient.get_customer_health_report`` unless prefixed ``portfolio.``, ``teams_yaml.``, or
``config.``. Does **not** enumerate per-page / per-feature Pendo detail rows (see ``terms``).

Run from repo root:
  python scripts/generate_data_summary_catalog.py

"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "config" / "comprehensive_data_element_list.json"


def _e(path: str, *terms: str) -> dict[str, Any]:
    tlist = [path, *[x for x in terms if x]]
    return {"path": path, "terms": tlist}


def build_entries() -> list[dict[str, Any]]:
    """Human-facing catalog rows (``path`` + ``terms``)."""
    rows: list[dict[str, Any]] = []

    # ── Meta / identity ─────────────────────────────────────────────────────
    rows.extend(
        [
            _e("customer", "[report] customer short name / query key", "Pendo identity"),
            _e("generated", "[report] generated date stamp", "health report"),
            _e("days", "[report] Pendo lookback window days"),
            _e("salesforce_primary_account_id", "[report] resolved Salesforce Account Id"),
            _e("customer_key_type", "[report] identity resolution: salesforce_account_id | name | none"),
            _e(
                "customer_name",
                "[data_summary] mirrors report.customer",
                "hydrate shorthand",
            ),
            _e("report_date", "[data_summary] mirrors report.generated"),
            _e("quarter", "[report] fiscal quarter label (QBR may set)"),
            _e("quarter_start", "[report] ISO date"),
            _e("quarter_end", "[report] ISO date"),
        ]
    )

    # ── Pendo health core (get_customer_health) ─────────────────────────────
    rows.extend(
        [
            _e("account.name", "[Pendo] account display name"),
            _e("account.region", "[Pendo] account region"),
            _e("account.csm", "[Pendo] CSM names joined"),
            _e("account.account_id", "[Pendo] subscription/account id"),
            _e("account.total_visitors", "[Pendo] visitor count (excludes internal)"),
            _e("account.internal_visitors", "[Pendo] LeanDNA staff visitors excluded from metrics"),
            _e("account.total_sites", "[Pendo] matched factory/site names"),
            _e("engagement.active_7d", "[Pendo] weekly active visitors"),
            _e("engagement.active_30d", "[Pendo] monthly active visitors"),
            _e("engagement.dormant", "[Pendo] dormant bucket"),
            _e("engagement.active_rate_7d", "[Pendo] active_7d / total_visitors %"),
            _e("engagement.role_active", "[Pendo] dict role→count (active)"),
            _e("engagement.role_dormant", "[Pendo] dict role→count (dormant)"),
            _e("benchmarks.customer_active_rate", "[Pendo] same as engagement rate %"),
            _e("benchmarks.peer_median_rate", "[Pendo] portfolio median active %"),
            _e("benchmarks.peer_count", "[Pendo] peers in benchmark set"),
            _e("benchmarks.cohort", "[config/cohorts.yaml] Pendo prefix cohort key"),
            _e("benchmarks.cohort_name", "[Pendo] display cohort name"),
            _e("benchmarks.cohort_median_rate", "[Pendo] cohort median active %"),
            _e("benchmarks.cohort_count", "[Pendo] cohort peer count"),
            _e("signals", "[Pendo] list of signal strings (not enumerated per-line here)"),
        ]
    )

    # ── Site list (aggregates only — no page/feature drill-down) ─────────────
    rows.extend(
        [
            _e(
                "sites",
                "[Pendo] array of site dicts — keys include sitename, visitors, pages_used, features_used, total_events, total_minutes, last_active",
                "no per-page tables in this catalog",
            ),
            _e("site_details", "[data_summary] trimmed copy of sites for prompts"),
            _e("site_details.name", "[Pendo→summary] site display name"),
            _e("site_details.visitors", "[Pendo→summary] visitors at site"),
            _e("site_details.pages_used", "[Pendo→summary] pages used"),
            _e("site_details.features_used", "[Pendo→summary] features used"),
            _e("site_details.events", "[Pendo→summary] total_events"),
            _e("site_details.total_minutes", "[Pendo→summary] minutes"),
            _e("site_details.last_active", "[Pendo→summary] last active"),
        ]
    )

    # ── Pendo ranked lists (blobs — not per-row catalog) ───────────────────────
    rows.extend(
        [
            _e("top_pages", "[Pendo] ranked page list — omitting per-row catalog"),
            _e("top_features", "[Pendo] ranked feature list — omitting per-row catalog"),
            _e("feature_adoption_insights", "[Pendo] structured adoption insights object"),
        ]
    )

    rows.extend(
        [
            _e("champions", "[Pendo] champion user rows"),
            _e("at_risk_users", "[Pendo] at-risk user rows"),
        ]
    )

    # ── Behavioral modules ────────────────────────────────────────────────────
    rows.extend(
        [
            _e("depth.customer", "[Pendo depth]"),
            _e("depth.days", "[Pendo depth]"),
            _e("depth.total_feature_events", "[Pendo depth]"),
            _e("depth.active_users", "[Pendo depth]"),
            _e("depth.read_events", "[Pendo depth]"),
            _e("depth.write_events", "[Pendo depth]"),
            _e("depth.collab_events", "[Pendo depth]"),
            _e("depth.write_ratio", "[Pendo depth] read vs write %"),
            _e("depth.breakdown", "[Pendo depth] category breakdown rows"),
            _e("exports.customer", "[Pendo exports]"),
            _e("exports.days", "[Pendo exports]"),
            _e("exports.total_exports", "[Pendo exports]"),
            _e("exports.exports_per_active_user", "[Pendo exports]"),
            _e("exports.active_users", "[Pendo exports]"),
            _e("exports.by_feature", "[Pendo exports] ranked export features"),
            _e("exports.top_exporters", "[Pendo exports] top users"),
            _e("kei.customer", "[Pendo Kei]"),
            _e("kei.days", "[Pendo Kei]"),
            _e("kei.total_queries", "[Pendo Kei] chat / Kei events"),
            _e("kei.unique_users", "[Pendo Kei]"),
            _e("kei.active_users", "[Pendo Kei]"),
            _e("kei.adoption_rate", "[Pendo Kei] %"),
            _e("kei.executive_users", "[Pendo Kei] exec role count"),
            _e("kei.executive_queries", "[Pendo Kei] exec query volume"),
            _e("kei.users", "[Pendo Kei] top user rows (email, role, queries, is_executive)"),
            _e("guides.customer", "[Pendo guides]"),
            _e("guides.days", "[Pendo guides]"),
            _e("guides.total_guide_events", "[Pendo guides]"),
            _e("guides.users_who_saw_guides", "[Pendo guides]"),
            _e("guides.total_visitors", "[Pendo guides]"),
            _e("guides.guide_reach", "[Pendo guides] %"),
            _e("guides.seen", "[Pendo guides] guideSeen count"),
            _e("guides.advanced", "[Pendo guides] guideAdvanced count"),
            _e("guides.dismissed", "[Pendo guides] guideDismissed count"),
            _e("guides.dismiss_rate", "[Pendo guides] %"),
            _e("guides.advance_rate", "[Pendo guides] %"),
            _e("guides.top_guides", "[Pendo guides] ranked guide rows"),
            _e("poll_events", "[Pendo] poll/survey engagement blob"),
            _e("frustration", "[Pendo] frustration signals blob"),
            _e("track_events_breakdown", "[Pendo] track-type breakdown blob"),
            _e("visitor_languages", "[Pendo] language distribution blob"),
            _e("pendo_catalog_appendix", "[Pendo] catalog appendix summary"),
        ]
    )

    # ── Jira / JSM (HELP-focused bundle) ─────────────────────────────────────
    rows.extend(
        [
            _e("jira.customer", "[Jira]"),
            _e("jira.days", "[Jira] lookback"),
            _e("jira.total_issues", "[Jira] HELP scope issue count in window"),
            _e("jira.open_issues", "[Jira]"),
            _e("jira.resolved_issues", "[Jira]"),
            _e("jira.escalated", "[Jira] escalated count"),
            _e("jira.open_bugs", "[Jira]"),
            _e("jira.by_status", "[Jira] histogram"),
            _e("jira.by_type", "[Jira] histogram"),
            _e("jira.by_priority", "[Jira] histogram"),
            _e("jira.by_sentiment", "[Jira] histogram"),
            _e("jira.by_request_type", "[Jira] histogram"),
            _e("jira.tickets_over_time", "[Jira] weekly buckets"),
            _e("jira.recent_issues", "[Jira] sample rows (key/summary)"),
            _e("jira.escalated_issues", "[Jira] sample escalations"),
            _e("jira.engineering", "[Jira] LEAN engineering slice"),
            _e("jira.enhancements", "[Jira] ER slice"),
            _e("jira.ttfr", "[Jira] time-to-first-response SLA stats"),
            _e("jira.ttr", "[Jira] time-to-resolve SLA stats"),
            _e("jira.customer_ticket_metrics", "[Jira] pre-aggregated ticket metrics"),
            _e("jira.jsm_organizations_resolved", "[Jira] org resolution trace"),
            _e("jira.help_scope", "[Jira] scope description string"),
            _e("jira.jql_queries", "[Jira] JQL trace for QA"),
            _e("support.total_tickets", "[data_summary] from jira.total_issues"),
            _e("support.open", "[data_summary] from jira.open_issues"),
            _e("support.resolved", "[data_summary] from jira.resolved_issues"),
        ]
    )

    # ── Salesforce ─────────────────────────────────────────────────────────
    rows.extend(
        [
            _e("salesforce.customer", "[SFDC]"),
            _e("salesforce.accounts", "[SFDC] matched Account rows"),
            _e("salesforce.account_ids", "[SFDC] Ids"),
            _e("salesforce.opportunity_count_this_year", "[SFDC]"),
            _e("salesforce.pipeline_arr", "[SFDC] pipeline ARR"),
            _e("salesforce.matched", "[SFDC] boolean"),
            _e("salesforce.resolution", "[SFDC] salesforce_account_id | name | none"),
            _e("salesforce.primary_account_id", "[SFDC]"),
            _e("salesforce.error", "[SFDC] when present"),
            _e("salesforce.row_limit", "[SFDC comprehensive] max rows per category"),
            _e("salesforce.account_ids_expanded", "[SFDC comprehensive] hierarchy-expanded Account Ids"),
            _e("salesforce.categories", "[SFDC comprehensive] dict label→rows (wide SOQL bundles)"),
            _e("salesforce.category_errors", "[SFDC comprehensive] per-category fetch errors"),
        ]
    )
    # Matched Customer Entity account fields.
    rows.extend(
        [
            _e("salesforce.accounts[].Id", "[SFDC] Account Id"),
            _e("salesforce.accounts[].Name", "[SFDC] Account name"),
            _e("salesforce.accounts[].LeanDNA_Entity_Name__c", "[SFDC] LeanDNA entity name"),
            _e("salesforce.accounts[].US_Persons_Only_Customer__c", "[SFDC] compliance flag"),
            _e("salesforce.accounts[].Contract_Status__c", "[SFDC] account contract status"),
            _e("salesforce.accounts[].factory_start_date", "[SFDC] factory/operational start (SF_ACCOUNT_FACTORY_START_DATE_FIELD)"),
            _e("salesforce.accounts[].Contract_Contract_Start_Date__c", "[SFDC] contract start date"),
            _e("salesforce.accounts[].Contract_Contract_End_Date__c", "[SFDC] contract end date"),
            _e("salesforce.accounts[].ARR__c", "[SFDC] account ARR custom field"),
            _e("salesforce.accounts[].ParentId", "[SFDC] parent account id"),
            _e("salesforce.accounts[].parent_name", "[SFDC] normalized parent account name"),
            _e("salesforce.accounts[].ultimate_parent_name", "[SFDC] normalized ultimate parent name"),
        ]
    )
    # Comprehensive category labels + field-level rows from salesforce_client MAINSTREAM_OBJECT_FIELDS.
    sf_category_fields: dict[str, tuple[str, ...]] = {
        "contacts": (
            "Id", "FirstName", "LastName", "Email", "Phone", "AccountId", "Title",
            "MailingCity", "MailingState", "MailingCountry", "OwnerId", "CreatedDate",
        ),
        "opportunities": (
            "Id", "Name", "AccountId", "StageName", "Amount", "Probability", "CloseDate",
            "Type", "ForecastCategoryName", "OwnerId", "CreatedDate", "LastModifiedDate",
        ),
        "opportunity_line_items": (
            "Id", "OpportunityId", "Product2Id", "Quantity", "UnitPrice", "TotalPrice", "ServiceDate",
        ),
        "cases": (
            "Id", "CaseNumber", "Subject", "Status", "Priority", "Origin",
            "AccountId", "ContactId", "OwnerId", "CreatedDate", "ClosedDate",
        ),
        "tasks": (
            "Id", "Subject", "Status", "Priority", "ActivityDate", "WhoId", "WhatId",
            "OwnerId", "IsClosed", "CreatedDate",
        ),
        "events": (
            "Id", "Subject", "StartDateTime", "EndDateTime", "Location", "WhoId",
            "WhatId", "OwnerId", "CreatedDate",
        ),
        "contracts": (
            "Id", "ContractNumber", "AccountId", "Status", "StartDate", "EndDate",
            "ContractTerm", "OwnerId", "CreatedDate",
        ),
        "orders": (
            "Id", "OrderNumber", "AccountId", "EffectiveDate", "Status", "TotalAmount",
            "Type", "OwnerId", "CreatedDate",
        ),
        "quotes": (
            "Id", "QuoteNumber", "Name", "OpportunityId", "AccountId", "Status",
            "ExpirationDate", "GrandTotal", "OwnerId", "CreatedDate",
        ),
        "assets": (
            "Id", "Name", "AccountId", "SerialNumber", "Status", "Product2Id", "InstallDate", "OwnerId",
        ),
        "owners_sample": ("Id", "Name", "Username", "Email", "IsActive", "ProfileId", "UserType"),
        "campaign_members": ("Id", "CampaignId", "LeadId", "ContactId", "Status", "CreatedDate"),
        "campaigns_related": (
            "Id", "Name", "Status", "Type", "StartDate", "EndDate", "BudgetedCost", "ActualCost", "OwnerId",
        ),
        "leads_name_match": (
            "Id", "FirstName", "LastName", "Company", "Email", "Phone", "Status", "LeadSource",
            "OwnerId", "CreatedDate", "LastModifiedDate", "IsConverted",
        ),
        "products_org_sample": (
            "Id", "Name", "ProductCode", "Description", "IsActive", "Family", "CreatedDate",
        ),
        "pricebooks_org_sample": ("Id", "Name", "IsActive", "IsStandard", "Description"),
    }
    for category, cols in sf_category_fields.items():
        rows.append(_e(f"salesforce.categories.{category}", "[SFDC comprehensive] category table"))
        for col in cols:
            rows.append(
                _e(
                    f"salesforce.categories.{category}[].{col}",
                    "[SFDC comprehensive] category row field",
                )
            )

    # ── Slack (customer channel conversation digests) ─────────────────────────
    rows.extend(
        [
            _e("slack.source", "[Slack]"),
            _e("slack.customer", "[Slack] customer or lookup name"),
            _e("slack.days", "[Slack] lookback window"),
            _e("slack.configured", "[Slack] token present"),
            _e("slack.channels_matched", "[Slack] matched channel id/name list"),
            _e("slack.conversation_summaries", "[Slack] per-channel digest blocks"),
            _e("slack.conversation_summaries[].channel_id", "[Slack]"),
            _e("slack.conversation_summaries[].channel_name", "[Slack]"),
            _e("slack.conversation_summaries[].message_count", "[Slack]"),
            _e("slack.conversation_summaries[].summary_lines", "[Slack] recent human messages"),
            _e("slack.conversation_summaries[].summary_text", "[Slack] bullet markdown"),
            _e("slack.combined_summary_markdown", "[Slack] all channels for customer"),
            _e("slack.note", "[Slack] when no channels matched"),
            _e("slack.error", "[Slack] when fetch failed"),
            _e("slack.skipped", "[Slack] not configured or disabled"),
        ]
    )

    # ── CS Report (CSR) — full nested metrics ───────────────────────────────
    rows.extend(
        [
            _e("csr.platform_health", "[CSR] platform health block"),
            _e("csr.platform_health.customer", "[CSR]"),
            _e("csr.platform_health.source", "[CSR] cs_report"),
            _e("csr.platform_health.factory_count", "[CSR]"),
            _e("csr.platform_health.health_distribution", "[CSR] health bucket counts"),
            _e("csr.platform_health.total_shortages", "[CSR]"),
            _e("csr.platform_health.total_critical_shortages", "[CSR]"),
            _e("csr.platform_health.sites", "[CSR] per-factory health rows"),
            _e("csr.platform_health.error", "[CSR] when load failed"),
            _e("csr.supply_chain.customer", "[CSR]"),
            _e("csr.supply_chain.source", "[CSR]"),
            _e("csr.supply_chain.factory_count", "[CSR]"),
            _e("csr.supply_chain.totals.on_hand", "[CSR]"),
            _e("csr.supply_chain.totals.on_order", "[CSR]"),
            _e("csr.supply_chain.totals.excess_on_hand", "[CSR]"),
            _e("csr.supply_chain.totals.excess_on_order", "[CSR]"),
            _e("csr.supply_chain.totals.past_due_po", "[CSR]"),
            _e("csr.supply_chain.totals.past_due_req", "[CSR]"),
            _e("csr.supply_chain.sites", "[CSR] per-factory supply rows"),
            _e("csr.supply_chain.error", "[CSR]"),
            _e("csr.platform_value.customer", "[CSR]"),
            _e("csr.platform_value.source", "[CSR]"),
            _e("csr.platform_value.factory_count", "[CSR]"),
            _e("csr.platform_value.total_savings", "[CSR]"),
            _e("csr.platform_value.total_open_ia_value", "[CSR]"),
            _e("csr.platform_value.total_potential_savings", "[CSR]"),
            _e("csr.platform_value.total_potential_to_sell", "[CSR]"),
            _e("csr.platform_value.total_recs_created_30d", "[CSR]"),
            _e("csr.platform_value.total_pos_placed_30d", "[CSR]"),
            _e("csr.platform_value.total_overdue_tasks", "[CSR]"),
            _e("csr.platform_value.sites", "[CSR] per-factory ROI rows"),
            _e("csr.platform_value.error", "[CSR]"),
        ]
    )

    # CSR site-row leaves (representative; same keys on each list item when present)
    ph_site = (
        "factory health_score clear_to_build_pct clear_to_commit_pct "
        "component_availability_pct component_availability_projected_pct shortages "
        "critical_shortages weekly_active_buyers_pct buyer_mapping_quality high_risk_items site entity"
    )
    rows.append(
        _e(
            "csr.platform_health.sites[]",
            f"[CSR site row] keys may include: {ph_site}",
        )
    )
    sc_site = (
        "factory on_hand_value on_order_value excess_on_hand doi_days days_coverage "
        "turns_of_inventory late_pos late_prs site entity"
    )
    rows.append(
        _e(
            "csr.supply_chain.sites[]",
            f"[CSR site row] keys may include: {sc_site}",
        )
    )
    pv_site = (
        "factory savings_current_period open_ia_value recs_created_30d pos_placed_30d "
        "overdue_tasks current_fy_spend previous_fy_spend site entity"
    )
    rows.append(
        _e(
            "csr.platform_value.sites[]",
            f"[CSR site row] keys may include: {pv_site}",
        )
    )

    # ── data_summary copies / rolls-ups (hydrate) ───────────────────────────
    rows.extend(
        [
            _e("total_users", "[data_summary] alias of total_visitors"),
            _e("total_visitors", "[data_summary]"),
            _e("unique_visitors", "[data_summary] alias"),
            _e("active_users", "[data_summary] from account.active_visitors if present"),
            _e("total_sites", "[data_summary]"),
            _e("active_sites", "[data_summary]"),
            _e("health_score", "[data_summary] from account"),
            _e("account_total_minutes", "[data_summary] summed site minutes"),
            _e("account_avg_weekly_hours", "[data_summary] derived"),
            _e("total_shortages", "[data_summary] from csr.platform_health"),
            _e("total_critical_shortages", "[data_summary]"),
            _e("weekly_active_buyers_pct_avg", "[data_summary] averaged CSR site rows"),
            _e("cs_health_sites", "[data_summary] CSR snippet for prompts"),
            _e("cs_health_sites.site", "[data_summary] CSR site label"),
            _e("cs_health_sites.health", "[data_summary] CSR health bucket"),
            _e("cs_health_sites.ctb", "[data_summary] CTB %"),
            _e("cs_health_sites.ctc", "[data_summary] CTC %"),
            _e("platform_value", "[data_summary] copy of csr.platform_value"),
            _e("supply_chain", "[data_summary] copy of csr.supply_chain"),
        ]
    )

    # Flatten common platform_value / supply_chain totals (also under data_summary when csr merged)
    rows.extend(
        [
            _e("platform_value.total_open_ia_value", "[CSR→summary]"),
            _e("platform_value.total_potential_savings", "[CSR→summary]"),
            _e("platform_value.total_potential_to_sell", "[CSR→summary]"),
            _e("platform_value.total_recs_created_30d", "[CSR→summary]"),
            _e("platform_value.total_pos_placed_30d", "[CSR→summary]"),
            _e("platform_value.total_overdue_tasks", "[CSR→summary]"),
            _e("platform_value.total_savings", "[CSR→summary]"),
            _e("platform_value.factory_count", "[CSR→summary]"),
        ]
    )
    rows.extend(
        [
            _e("supply_chain.factory_count", "[CSR→summary]"),
            _e("supply_chain.totals.on_hand", "[CSR→summary]"),
            _e("supply_chain.totals.on_order", "[CSR→summary]"),
            _e("supply_chain.totals.excess_on_hand", "[CSR→summary]"),
            _e("supply_chain.totals.excess_on_order", "[CSR→summary]"),
            _e("supply_chain.totals.past_due_po", "[CSR→summary]"),
            _e("supply_chain.totals.past_due_req", "[CSR→summary]"),
        ]
    )

    # ── LeanDNA API enrichments (report-level blobs) ────────────────────────
    rows.extend(
        [
            _e("leandna_item_master", "[LeanDNA Data API] item master enrichment blob"),
            _e("leandna_shortage_trends", "[LeanDNA Data API] shortage trends enrichment"),
            _e("leandna_lean_projects", "[LeanDNA Data API] lean projects + savings enrichment"),
        ]
    )

    # ── Signals / optional LLM overlays ─────────────────────────────────────
    rows.extend(
        [
            _e("signals_trend_context", "[Pendo] optional trend context when BPO_SIGNALS_TRENDS"),
            _e("_signals_llm_manifest_rules", "[internal] LLM editorial manifest excerpt"),
            _e("_signals_llm_slide_prompt", "[internal] slide prompt excerpt"),
        ]
    )

    # ── Portfolio report (cross-customer) ───────────────────────────────────
    rows.extend(
        [
            _e("portfolio.type", "[portfolio] discriminator"),
            _e("portfolio.days", "[portfolio]"),
            _e("portfolio.generated", "[portfolio]"),
            _e("portfolio.customer_count", "[portfolio]"),
            _e("portfolio.customers", "[portfolio] array of per-customer summary rows"),
            _e("portfolio.portfolio_signals", "[portfolio] ranked signal lines"),
            _e("portfolio.portfolio_trends", "[portfolio] trend aggregates"),
            _e("portfolio.portfolio_leaders", "[portfolio] leader board"),
            _e("portfolio.cohort_digest", "[portfolio] cohort digest"),
            _e("portfolio.cohort_findings_bullets", "[portfolio] cohort bullets"),
        ]
    )
    rows.append(
        _e(
            "portfolio.customers[]",
            "[portfolio row] keys include customer pendo_csm engagement benchmarks signals "
            "active_users total_users login_pct depth kei guides exports",
        )
    )

    # ── GitHub (when wired) ─────────────────────────────────────────────────
    rows.extend(
        [
            _e("github", "[GitHub] blob when present on report — structure varies by wiring"),
        ]
    )

    # ── config/teams.yaml (not on report dict — config by customer key) ─────
    rows.extend(
        [
            _e(
                "teams_yaml.customer_team",
                "[config/teams.yaml] list of {name, title} customer-facing team",
            ),
            _e(
                "teams_yaml.leandna_team",
                "[config/teams.yaml] list of {name, title} LeanDNA team",
            ),
            _e("teams_yaml.leandna_site_ids", "[config/teams.yaml] optional LeanDNA site ids per customer"),
            _e(
                "cohorts_yaml",
                "[config cohorts.yaml] cohort / optional exclude / vertical per customer; "
                "Pendo orphans → pendo_orphans.yaml (not nested under report)",
            ),
            _e(
                "pendo_orphans_yaml",
                "[config pendo_orphans.yaml] Pendo sitename prefixes with no SF Customer Entity match, "
                "omitted from portfolio rollup (not nested under report)",
            ),
        ]
    )

    # ── QBR / deck internal keys (not business metrics but present on report)
    rows.extend(
        [
            _e("_slide_plan", "[internal] resolved template slide plan"),
            _e("_hydrate_slide_hints", "[internal] hydrate hints map"),
            _e("_slides_svc", "[internal] Google Slides service"),
            _e("_drive_svc", "[internal] Drive service"),
        ]
    )

    return rows


def main() -> int:
    payload = {
        "version": 1,
        "_comment": (
            "Catalog of data elements BPO can access for QBR / decks (path + terms). "
            "Terms drive hydrate phrase matching and qbr_mappings target resolution (see "
            "resolve_data_summary_target_path). "
            "Pendo per-page and per-feature detail rows are intentionally omitted (see sites/top_pages). "
            "After regenerating from this script, re-merge any custom hydrate phrases into ``terms`` if needed."
        ),
        "entries": build_entries(),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    OUT.write_text(text, encoding="utf-8")
    print(f"Wrote {OUT} ({len(payload['entries'])} entries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
