# Data Registry

Cross-system registry of data items known to BPO as of March 2026.

This document is a living governance artifact. Its first purpose is to make
missing data visible. Later passes can add canonical-source decisions,
duplication cleanup, stewardship, and review history.

## Purpose

- assign a stable identifier to each known data item
- organize items by source system
- document where each item comes from and where BPO uses it
- flag obvious missing, duplicated, derived, or suspiciously sourced items

## Scope Rules For v1

- include data items the app actively reads today
- include high-signal fields or endpoints that are clearly available in current
  source-system docs, even if BPO does not consume them yet
- do not treat Google Drive / Slides transport metadata as business data unless
  it becomes a reported metric

## Identifier Convention

Use uppercase hyphenated identifiers with enough detail to distinguish:

- metric or field name
- subject or entity
- time window when relevant
- aggregation when relevant

Examples:

- `ACTIVE-USERS-7-DAYS`
- `HELP-TICKETS-OPEN-COUNT`
- `TIME-TO-FIRST-RESPONSE-MEDIAN-1-YEAR`
- `SALESFORCE-ACCOUNT-CSM-NAME`

## Status Note Vocabulary

- `DERIVED`: calculated from lower-level fields rather than stored directly
- `MISSING`: needed by the business but not currently available from the source
- `DUPLICATE?`: similar item appears in multiple systems and needs governance
- `WRONG-SOURCE?`: item is available, but probably not from the best system
- `UNUSED`: documented/available today but not consumed by BPO yet
- `NEEDS-REVIEW`: known ambiguity in semantics, matching, or source quality

## Related Docs

- [`JIRA_DATA_SCHEMA.md`](./JIRA_DATA_SCHEMA.md)
- [`PENDO_DATA_SCHEMA.md`](./PENDO_DATA_SCHEMA.md)
- [`CSR_DATA_SCHEMA.md`](./CSR_DATA_SCHEMA.md) — Customer Success Report (CS Report) export
- [`SALESFORCE_SETUP.md`](../SALESFORCE_SETUP.md)
- [`USAGE_DATA_PRIORITIES.md`](../USAGE_DATA_PRIORITIES.md)
- [`../../src/jira_client.py`](../../src/jira_client.py)
- [`../../src/pendo_client.py`](../../src/pendo_client.py)
- [`../../src/cs_report_client.py`](../../src/cs_report_client.py)
- [`../../src/salesforce_client.py`](../../src/salesforce_client.py)

## Atlassian / Jira

### Query Surfaces

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `JIRA-JQL-ISSUE-SEARCH` | Primary issue retrieval surface for customer, project, and trend reporting. | `POST /rest/api/3/search/jql` | `docs/data-schema/JIRA_DATA_SCHEMA.md`, `src/jira_client.py` | Core surface |
| `JIRA-SERVICE-DESK-ORGANIZATIONS` | JSM organization objects used for customer matching. | `customfield_10502`, JSM organization records | `docs/data-schema/JIRA_DATA_SCHEMA.md`, `src/jira_client.py` | Core surface |
| `JIRA-ASSET-SITE-ENTITY-REFERENCE` | CMDB object references for site and entity on issues. | `customfield_11121`, `customfield_11154` | `docs/data-schema/JIRA_DATA_SCHEMA.md`, `src/jira_client.py` | `UNUSED` in most slide logic |

### Registry Entries

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `JIRA-ISSUE-KEY` | Unique issue identifier such as `HELP-12345`. | `key` | `src/jira_client.py`, ticket slides | Core identifier |
| `JIRA-ISSUE-SUMMARY` | Ticket title / short description. | `summary` | `src/jira_client.py`, support + engineering slides | Core field |
| `JIRA-ISSUE-TYPE` | Jira issue type such as Bug, Task, Epic, Developer escalation. | `issuetype.name` | `src/jira_client.py`, support/customer ticket slides | Core field |
| `JIRA-ISSUE-STATUS` | Current workflow status. | `status.name` | `src/jira_client.py`, support/customer/project slides | Core field |
| `JIRA-ISSUE-PRIORITY` | Current Jira priority. | `priority.name` | `src/jira_client.py`, support slides | Core field |
| `JIRA-ISSUE-RESOLUTION` | Final resolution if ticket is closed. | `resolution.name` | `src/jira_client.py` | Core field |
| `JIRA-ISSUE-CREATED-TIMESTAMP` | Ticket creation timestamp. | `created` | `src/jira_client.py` | Core field |
| `JIRA-ISSUE-UPDATED-TIMESTAMP` | Last update timestamp. | `updated` | `src/jira_client.py` | Core field |
| `JIRA-ISSUE-RESOLUTION-TIMESTAMP` | Ticket resolution timestamp. | `resolutiondate` | `src/jira_client.py` | Core field |
| `JIRA-CUSTOMER-NAME` | Multi-select customer association on the issue. | `customfield_10100` | `src/jira_client.py` | `DUPLICATE?` customer identity also appears elsewhere |
| `JIRA-ORGANIZATION-NAME` | JSM organization name used for customer matching. | `customfield_10502.name` | `src/jira_client.py`, support deck | `DUPLICATE?` with Salesforce account and Pendo account naming |
| `JIRA-SITE-IDS-FREE-TEXT` | Free-text site identifiers attached to a ticket. | `customfield_10613` | `src/jira_client.py` | `NEEDS-REVIEW` normalization unclear |
| `JIRA-BUG-SEVERITY` | Severity for bug tickets. | `customfield_10629.value` | `src/jira_client.py` | Core field |
| `JIRA-TIME-TO-FIRST-RESPONSE` | JSM SLA record for first response. | `customfield_10666` | `src/jira_client.py`, support/customer ticket slides | Core field |
| `JIRA-TIME-TO-RESOLUTION` | JSM SLA record for resolution. | `customfield_10665` | `src/jira_client.py`, support/customer ticket slides | Core field |
| `JIRA-REQUEST-TYPE` | Portal or request type classification. | `customfield_10604` | `src/jira_client.py` | Core field |
| `JIRA-SENTIMENT` | AI-detected customer sentiment on a ticket. | `customfield_10685` | `src/jira_client.py`, SLA slide | `NEEDS-REVIEW` semantic quality unknown |
| `JIRA-SITE-CMDB-OBJECT` | CMDB site object reference. | `customfield_11121` | `src/jira_client.py` | `UNUSED` in most report outputs |
| `JIRA-ENTITY-CMDB-OBJECT` | CMDB entity object reference. | `customfield_11154` | `src/jira_client.py` | `UNUSED` in most report outputs |
| `HELP-TICKETS-OPEN-COUNT` | Count of unresolved HELP tickets using workflow state. | JQL `project = HELP AND statusCategory != Done` | `src/jira_client.py`, engineering portfolio | Core metric |
| `PROJECT-TICKETS-OPEN-COUNT` | Open count for HELP, CUSTOMER, or LEAN project slides. | JQL `project = <KEY> AND statusCategory != Done` | `src/jira_client.py`, `src/slides_client.py` | Core metric |
| `PROJECT-TICKETS-RESOLVED-6-MONTHS` | Count of tickets resolved in last 180 days. | JQL `project = <KEY> AND resolution is not EMPTY AND resolved >= -180d` | `src/jira_client.py`, project slides | Core metric |
| `OPEN-TICKET-STATUS-DISTRIBUTION` | Histogram of current open tickets by status. | Derived from `status.name` on open-ticket JQL results | `src/jira_client.py`, project/customer slides | `DERIVED` |
| `UNRESOLVED-TICKET-TYPE-DISTRIBUTION` | Histogram of unresolved tickets by issue type. | Derived from `issuetype.name` on open-ticket results | `src/jira_client.py`, customer ticket slide | `DERIVED` |
| `MEDIAN-OPEN-TICKET-AGE-DAYS` | Median age of open tickets in days. | Derived from `created` on unresolved tickets | `src/jira_client.py`, project slides | `DERIVED` |
| `AVERAGE-RESOLUTION-CYCLE-DAYS-6-MONTHS` | Average days from created to resolved over the last 6 months. | Derived from `created` and `resolutiondate` | `src/jira_client.py`, project slides | `DERIVED` |
| `TIME-TO-FIRST-RESPONSE-MEDIAN-1-YEAR` | Median TTFR for HELP tickets over last year. | Derived from `customfield_10666.completedCycles` | `src/jira_client.py`, customer ticket slide | `DERIVED` |
| `TIME-TO-RESOLVE-MEDIAN-1-YEAR` | Median TTR for HELP tickets over last year. | Derived from `customfield_10665.completedCycles` | `src/jira_client.py`, customer ticket slide | `DERIVED` |
| `SLA-ADHERENCE-1-YEAR` | Percent of HELP tickets meeting all measured SLA cycles. | Derived from TTFR/TTR breach flags | `src/jira_client.py`, customer ticket slide | `DERIVED` |
| `HELP-TICKETS-CREATED-BY-MONTH-12-MONTHS` | Monthly HELP created volume for 12 months. | JQL `project = HELP AND (created >= -365d OR resolved >= -365d)` + `created` | `src/jira_client.py`, support deck | `DERIVED` |
| `HELP-TICKETS-RESOLVED-BY-MONTH-12-MONTHS` | Monthly HELP resolved volume for 12 months. | Same JQL + `resolutiondate` | `src/jira_client.py`, support deck | `DERIVED` |
| `HELP-TICKETS-JIRA-ESCALATED-CREATED-BY-MONTH-12-MONTHS` | Monthly created volume for HELP tickets labeled `jira_escalated`. | Same HELP trend JQL + `labels` filter | `src/jira_client.py`, support deck | `DERIVED` |
| `HELP-TICKETS-NON-ESCALATED-CREATED-BY-MONTH-12-MONTHS` | Monthly created volume for HELP tickets excluding `jira_escalated`. | Same HELP trend JQL + inverse `labels` filter | `src/jira_client.py`, support deck | `DERIVED` |
| `JIRA-ENGINEERING-OPEN-TICKETS` | LEAN tickets currently in progress/open/reopened for engineering reporting. | JQL `project = LEAN AND status in (...)` | `src/jira_client.py`, engineering slides | Core metric |
| `JIRA-ENGINEERING-CLOSED-TICKETS-30-DAYS` | LEAN tickets recently closed in the reporting period. | JQL `project = LEAN AND status = Closed AND updated >= -<days>d` | `src/jira_client.py`, engineering slides | Core metric |
| `JIRA-ENHANCEMENT-REQUESTS-OPEN` | Open ER tickets in last year. | ER JQL in `get_engineering_portfolio()` | `src/jira_client.py`, engineering slides | Core metric |
| `JIRA-ENHANCEMENT-REQUESTS-SHIPPED` | Shipped ER tickets in last year. | ER JQL in `get_engineering_portfolio()` | `src/jira_client.py`, engineering slides | Core metric |
| `JIRA-SATISFACTION-SCORE` | JSM customer satisfaction score. | `customfield_10609` | `docs/data-schema/JIRA_DATA_SCHEMA.md` | `UNUSED` |
| `JIRA-TIME-TO-DONE` | Additional SLA metric available in the instance. | `customfield_10815` | `docs/data-schema/JIRA_DATA_SCHEMA.md` | `UNUSED` |
| `JIRA-PENDING-REASON` | Reason a ticket is waiting. | `customfield_10641` | `docs/data-schema/JIRA_DATA_SCHEMA.md` | `UNUSED` |
| `JIRA-RESOLUTION-TYPE` | How a ticket was resolved. | `customfield_10679` | `docs/data-schema/JIRA_DATA_SCHEMA.md` | `UNUSED` |
| `JIRA-CSM-FREE-TEXT` | Free-text CSM field on Jira issues. | `customfield_11220` | `docs/data-schema/JIRA_DATA_SCHEMA.md` | `UNUSED`, `WRONG-SOURCE?` |

## Pendo

### Query Surfaces

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `PENDO-VISITORS-SOURCE` | Visitor metadata source for account, role, site, and activity. | Aggregation source `visitors` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-ACCOUNTS-SOURCE` | Account metadata source. | Aggregation source `accounts` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-PAGE-EVENTS-SOURCE` | Page interaction event stream. | Aggregation source `pageEvents` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-FEATURE-EVENTS-SOURCE` | Feature interaction event stream. | Aggregation source `featureEvents` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-GUIDE-EVENTS-SOURCE` | Guide lifecycle events such as seen, advanced, dismissed. | Aggregation source `guideEvents` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-POLL-EVENTS-SOURCE` | Poll and NPS response event stream. | Aggregation source `pollEvents` | `docs/data-schema/PENDO_DATA_SCHEMA.md` | `UNUSED` by app today |
| `PENDO-TRACK-EVENTS-SOURCE` | Custom app track events. | Aggregation source `events` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Used for Kei fallback |
| `PENDO-PAGE-CATALOG` | Page catalog used to map page IDs to readable names. | `GET /page` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-FEATURE-CATALOG` | Feature catalog used to map feature IDs to readable names. | `GET /feature` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-GUIDE-CATALOG` | Guide catalog used to map guide IDs to readable names. | `GET /guide` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core surface |
| `PENDO-TRACK-TYPE-CATALOG` | Track-event type catalog. | `GET /tracktype` | `docs/data-schema/PENDO_DATA_SCHEMA.md` | `UNUSED` |
| `PENDO-REPORT-CATALOG` | Saved report catalog. | `GET /report` | `docs/data-schema/PENDO_DATA_SCHEMA.md` | `UNUSED` |
| `PENDO-SEGMENT-CATALOG` | Segment catalog. | `GET /segment` | `docs/data-schema/PENDO_DATA_SCHEMA.md` | `UNUSED` |

### Registry Entries

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `PENDO-VISITOR-ID` | Stable Pendo visitor identifier. | `visitorId` | `src/pendo_client.py` | Core identifier |
| `PENDO-ACCOUNT-ID` | Stable Pendo account identifier. | `accountId`, `metadata.auto.accountid` | `src/pendo_client.py` | `DUPLICATE?` with Salesforce account identity |
| `PENDO-ACCOUNT-NAME` | Account name from Pendo account metadata. | `accounts.metadata.agent.name` | `src/pendo_client.py` | `DUPLICATE?`, `WRONG-SOURCE?` for CRM governance |
| `PENDO-SITE-NAME` | Site name associated with a visitor or event. | `metadata.agent.sitename`, `sitenames`, event snapshot property | `src/pendo_client.py` | `DUPLICATE?` with CS Report factory and Jira site refs |
| `PENDO-SITE-ID` | Site identifier attached to a visitor. | `metadata.agent.siteid`, `siteids` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core field |
| `PENDO-ENTITY-NAME` | Entity attached to a visitor or event. | `metadata.agent.entity` | `src/pendo_client.py` | `DUPLICATE?` with Jira/CS Report entity naming |
| `PENDO-CSM-NAME` | Account owner / CSM from visitor metadata. | `metadata.agent.ownername` | `src/pendo_client.py`, health deck | `WRONG-SOURCE?` probably operational, not canonical CRM |
| `PENDO-USER-EMAIL` | User email from visitor metadata. | `metadata.agent.emailaddress` | `src/pendo_client.py` | Core field |
| `PENDO-USER-ROLE` | User role from visitor metadata. | `metadata.agent.role` | `src/pendo_client.py`, health/people slides | Core field |
| `PENDO-PROFILE-TYPE` | User profile type from visitor metadata. | `metadata.agent.profiletype` | `src/pendo_client.py` | Core field |
| `PENDO-LANGUAGE` | User language setting. | `metadata.agent.language` | `docs/data-schema/PENDO_DATA_SCHEMA.md` | `UNUSED` |
| `PENDO-INTERNAL-USER-FLAG` | Whether the visitor is internal LeanDNA staff. | `metadata.agent.isinternaluser` | `src/pendo_client.py` | Core field |
| `PENDO-FIRST-VISIT-TIMESTAMP` | First visit timestamp for visitor/account. | `metadata.auto.firstvisit` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `src/pendo_client.py` | Core field |
| `PENDO-LAST-VISIT-TIMESTAMP` | Last visit timestamp for visitor/account. | `metadata.auto.lastvisit` | `src/pendo_client.py` | Core field |
| `ACTIVE-USERS-7-DAYS` | Count of visitors active within the last 7 days. | Derived from `lastvisit` | `src/pendo_client.py`, health deck | `DERIVED` |
| `ACTIVE-USERS-30-DAYS` | Count of visitors active within the last 30 days but not 7-day active. | Derived from `lastvisit` | `src/pendo_client.py`, health deck | `DERIVED` |
| `DORMANT-USERS-30-DAYS-PLUS` | Count of visitors inactive for more than 30 days. | Derived from `lastvisit` | `src/pendo_client.py`, health deck | `DERIVED` |
| `ACTIVE-RATE-7-DAYS` | Weekly active rate for a customer. | `ACTIVE-USERS-7-DAYS / total_visitors` | `src/pendo_client.py`, health deck, QA | `DERIVED`, `DUPLICATE?` with CS Report engagement proxy |
| `ROLE-ACTIVE-COUNT` | Active users grouped by role. | Derived from visitor role + `lastvisit` | `src/pendo_client.py`, engagement slide | `DERIVED` |
| `ROLE-DORMANT-COUNT` | Dormant users grouped by role. | Derived from visitor role + `lastvisit` | `src/pendo_client.py`, engagement slide | `DERIVED` |
| `TOTAL-SITES` | Count of matched customer sites in Pendo. | Derived from `sitenames` matching | `src/pendo_client.py`, health deck, QA | `DERIVED`, `DUPLICATE?` with CS Report factory count |
| `TOP-PAGES-EVENTS-30-DAYS` | Most-used pages with event and minute counts. | `pageEvents.pageId`, `numEvents`, `numMinutes` + page catalog | `src/pendo_client.py`, features slide | `DERIVED` |
| `TOP-FEATURES-EVENTS-30-DAYS` | Most-used features with event counts. | `featureEvents.featureId`, `numEvents` + feature catalog | `src/pendo_client.py`, features slide | `DERIVED` |
| `SITE-PAGE-VIEWS-30-DAYS` | Page views grouped by site/entity. | Event property `sitename` + aggregated `numEvents` | `src/pendo_client.py`, sites slide | `DERIVED` |
| `SITE-FEATURE-CLICKS-30-DAYS` | Feature clicks grouped by site/entity. | Event property `sitename` + aggregated `numEvents` | `src/pendo_client.py`, sites slide | `DERIVED` |
| `SITE-TOTAL-MINUTES-30-DAYS` | Minutes of activity grouped by site/entity. | `numMinutes` | `src/pendo_client.py`, sites slide | `DERIVED` |
| `WRITE-RATIO-30-DAYS` | Share of read/write events classified as write behavior. | Derived from categorized feature events | `src/pendo_client.py`, depth slide | `DERIVED` |
| `COLLABORATION-EVENTS-30-DAYS` | Collaboration-related feature-event count. | Derived from categorized feature events | `src/pendo_client.py`, depth slide | `DERIVED` |
| `TOTAL-EXPORTS-30-DAYS` | Total export interactions over the last 30 days. | Export-classified feature events | `src/pendo_client.py`, exports slide | `DERIVED` |
| `EXPORTS-PER-ACTIVE-USER-30-DAYS` | Total exports divided by active users. | `TOTAL-EXPORTS-30-DAYS / active_users` | `src/pendo_client.py`, exports slide | `DERIVED` |
| `KEI-QUERIES-30-DAYS` | Total Kei AI interactions. | Kei feature events plus track-event fallback | `src/pendo_client.py`, Kei slide | `DERIVED` |
| `KEI-ADOPTION-RATE-30-DAYS` | Percent of active users using Kei AI. | `unique Kei users / active_users` | `src/pendo_client.py`, Kei slide | `DERIVED` |
| `GUIDE-DISMISS-RATE-30-DAYS` | Percent of seen guides that were dismissed. | `guideDismissed / guideSeen` | `src/pendo_client.py`, guides slide | `DERIVED` |
| `GUIDE-ADVANCE-RATE-30-DAYS` | Percent of seen guides advanced by users. | `guideAdvanced / guideSeen` | `src/pendo_client.py`, guides slide | `DERIVED` |
| `GUIDE-REACH-30-DAYS` | Percent of visitors who saw at least one guide. | `users_with_guides / total_visitors` | `src/pendo_client.py`, guides slide | `DERIVED` |
| `CHAMPION-USERS` | Most recently active users for a customer. | Derived from visitor activity list | `src/pendo_client.py`, champions slide | `DERIVED` |
| `AT-RISK-USERS` | Users inactive ≥14 days and <183 days (~2 wk–6 mo). | Derived from visitor activity list | `src/pendo_client.py`, champions slide | `DERIVED` |
| `PENDO-NPS-SCORE` | NPS response data from polls. | `pollEvents.pollResponse`, `pollType` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `docs/USAGE_DATA_PRIORITIES.md` | `UNUSED`, `MISSING` from current deck outputs |
| `PENDO-FRUSTRATION-SIGNALS` | Rage clicks, dead clicks, error clicks, U-turns by page/feature/account. | Event fields `rageClickCount`, `deadClickCount`, `errorClickCount`, `uTurnCount` | `docs/data-schema/PENDO_DATA_SCHEMA.md`, `docs/USAGE_DATA_PRIORITIES.md` | `UNUSED`, high-value candidate |
| `LICENSED-SEATS` | Licensed-seat denominator for seat utilization. | Not available in Pendo | `docs/USAGE_DATA_PRIORITIES.md` | `MISSING`, likely CRM/billing |
| `RENEWAL-DATE` | Contract/renewal timing for account risk and QBRs. | Not available in Pendo | `docs/USAGE_DATA_PRIORITIES.md` | `MISSING`, likely Salesforce |
| `EXECUTIVE-SPONSOR` | Executive sponsor or key commercial contact. | Not available in Pendo | `docs/USAGE_DATA_PRIORITIES.md` | `MISSING`, likely Salesforce |

## CS Report

Column names, KPI JSON shape, and how BPO reads the workbook are documented in **[`CSR_DATA_SCHEMA.md`](./CSR_DATA_SCHEMA.md)**.

### Query Surfaces

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `CSR-LATEST-EXPORT-FILE` | Latest XLSX export from the shared drive. | Google Drive export of newest file in Data Exports folder | `docs/data-schema/CSR_DATA_SCHEMA.md`, `src/cs_report_client.py` | Core surface |
| `CSR-CUSTOMER-WEEK-ROW` | Per-customer weekly row selection in the CS Report workbook. | Workbook columns `customer`, `delta = week` | `src/cs_report_client.py` | Core surface |

### Registry Entries

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `CSR-CUSTOMER-NAME` | Customer name in the export row. | `customer` | `src/cs_report_client.py` | `DUPLICATE?` customer identity appears in several systems |
| `CSR-DELTA` | Time bucket / delta for the export row. | `delta` | `src/cs_report_client.py` | Core field |
| `CSR-FACTORY-NAME` | Factory/site name in CS Report. | `factoryName` | `src/cs_report_client.py`, QA | `DUPLICATE?` with Pendo site name |
| `CSR-SITE` | Optional site column when present. | `site` | `src/cs_report_client.py` | `DUPLICATE?` |
| `CSR-ENTITY` | Optional entity column when present. | `entity` | `src/cs_report_client.py` | `DUPLICATE?` |
| `CSR-HEALTH-SCORE` | Site/platform health classification. | `healthScore` | `src/cs_report_client.py`, platform health slide | Core field |
| `SHORTAGE-ITEM-COUNT` | Count of shortage items. | `shortageItemCount.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `CRITICAL-SHORTAGE-COUNT` | Count of critical shortages. | `criticalShortages.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `CLEAR-TO-BUILD-PERCENT` | Clear-to-build percent. | `clearToBuildPercent.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `CLEAR-TO-COMMIT-PERCENT` | Clear-to-commit percent. | `clearToCommitPercent.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `COMPONENT-AVAILABILITY-PERCENT` | Current component availability percent. | `componentAvailabilityPercent.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `COMPONENT-AVAILABILITY-PROJECTED-PERCENT` | Projected component availability percent. | `componentAvailabilityPercentProjected.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `BUYER-MAPPING-QUALITY-SCORE` | Quality score for buyer mapping. | `buyerMappingQualityScore.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `WEEKLY-ACTIVE-BUYERS-PERCENT` | Weekly active buyers percent. | `weeklyActiveBuyersPercent.endValue` | `src/cs_report_client.py`, QA, platform health slide | `DUPLICATE?` similar but not identical to Pendo active rate |
| `AGGREGATE-RISK-SCORE-HIGH-COUNT` | High-risk item count. | `aggregateRiskScoreHighCount.endValue` | `src/cs_report_client.py`, platform health slide | Core field |
| `TOTAL-ON-HAND-VALUE` | Inventory value on hand. | `totalOnHandValue.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `TOTAL-ON-ORDER-VALUE` | Inventory value on order. | `totalOnOrderValue.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `EXCESS-ON-HAND-VALUE-POSITIVE` | Positive excess inventory value on hand. | `excessOnhandValuePositive.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `EXCESS-ON-ORDER-VALUE-POSITIVE` | Positive excess inventory value on order. | `excessOnOrderValuePositive.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `DAYS-OF-INVENTORY-FORWARD` | Forward-looking DOI. | `doiForwards.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `DAYS-COVERAGE` | Days coverage measure. | `daysCoverage.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `PAST-DUE-PO-VALUE` | Value of past-due purchase orders. | `pastDuePOValue.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `PAST-DUE-REQUIREMENT-VALUE` | Value of past-due requirements. | `pastDueRequirementValue.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `LATE-PO-COUNT` | Count of late purchase orders. | `latePOCount.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `LATE-PR-COUNT` | Count of late purchase requisitions. | `latePRCount.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `DAILY-INVENTORY-USAGE` | Daily inventory usage measure. | `dailyInventoryUsage.endValue` | `src/cs_report_client.py` | `UNUSED` in current outputs |
| `TURNS-OF-INVENTORY-FORWARD` | Turns of inventory. | `toiForwards.endValue` | `src/cs_report_client.py`, supply chain slide | Core field |
| `INVENTORY-ACTION-SAVINGS-CURRENT-PERIOD` | Savings in current reporting period. | `inventoryActionCurrentReportingPeriodSavings.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `INVENTORY-ACTION-OPEN-VALUE` | Open inventory action value. | `inventoryActionOpenValue.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `RECS-CREATED-30-DAYS-COUNT` | Recommendations created in last 30 days. | `recsCreatedLast30DaysCt.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `POS-PLACED-30-DAYS-COUNT` | Purchase orders placed in last 30 days. | `posPlacedInLast30DaysCt.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `WORKBENCH-OVERDUE-TASKS-COUNT` | Count of overdue workbench tasks. | `workbenchOverdueTasksCt.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `POTENTIAL-SAVINGS` | Potential savings estimate. | `potentialSavings.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `POTENTIAL-TO-SELL` | Potential to sell estimate. | `potentialToSell.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `CURRENT-FY-SPEND` | Current fiscal-year spend. | `currentFySpend.endValue` | `src/cs_report_client.py`, platform value slide | Core field |
| `PREVIOUS-FY-SPEND` | Previous fiscal-year spend. | `previousFySpend.endValue` | `src/cs_report_client.py`, platform value slide | Core field |

## Salesforce

Field-level schema and HTTP surfaces: [`SALESFORCE_DATA_SCHEMA.md`](./SALESFORCE_DATA_SCHEMA.md).

### Query Surfaces

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `SALESFORCE-ACCOUNT-ENTITY-CONTRACT-QUERY` | Account query for customer-entity contract data. | SOQL `SELECT ... FROM Account WHERE Type = 'Customer Entity'` | `src/salesforce_client.py` | Core surface |
| `SALESFORCE-OPPORTUNITY-CREATION-QUERY` | Opportunity-count query for current-year creation volume. | SOQL `SELECT COUNT() FROM Opportunity ... CALENDAR_YEAR(CreatedDate)` | `src/salesforce_client.py` | Core surface |
| `SALESFORCE-OPPORTUNITY-PIPELINE-ARR-QUERY` | Opportunity query for advanced pipeline ARR. | SOQL `SELECT SUM(ARR__c) ... StageName IN (...)` | `src/salesforce_client.py` | Core surface |

### Registry Entries

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `SALESFORCE-ACCOUNT-ID` | Salesforce Account record ID. | `Account.Id` | `src/salesforce_client.py` | Core identifier |
| `SALESFORCE-ACCOUNT-NAME` | Account name in Salesforce. | `Account.Name` | `src/salesforce_client.py` | `DUPLICATE?` with Pendo/Jira naming |
| `SALESFORCE-LEANDNA-ENTITY-NAME` | LeanDNA-specific entity/customer name on Account. | `Account.LeanDNA_Entity_Name__c` | `src/salesforce_client.py` | Core match field |
| `SALESFORCE-US-PERSONS-ONLY-CUSTOMER-FLAG` | Compliance-related customer flag. | `Account.US_Persons_Only_Customer__c` | `src/salesforce_client.py` | `UNUSED` in current deck outputs |
| `SALESFORCE-CONTRACT-STATUS` | Contract status for matched customer account. | `Account.Contract_Status__c` | `src/salesforce_client.py`, account reporting | Core field |
| `SALESFORCE-CONTRACT-START-DATE` | Contract start date. | `Account.Contract_Contract_Start_Date__c` | `src/salesforce_client.py` | Core field |
| `SALESFORCE-CONTRACT-END-DATE` | Contract end date. | `Account.Contract_Contract_End_Date__c` | `src/salesforce_client.py` | Core field, likely renewal proxy |
| `SALESFORCE-ARR` | Annual recurring revenue on the customer entity account. | `Account.ARR__c` | `src/salesforce_client.py` | Core field |
| `SALESFORCE-OPPORTUNITY-TYPE` | Opportunity type used in creation/pipeline filters. | `Opportunity.Type` | `src/salesforce_client.py` | Core field |
| `SALESFORCE-PIPELINE-STAGE` | Opportunity stage used for pipeline ARR. | `Opportunity.StageName` | `src/salesforce_client.py` | Core field |
| `SALESFORCE-OPPORTUNITY-CREATED-DATE` | Opportunity creation date. | `Opportunity.CreatedDate` | `src/salesforce_client.py` | Core field |
| `SALESFORCE-OPPORTUNITY-COUNT-THIS-YEAR` | Count of qualifying opportunities created this year for matched accounts. | Derived from Opportunity count query | `src/salesforce_client.py` | `DERIVED` |
| `SALESFORCE-PIPELINE-ARR` | Sum of ARR for advanced-stage opportunities. | Derived from Opportunity ARR query | `src/salesforce_client.py` | `DERIVED` |
| `SALESFORCE-CUSTOMER-MATCH-FLAG` | Whether a Salesforce customer match was found by name/entity. | Derived from account matching logic | `src/salesforce_client.py` | `DERIVED`, `NEEDS-REVIEW` fuzzy matching |

## Internal / Derived

| Identifier | Description | Source field / query surface | Where used | Status note |
|---|---|---|---|---|
| `CUSTOMER-COHORT` | Manufacturing cohort assigned to a customer. | `cohorts.yaml` classification | `src/pendo_client.py`, `docs/CUSTOMER_COHORTS.md` | Internal reference data |
| `PEER-MEDIAN-ACTIVE-RATE` | Median weekly active rate across all peers. | Derived from Pendo customer list | `src/pendo_client.py`, health/benchmark slides | `DERIVED` |
| `COHORT-MEDIAN-ACTIVE-RATE` | Median weekly active rate across the customer's cohort. | Derived from Pendo + cohort mapping | `src/pendo_client.py`, health/benchmark slides | `DERIVED` |
| `DATA-QUALITY-CHECK` | Runtime governance check flag produced by BPO. | `src/qa.py` registry entries | `src/qa.py`, data-quality slide | Internal governance artifact |

## Governance Observations (v1)

### Obvious Missing Data

| Identifier | Why it matters | Likely canonical source |
|---|---|---|
| `LICENSED-SEATS` | Needed for seat-utilization and adoption denominator. | Salesforce, billing, or provisioning system |
| `EXECUTIVE-SPONSOR` | Needed for commercial/account governance and QBR context. | Salesforce |
| `RENEWAL-DATE` | Needed for renewal risk and timing. Contract end date may be a proxy, but the business definition is not formalized here. | Salesforce |
| `SUPPORT-TICKET-HEALTH-OUTSIDE-JIRA` | If support exists in systems other than Jira/JSM, BPO does not registry-link that yet. | TBD |

### Likely Duplicated Or Semantically Overlapping

| Candidate | Systems | Note |
|---|---|---|
| Customer name / organization / account identity | Jira, Pendo, Salesforce, CS Report | Needs canonical naming and alias policy |
| Site / factory / entity identity | Pendo, Jira CMDB refs, CS Report | Needs cross-system site/entity registry |
| Weekly engagement rate | Pendo, CS Report | Similar business meaning, but not identical metric definitions |
| CSM / account owner | Pendo, Jira custom field, possibly Salesforce | Needs canonical owner source decision |

### Likely Wrong-Source Or Needs-Review Candidates

| Candidate | Current behavior | Why review |
|---|---|---|
| `PENDO-CSM-NAME` | Uses `ownername` from visitor metadata | Operationally useful, but likely not canonical CRM ownership |
| `SALESFORCE-CUSTOMER-MATCH-FLAG` | Fuzzy customer matching by `Name` / `LeanDNA_Entity_Name__c` | Matching logic should eventually be governed by stable IDs |
| `JIRA-CSM-FREE-TEXT` | Available in Jira, not used | Likely not the right source for customer ownership |
| `JIRA-SITE-IDS-FREE-TEXT` | Free-text site IDs | Should likely be normalized against CMDB or master site registry |

## Next Steps

1. Add canonical-source decisions for duplicated identities such as customer, site, entity, and CSM.
2. Expand `UNUSED` but available source fields into either active roadmap items or explicit out-of-scope decisions.
3. Add review metadata later if this registry becomes an operational governance control rather than just an inventory.
