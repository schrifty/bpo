# LLM export-all consumer guide

Audience: engineers, analysts, and LLMs reading **`LLM-Context-All_Customers.md`** (from `cortex export-all` or `python -m src.export_llm_context_snapshot`).

This document lists what the snapshot contains, how scopes and caps work, and known caveats so consumers do not misread merged or partial data.

---

## Command and output

| Item | Detail |
|------|--------|
| **CLI** | `cortex export-all [--days N] [--skip-risk-insights] [--customers-sf-allowlist] [--customers-exclude-sf-churned] [--exclude-customer NAME …]` |
| **Output** | `LLM-Context-All_Customers.md` on Google Drive under the generator `Output/` folder (stable path + dated bundle folder) |
| **Lookback** | `--days` (default 90) drives Pendo portfolio rollup and Jira HELP lookback |
| **Profile** | `llm_export_all_customers` — see `src/data_sources/llm_export_report.py` |

---

## Snapshot sections (typical markdown layout)

| Section | Report key(s) | Source(s) | Scope |
|---------|---------------|-----------|--------|
| **§1 Portfolio customers** | `customers[]`, `portfolio_signals` | Pendo (+ Salesforce merge for SF-only rows) | Active installed base; true **CHURNED** SF labels stripped when universe merge runs |
| **§2 Jira HELP** | `jira` | Jira Service Management (project HELP) | **`top_ultimate_parents_by_arr`** — top N ultimate parents by **`current_arr`**; merged JSM `Organizations in (...)` across parent + subsidiary names |
| **§3 Salesforce (active book)** | `salesforce`, `portfolio_revenue_book` | Salesforce Customer Entity accounts | **Current book only** (`ACTIVE` + `OUT_OF_CONTRACT_RENEWING`); see commercial status below |
| **§3b Churned / lost** | `salesforce_churned_segment` | Salesforce only | `commercial_status = CHURNED` — **do not merge** with §1/§3 |
| **§3b-renewal** | `salesforce_renewal_negotiation_segment` | Salesforce only | `OUT_OF_CONTRACT_RENEWING` — open parent-account renewal pipeline |
| **§3b-future** | `salesforce_future_contract_segment` | Salesforce only | `FUTURE` — won/signed contract not yet started |
| **§3c SF comprehensive** | `salesforce_comprehensive_portfolio` | Salesforce multi-object CRM | Per-label categories + `entity_accounts[]` + **`arr_by_ultimate_parent[]`** |
| **§4 CS Report** | `csr` | CS Report week export | Same top-N ultimate parents as §2 (`top_ultimate_parents_by_arr`) |
| **§5 Portfolio signals** | `portfolio_signals` | Pendo-derived | Active book; churned SF keys removed when configured |
| **§6 Slack** (optional) | `slack` | Slack channel digests | Top ultimate parents by `current_arr` when `CORTEX_LLM_EXPORT_SLACK` enabled |
| **§7 Risk insights** (optional) | appended LLM section | LLM over export payload | Skipped with `--skip-risk-insights` |

---

## Salesforce commercial status (replaces boolean `active`)

Each row in `portfolio_revenue_book.matched_customer_contract_rollups[]` (and mirrored export accounts) uses **`commercial_status`** instead of a bare `active` flag.

| Status | Meaning | In §3 active book? | In current-ARR ranking? |
|--------|---------|--------------------|-------------------------|
| **`ACTIVE`** | At least one entity with non-churned `Contract_Status__c` | Yes | Yes (`active_arr`) |
| **`OUT_OF_CONTRACT_RENEWING`** | No active contract; open renewal pipeline on parent account (stages 3–5) | Yes (negotiation segment also in §3b-renewal) | Yes (`renewal_arr`) |
| **`CHURNED`** | No active contract and no qualifying renewal pipeline | No (§3b only) | No |
| **`FUTURE`** | No active contract yet; won/signed contract with future start (or pending-activation status) | No (§3b-future only) | No |

**Churned contract statuses** (entity level): `churned`, `cancelled`, `terminated`, `expired`, `closed` (case-insensitive).

---

## ARR fields on each rollup row

| Field | Definition |
|-------|------------|
| **`historical_arr`** | Sum of `Account.ARR__c` for all entities in the reporting group (regardless of status) |
| **`active_arr`** | ARR from entities with an active (non-churned) contract |
| **`renewal_arr`** | Entity ARR when `commercial_status = OUT_OF_CONTRACT_RENEWING`; otherwise `0` |
| **`current_arr`** | **`active_arr + renewal_arr`** — use this for “top customers by current ARR” |
| **`arr`** | Alias of **`historical_arr`** (legacy key name) |

Book-level totals on `portfolio_revenue_book` include `active_arr`, `renewal_arr`, `current_arr`, `historical_arr`, `churned_contract_arr`, `future_contract_arr`, and legacy names `active_installed_base_arr` / `renewal_in_flight_contract_arr` aligned to the same logic.

**Ranking:** `top_customers_by_arr` and LLM top-N slices (§2, §4, §3c cap, Slack) sort by **`current_arr`**, not raw historical ARR alone.

---

## Top-N caps (env overrides)

| Surface | Env var | Default |
|---------|---------|---------|
| CS Report §4 | `CORTEX_LLM_EXPORT_CSR_TOP_N` | 20 |
| Jira §2 | `CORTEX_LLM_EXPORT_JIRA_TOP_N` | same as CSR |
| Slack §6 | `CORTEX_LLM_EXPORT_SLACK_TOP_N` | same as CSR |
| SF comprehensive §3c | `CORTEX_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP` | 12 (`0` / `all` = uncapped) |

Selection groups Salesforce contract rollups by **ultimate parent** (parenthetical names, corporate rollups, optional SF Ultimate Parent lookup) so Carrier-style divisions collapse to one ranked parent.

---

## Critical caveats for consumers

### 1. Do not merge inactive segments into active totals

§3b, §3b-renewal, and §3b-future are **Salesforce-only** and labeled **`do_not_merge_with_active_book`**. Summing §3 + §3b ARR double-counts or mixes churn with installed base.

### 2. `commercial_status` vs legacy `active`

New exports emit **`commercial_status`** and ARR components. The boolean **`active`** is **no longer written** on new rollups. Older snapshots may still show `active`; treat **`commercial_status`** as authoritative when present.

### 3. Renewal customers stay in executive rankings

Customers in **`OUT_OF_CONTRACT_RENEWING`** appear in top-by-ARR slices via **`current_arr`** so Ford-style renewal negotiations do not disappear from Jira/CSR/SF comprehensive caps while still being distinguishable from fully **`ACTIVE`** accounts.

### 4. `arr_by_ultimate_parent` vs contract rollups

§3c **`arr_by_ultimate_parent`** sums **entity account `ARR__c`** by ultimate parent. §3 rollups sum by **corporate reporting group** from contract rollups. Totals may differ slightly when grouping rules or entity coverage differ.

### 5. Jira org naming is merged, not perfect

§2 uses one HELP query per ultimate parent with **`Organizations in (...)`** across parent name, Salesforce labels, and division-only aliases. Dirty JSM directory names can still miss tickets; check `jira_lookup_keys`, `jira_match_terms`, and `jsm_organizations_resolved`.

### 6. Pendo is not authoritative for customer inventory or churn

Salesforce Customer Entity accounts decide **who is a customer** and **commercial_status**. Pendo prefixes enrich usage; absence of Pendo does not imply churn.

### 7. Salesforce cache staleness

In-process Salesforce cache (`CORTEX_SALESFORCE_CACHE_TTL_HOURS`, default 48h) can make contract status and ARR stale. Check export coverage / provenance for load time when decisions are CRM-sensitive.

### 8. CS Report / Jira name aliases

Cross-system joins use `config/cs_report_customer_aliases.yaml`, `config/jsm_organization_aliases.yaml`, cohort maps, and Pendo prefix allowlists. Unmapped labels produce partial or empty §2/§4 blocks—not silent success.

### 9. Compaction and size caps

Large exports compact JSON (rollup row caps, Jira count-only mode, CSR site limits). **`export_compaction`** metadata in the markdown describes what was truncated.

### 10. Risk insights §7 is LLM-generated

§7 is not sourced from CRM; failures are inlined in the section. Use `--skip-risk-insights` for deterministic-only snapshots.

---

## Related docs

- [`SALESFORCE_REVENUE_AND_ARR.md`](./SALESFORCE_REVENUE_AND_ARR.md) — ARR source of truth (`Account.ARR__c`)
- [`DATA_DICTIONARY.md`](./DATA_DICTIONARY.md) — field-level catalog
- Workspace rule **salesforce-canonical-customers** — active vs churned presentation policy
