# Customer Success Report (CS Report / CSR) Data Schema

Cortex reads the **latest CS Report** as an **XLSX** file from a shared Google Drive folder, parses the first worksheet into rows, and treats many metric columns as **JSON-encoded KPI objects**.

Implementation: [`src/cs_report_client.py`](../../src/cs_report_client.py).  
Cross-links: governance registry in [`DATA_REGISTRY.md`](./DATA_REGISTRY.md).

In a **merged health report** (e.g. from `PendoClient.get_customer_health_report`), CS Report payloads are grouped under **`report["csr"]`**:

| Key | Produced by |
|-----|----------------|
| `csr["platform_health"]` | `get_customer_platform_health` |
| `csr["supply_chain"]` | `get_customer_supply_chain` |
| `csr["platform_value"]` | `get_customer_platform_value` |

`get_csr_section(report)` returns this object and still accepts legacy top-level `cs_platform_health` / `cs_supply_chain` / `cs_platform_value` for older JSON.

---

## 1. Transport & file shape

| Item | Detail |
|------|--------|
| **Source** | Google Shared Drive “Data Exports” (`_DATA_EXPORTS_DRIVE_ID` in code), folder `_CS_REPORT_FOLDER_ID` |
| **Selection** | Newest file by `modifiedTime` in that folder |
| **Format** | `files.export` → `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` |
| **Worksheet** | First sheet only (`wb[wb.sheetnames[0]]`) |
| **Header row** | Row 1 = column names; subsequent rows are data |
| **Row filter** | Rows must have a non-empty `customer` column to be loaded |

---

## 2. Row identity & time bucket

| Column | Type | Usage |
|--------|------|--------|
| `customer` | string | Required; matched case-insensitively to the deck customer |
| `delta` | string | Time bucket; Cortex filters with **`delta == "week"`** for all public APIs (`_customer_rows`) |
| `factoryName` | string | Factory / site label (primary display name per row) |
| `site` | string | Optional; copied through when column exists |
| `entity` | string | Optional; copied through when column exists |

Headers are taken **exactly** from the spreadsheet (stringified cell values). Optional columns are detected case-insensitively for `site` / `entity`.

---

## 3. KPI cell JSON

Many numeric fields are **not raw numbers** in the sheet. They are stored as JSON strings. Cortex parses them with `_parse_kpi`:

| JSON key | Meaning |
|----------|---------|
| `empty` | If truthy, KPI is ignored |
| `startValue` | Period start (optional) |
| `endValue` | **Primary value** used when present |
| `deltaPercent` | Used where `_kpi_delta_pct` is needed |

**`endValue` precedence:** `_kpi_end` prefers `endValue`, then falls back to `startValue`.

Invalid JSON or missing dict → treated as no value.

---

## 4. Columns consumed by Cortex (by feature)

Cortex maps **every metric column** in the current CS Report XLSX (72 columns as of 2026-07) into the per-customer export. Row filters (`customer`, `delta=week`) and identity columns (`factoryName` → `factory`, `entity`, optional `site`) are used for matching; KPI JSON cells export **`endValue`** under snake_case keys on each merged factory row.

Full export column order lives in ``CSR_MERGED_SITE_EXPORT_COLUMNS`` in ``cs_report_client.py`` (markdown §13.2 and spreadsheet tab ``csr_factories``).

Below, “KPI” means a JSON-encoded column as in §3.

### Platform health — `get_customer_platform_health`

| Spreadsheet column | Export key | Notes |
|--------------------|------------|--------|
| `healthScore` | `health_score` | When `NONE`, may fall back to `automatedHealthScores[0]` |
| `automatedHealthScores` | `automated_health_composite`, `automated_health_override`, `automated_health_scores` | Raw JSON preserved on `automated_health_scores` |
| `shortageItemCount` | `shortages` | KPI |
| `criticalShortages` | `critical_shortages` | KPI |
| `shortagesByOrderLines` | `shortages_by_order_lines` | KPI |
| `clearToBuildPercent` | `clear_to_build_pct` | KPI |
| `clearToCommitPercent` | `clear_to_commit_pct` | KPI |
| `componentAvailabilityPercent` | `component_availability_pct` | KPI |
| `componentAvailabilityPercentProjected` | `component_availability_projected_pct` | KPI |
| `buyerMappingQualityScore` | `buyer_mapping_quality` | KPI |
| `weeklyActiveBuyersPercent` | `weekly_active_buyers_pct` | KPI |
| `dailyActiveBuyersPercent` | `daily_active_buyers_pct` | KPI |
| `dailyEngagedBuyersPercent` | `daily_engaged_buyers_pct` | KPI |
| `weeklyEngagedIABuyersPercent` | `weekly_engaged_ia_buyers_pct` | KPI |
| `weeklyEngagedSuppliersPercent` | `weekly_engaged_suppliers_pct` | KPI |
| `aggregateRiskScoreHighCount` | `high_risk_items` | KPI |
| `businessUnit` | `business_unit` | plain |
| `division` | `division` | plain |
| `region` | `region` | plain |
| `customerNdx` | `customer_ndx` | plain |
| `factoryNdx` | `factory_ndx` | plain |
| `dateCreated` / `dateModified` / `startDate` / `endDate` | `date_created`, etc. | plain |

### Supply chain — `get_customer_supply_chain`

| Spreadsheet column | Export key | Notes |
|--------------------|------------|--------|
| `totalOnHandValue` | `on_hand_value` | KPI; rolled to `totals.on_hand` |
| `totalOnOrderValue` | `on_order_value` | KPI |
| `excessOnhandValuePositive` | `excess_on_hand` | KPI |
| `excessOnOrderValuePositive` | `excess_on_order_value` | KPI; rolled to `totals.excess_on_order` |
| `excessOnhandDemandedValue` | `excess_onhand_demanded_value` | KPI |
| `excessOnhandObsoleteValue` | `excess_onhand_obsolete_value` | KPI |
| `excessOnOrderObsoleteValue` | `excess_on_order_obsolete_value` | KPI |
| `manufacturedInventoryValue` | `manufactured_inventory_value` | KPI |
| `earlyDeliveriesValue` | `early_deliveries_value` | KPI |
| `doiForwards` | `doi_days` | KPI |
| `doiBackwards` | `doi_backwards` | KPI |
| `daysCoverage` | `days_coverage` | KPI |
| `onOrderDays` | `on_order_days` | KPI |
| `pastDuePOValue` | `past_due_po_value` | KPI |
| `pastDueRequirementValue` | `past_due_req_value` | KPI |
| `latePOCount` | `late_pos` | KPI |
| `latePRCount` | `late_prs` | KPI |
| `dailyInventoryUsage` | `daily_inventory_usage` | KPI |
| `toiForwards` | `turns_of_inventory` | KPI |
| `toiBackwards` | `toi_backwards` | KPI |
| `openPoCt` | `open_po_ct` | KPI |
| `nonCompliantPosCt` | `non_compliant_pos_ct` | KPI |
| `apexPoActionPoCt` | `apex_po_action_po_ct` | KPI |
| `erpExceptionMsgPoCt` | `erp_exception_msg_po_ct` | KPI |
| `supplierCt` | `supplier_ct` | KPI |
| `supplierCommitDatePercent` | `supplier_commit_date_pct` | KPI |
| `commitDateCoverage` | `commit_date_coverage_pct` | KPI |

### Platform value / ROI — `get_customer_platform_value`

| Spreadsheet column | Export key | Notes |
|--------------------|------------|--------|
| `inventoryActionCurrentReportingPeriodSavings` | `savings_current_period` | KPI |
| `inventoryActionOpenValue` | `open_ia_value` | KPI |
| `inventoryActionCurrentReportingPeriodOpenValue` | `ia_current_period_open_value` | KPI |
| `inventoryActionPreviousReportingPeriodSavings` | `ia_previous_period_savings` | KPI |
| `inventoryActionFixRateTrailing90Days` | `ia_fix_rate_trailing_90d` | KPI |
| `inventoryActionUnableToFixRateTrailing90Days` | `ia_unable_to_fix_rate_trailing_90d` | KPI |
| `inventoryActionCurrentReportingPeriod` | `ia_current_reporting_period` | plain label |
| `inventoryActionPreviousReportingPeriod` | `ia_previous_reporting_period` | plain label |
| `recsCreatedLast30DaysCt` | `recs_created_30d` | KPI |
| `posPlacedInLast30DaysCt` | `pos_placed_30d` | KPI |
| `workbenchOverdueTasksCt` | `overdue_tasks` | KPI |
| `potentialSavings` | `potential_savings` | KPI |
| `potentialToSell` | `potential_to_sell` | KPI |
| `currentFySpend` | `current_fy_spend` | KPI |
| `previousFySpend` | `previous_fy_spend` | KPI |
| `currentWeek52ldnaTarget` | `current_week52_ldna_target` | KPI |

---

## 5. Cross-validation (Pendo)

`cross_validate_with_pendo` compares CS Report rows (week) to Pendo health: site counts, weekly engagement proxy vs `weeklyActiveBuyersPercent`, and factory names vs Pendo `sitename`. See `cs_report_client.py` for exact rules.

---

## 6. Gaps & stability

- **Schema drift:** New columns in the XLSX are ignored until referenced in code; renames break KPI extraction.
- **Sheet:** Only the first worksheet is read; multi-sheet workbooks may hide data.
- **Caching:** Parsed rows are cached in-process after first download.

---

## 7. Related

- [`DATA_REGISTRY.md`](./DATA_REGISTRY.md) — CS Report identifiers (`CSR-*`)
- [`PENDO_DATA_SCHEMA.md`](./PENDO_DATA_SCHEMA.md) — overlap with engagement / site semantics
