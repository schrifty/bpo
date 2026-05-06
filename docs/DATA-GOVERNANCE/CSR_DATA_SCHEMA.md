# Customer Success Report (CS Report / CSR) Data Schema

BPO reads the **latest CS Report** as an **XLSX** file from a shared Google Drive folder, parses the first worksheet into rows, and treats many metric columns as **JSON-encoded KPI objects**.

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
| `delta` | string | Time bucket; BPO filters with **`delta == "week"`** for all public APIs (`_customer_rows`) |
| `factoryName` | string | Factory / site label (primary display name per row) |
| `site` | string | Optional; copied through when column exists |
| `entity` | string | Optional; copied through when column exists |

Headers are taken **exactly** from the spreadsheet (stringified cell values). Optional columns are detected case-insensitively for `site` / `entity`.

---

## 3. KPI cell JSON

Many numeric fields are **not raw numbers** in the sheet. They are stored as JSON strings. BPO parses them with `_parse_kpi`:

| JSON key | Meaning |
|----------|---------|
| `empty` | If truthy, KPI is ignored |
| `startValue` | Period start (optional) |
| `endValue` | **Primary value** used when present |
| `deltaPercent` | Used where `_kpi_delta_pct` is needed |

**`endValue` precedence:** `_kpi_end` prefers `endValue`, then falls back to `startValue`.

Invalid JSON or missing dict → treated as no value.

---

## 4. Columns consumed by BPO (by feature)

Below, “KPI” means a JSON-encoded column as in §3.

### Platform health — `get_customer_platform_health`

| Spreadsheet column | Notes |
|--------------------|--------|
| `healthScore` | String bucket (e.g. GREEN / YELLOW / RED) |
| `shortageItemCount` | KPI |
| `criticalShortages` | KPI |
| `clearToBuildPercent` | KPI |
| `clearToCommitPercent` | KPI |
| `componentAvailabilityPercent` | KPI |
| `componentAvailabilityPercentProjected` | KPI |
| `buyerMappingQualityScore` | KPI |
| `weeklyActiveBuyersPercent` | KPI |
| `aggregateRiskScoreHighCount` | KPI |

### Supply chain — `get_customer_supply_chain`

| Spreadsheet column | Notes |
|--------------------|--------|
| `totalOnHandValue` | KPI |
| `totalOnOrderValue` | KPI |
| `excessOnhandValuePositive` | KPI |
| `excessOnOrderValuePositive` | KPI |
| `doiForwards` | KPI |
| `daysCoverage` | KPI |
| `pastDuePOValue` | KPI |
| `pastDueRequirementValue` | KPI |
| `latePOCount` | KPI |
| `latePRCount` | KPI |
| `dailyInventoryUsage` | KPI (present in code path; not all slides use it) |
| `toiForwards` | KPI |

### Platform value / ROI — `get_customer_platform_value`

| Spreadsheet column | Notes |
|--------------------|--------|
| `inventoryActionCurrentReportingPeriodSavings` | KPI |
| `inventoryActionOpenValue` | KPI |
| `recsCreatedLast30DaysCt` | KPI |
| `posPlacedInLast30DaysCt` | KPI |
| `workbenchOverdueTasksCt` | KPI |
| `potentialSavings` | KPI |
| `potentialToSell` | KPI |
| `currentFySpend` | KPI |
| `previousFySpend` | KPI |

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
