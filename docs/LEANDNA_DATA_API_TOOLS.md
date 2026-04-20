# LeanDNA Data API - Tool Opportunities for BPO

This document analyzes the LeanDNA Data API swagger spec and outlines what tools/integrations can be built to enhance BPO's QBR deck generation and CS reporting.

## API Overview

**Base URL:** `https://app.leandna.com/api`  
**Auth:** `Authorization: Bearer {token}` (from env `DATA_API_BEARER_TOKEN`)  
**Scope Control:** `RequestedSites` header (comma-separated site IDs)  
**Total Endpoints:** 40+ operations across 10 tag groups

---

## Key Data Available (That BPO Currently Lacks or Gets from CS Report XLSX)

### 1. **Item Master Data** (`/data/ItemMasterData`)

**Rich inventory metrics per item/site:**

| Field | Relevance to BPO | Notes |
|-------|------------------|-------|
| `daysOfInventoryBackward` | ✅ **NEW** — User asked about "DOI Backwards" | Not in current CSR schema |
| `daysOfInventoryForward` | ✅ Maps to CSR `doiForwards` | Live API vs static export |
| `daysOfCoverageWorkDays` | ✅ Maps to CSR `daysCoverage` | — |
| `excessOnHandQty`, `excessOnHandValue` | ✅ Supply chain slide | — |
| `aggregateRiskScore`, `riskLevel` | ⭐ **NEW** — item-level risk not in CSR | Could enhance platform health |
| `leadTime`, `observedLeadTime` | ⭐ **NEW** — performance vs plan | Supplier scorecard use |
| `abcRank`, `criticalityLevel` | ⭐ **NEW** — prioritization | Not exposed in CSR |
| `weeklyDemandStdDev`, `futureDemandDaily` | ⭐ **NEW** — forecasting | — |
| `ctbImpactedOrders`, `ctbShortageImpactedValue` | ✅ Clear-to-build metrics | — |

**Tool idea:** `leandna_item_master_data(customer, site=None, item_codes=None)` → JSON for QBR slides; cache with TTL; add to `DATA_REGISTRY`.

---

### 2. **Material Shortages** (Daily/Weekly/Monthly)

**Endpoints:**
- `/data/MaterialShortages/ShortagesByItem/Daily`
- `/data/MaterialShortages/ShortagesByItem/Weekly`
- `/data/MaterialShortages/ShortagesByItem/Monthly`
- `/data/MaterialShortages/ShortagesByOrder`
- `/data/MaterialShortages/ShortagesByItemWithScheduledDeliveries/Daily` (+ Weekly/Monthly)

**Key fields:**
- `daysInShortage`, `currentShortage`, `criticalityLevel`
- `firstPORequestedDate`, `firstPOCommitDate`, `deliveryDateStatus`
- `bucket1...bucket32` (weekly) or `day1...day45` (daily) with `quantity`, `criticality`, `startDate`, `endDate`
- `firstImpactedOrder`, `firstImpactedOrderDate`, `firstImpactedSO`
- `scheduledDeliveries`, `scheduledQuantity`, `firstDeliveryDate` (with scheduled deliveries variant)

**Tool ideas:**
1. **Shortage trend tool** — time series for QBR "platform health" or new "shortages over time" slide.
2. **Critical shortage alerts** — filter `criticalityLevel=Critical`, surface in speaker notes or exec summary.
3. **Bucket visualization** — daily/weekly buckets → chart data for "shortage forecast" slide (not in current QBR template).

---

### 3. **Purchase Orders** (`/data/SupplyOrder/PurchaseOrder`)

**Fields:**
- `poStatus`, `poCommitDate`, `confirmationStatus`, `deliveryStatus`
- `originalRequestedDeliveryDate`, `currentRequestedDeliveryDate`, `currentExpectedDeliveryDate`
- `depthOfDelay`, `lateDeliveryCause`, `lateDeliveryLiability`
- `scheduledDeliveries`, `firstDeliveryDate`, `firstDeliveryTrackingNumber`
- `openPoValue`, `futurePPV` (purchase price variance)
- `leadTime`, `poLeadTime`, `actualLeadTime` (performance vs target)

**Tool ideas:**
1. **Late PO summary** — aggregate `depthOfDelay`, `lateDeliveryCause` → supply chain slide "Late POs" narrative.
2. **Supplier performance** — `actualLeadTime` vs `leadTime` by supplier → new slide or speaker notes.
3. **PO tracking detail** — drill-down for QA or user questions in agent mode.

---

### 4. **Inventory** (`/data/Inventory/Purchased`)

**Fields:**
- `quantity`, `value`, `unitPrice`, `state`, `location`
- `availableDate`, `orderNumber`, `specialStock`
- `materialType`, `materialGroup`

**Tool idea:** Inventory snapshot by site/location → cross-check CSR totals or enrich "on-hand" narratives.

---

### 5. **Lean Projects** (Multiple endpoints)

**Endpoints:**
- `/data/LeanProject` (list all, filter by `startMonth`/`endMonth`)
- `/data/LeanProject/{projectId}/Tasks`, `/data/LeanProject/{projectId}/Issues`
- `/data/LeanProject/{projectIds}/Savings` (monthly target vs actual)
- `/data/LeanProject/{projectIds}/Stage/History`
- `/data/LeanProject/Areas`, `/data/LeanProject/Types`, `/data/LeanProject/Categories`

**Fields (project):**
- `name`, `stage`, `state` (good/warn/bad), `startDate`, `dueDate`
- `totalActualSavingsForPeriod`, `totalTargetSavingsForPeriod`
- `isBestPractice`, `isProjectResultsValidated`
- `customFieldValues` (extensible metadata)

**Fields (savings):**
- `month`, `savingsCategory`, `savingsType`, `actual`, `target`, `weightedTarget`

**Tool ideas:**
1. **Project portfolio slide** — list active projects, stage distribution, total savings vs target for QBR period.
2. **Savings waterfall** — monthly actual vs target chart (currently not in QBR; would be high-value add).
3. **Best practice spotlight** — filter `isBestPractice=true` → speaker notes or exec summary.
4. **Task/issue health** — count overdue tasks, open issues per project → project mgmt health metric.

---

### 6. **Metrics** (`/data/Metric`, `/data/MetricReport`)

**`/data/Metric`:** List all metrics (Manual, Automatic, ProcurementLog, Calculated) with `name`, `siteId`, `possibleValueStreams`, `currentCategories`.

**`/data/MetricReport`:** Monthly aggregated metric data for fiscal year, filtered by `metrics` (ids), `valueStreams`.

**Response:**
- `metrics` — metadata array
- `metricValues` — time-series data per metric/month
- `fiscalYear`, `startTimestamp`, `endTimestamp`, `currency`

**Tool idea:** Generic metric fetcher for custom KPIs → slides/charts. Example: if CSR lacks a field, customer can define a LeanDNA metric and BPO pulls it via this endpoint.

---

### 7. **Data Share** (`/data/DataShare`)

**Parquet reports** (bulk exports):
- `ClearToBuildMultiLevelProductionOrder`, `ClearToBuildSingleLevelRequirements`
- `InventoryBurnoff`, `ItemsUnderSafetyStock`, `LineOfBalance`
- `PastDueRequirements`, `PlanForEveryPartAllPurchasedItems`
- `PlannedProductionOrders`, `ProcurementTasks`, `ProductionOrders`
- `PurchaseOrders`, `PurchaseRequisitions`, `SupplierPerformance`

**Response:** Signed download URLs (parquet format, 1h expiry).

**Tool idea:**
1. **Bulk ingest alternative** — replace CS Report XLSX with parquet downloads; richer, more granular.
2. **Scheduled sync** — nightly job downloads latest parquet, normalizes to BPO schema, caches on Drive.
3. **CTB detail** — `ClearToBuildMultiLevelProductionOrder` → detailed CTB slide (multi-level BOM view not in CSR).

---

### 8. **Session Info** (`/data/identity`)

**Fields:** `userId`, `customerId`, `userName`, `emailAddress`, `authorizedSites` (with `siteId`, `siteName`, `entity`, `division`, `businessUnit`, `currencyCode`).

**Tool idea:** Probe/health check; log authorized sites for QA; confirm customer site mapping before QBR run.

---

### 9. **Write Back** (Read + Update)

**`/data/WriteBack/v1/PurchaseOrderActions` (GET):** Retrieve `WAITING` actions (e.g., `PurchaseOrderCommitDate`, `PurchaseOrderTrackingNumber`, `PurchaseOrderComment`, etc.).

**`/data/WriteBack/v1/TransitionActions` (PUT):** Update action status (`WAITING` → `RUNNING` → `SUCCESSFUL`/`FAILED`/`SKIPPED`).

**Tool idea (advanced):** If BPO agent mode includes "request PO update" flow, surface `WAITING` actions and guide user to ERP write-back (read-only for now; no auto-write unless explicitly requested and scoped).

---

## Comparison: LeanDNA Data API vs CS Report XLSX

| Capability | CS Report (current) | LeanDNA Data API | Advantage |
|------------|---------------------|------------------|-----------|
| **DOI Backwards** | ❌ Not documented | ✅ `daysOfInventoryBackward` in ItemMasterData | API only |
| **DOI Forwards** | ✅ `doiForwards` | ✅ `daysOfInventoryForward` | Live vs static |
| **CTB %** | ✅ `clearToBuildPercent` (KPI JSON) | ✅ Multiple CTB fields in ItemMasterData + Shortages | API: item-level, CSR: site aggregate |
| **Shortage detail** | ❌ Aggregate counts only | ✅ Daily/weekly/monthly buckets, scheduled deliveries | API: time-series + granular |
| **Lean Projects** | ❌ Not in CSR | ✅ Full project + savings API | API only |
| **Supplier performance** | ❌ Not in CSR | ✅ PO lead time actual vs target | API only |
| **Risk scores** | ❌ Not in CSR | ✅ `aggregateRiskScore`, `riskLevel` per item | API only |
| **Multi-level CTB** | ❌ Not in CSR | ✅ Data Share parquet | API only |
| **Historical trends** | ⚠️ One snapshot per export | ✅ Daily/weekly/monthly endpoints | API: built-in time windows |

**Recommendation:** LeanDNA Data API is **superset** of CS Report for most metrics. Consider **hybrid** approach:
1. Keep CSR XLSX for backward compat / stable schema.
2. Add **optional** Data API integrations for high-value gaps (DOI backwards, Lean Projects, shortage trends, supplier perf).
3. Long-term: migrate to Data API as primary source if parquet Data Share covers all CSR fields.

---

## Proposed BPO Tools (Priority Order)

### High Priority (QBR Value Add)

1. **`leandna_item_master_tool`**  
   - **GET** `/data/ItemMasterData`  
   - **Use:** Enrich supply chain slide with DOI backwards, risk scores, ABC rank, lead time variance.  
   - **Cache:** 24h TTL on Drive (JSON snapshot).  
   - **QA:** Cross-validate `daysOfInventoryForward` vs CSR `doiForwards`.

2. **`leandna_shortage_trends_tool`**  
   - **GET** `/data/MaterialShortages/ShortagesByItem/Weekly` (or Daily/Monthly)  
   - **Use:** New QBR slide: "Shortage Forecast" with weekly buckets chart; "Critical Shortages" list.  
   - **Cache:** Snapshot per QBR run.

3. **`leandna_lean_projects_tool`**  
   - **GET** `/data/LeanProject`, `/data/LeanProject/{projectIds}/Savings`  
   - **Use:** New QBR slide: "Lean Project Portfolio" with savings waterfall, stage distribution.  
   - **Cache:** Snapshot per QBR run.

4. **`leandna_purchase_orders_tool`**  
   - **GET** `/data/SupplyOrder/PurchaseOrder`  
   - **Use:** Enrich "Late POs" narrative; supplier performance heatmap (new slide or speaker notes).  
   - **Cache:** 24h TTL.

### Medium Priority (CS Report Replacement Path)

5. **`leandna_data_share_bulk_tool`**  
   - **GET** `/data/DataShare` → download parquet  
   - **Use:** Nightly sync to Drive; parse parquet → normalize to CSR schema; replace XLSX dependency.  
   - **Cache:** Parquet files on Drive with metadata (reportDate, etag).

6. **`leandna_inventory_tool`**  
   - **GET** `/data/Inventory/Purchased`  
   - **Use:** Cross-check CSR on-hand totals; location-level drill-down for QA.

### Low Priority (Agent Mode / Advanced)

7. **`leandna_metrics_tool`**  
   - **GET** `/data/Metric`, `/data/MetricReport`  
   - **Use:** Custom KPI fetch for customers with non-standard metrics; fiscal-year trends.

8. **`leandna_identity_tool`**  
   - **GET** `/data/identity`  
   - **Use:** Preflight check; log authorized sites; confirm site mapping.

9. **`leandna_writeback_tool`** (read-only initially)  
   - **GET** `/data/WriteBack/v1/PurchaseOrderActions`  
   - **Use:** Agent mode: "Show pending PO updates" for context (no auto-write).

---

## Implementation Notes

### Auth & Config

Add to `.env` / `src/config.py`:

```python
# LeanDNA Data API
LEANDNA_DATA_API_BASE_URL = os.environ.get("LEANDNA_DATA_API_BASE_URL", "https://app.leandna.com/api")
LEANDNA_DATA_API_BEARER_TOKEN = os.environ.get("LEANDNA_DATA_API_BEARER_TOKEN")  # required
# Optional: default sites (comma-separated IDs) if not passed per-call
LEANDNA_DATA_API_DEFAULT_SITES = os.environ.get("LEANDNA_DATA_API_DEFAULT_SITES", "").strip()
```

### Client Module (`src/leandna_data_client.py`)

```python
"""LeanDNA Data API client for BPO.

Thread-safe caching (in-memory + optional Drive backup).
All GET endpoints return JSON; POST/PUT methods guarded with confirmation (not exposed to agent by default).
"""
import requests
from .config import LEANDNA_DATA_API_BASE_URL, LEANDNA_DATA_API_BEARER_TOKEN, logger

def _headers(requested_sites: str | None = None) -> dict:
    h = {
        "Authorization": f"Bearer {LEANDNA_DATA_API_BEARER_TOKEN}",
        "Accept": "application/json",
    }
    if requested_sites:
        h["RequestedSites"] = requested_sites
    return h

def get_item_master_data(sites: str | None = None) -> list[dict]:
    """Retrieve Item Master Data (all items across requested sites)."""
    r = requests.get(
        f"{LEANDNA_DATA_API_BASE_URL}/data/ItemMasterData",
        headers=_headers(sites),
        timeout=120,
    )
    r.raise_for_status()
    return r.json()

# Similar for other endpoints...
```

### Tool Wrappers (`src/tools/leandna_tool.py`)

```python
"""LangChain tool wrappers for LeanDNA Data API (agent mode)."""
from langchain.tools import tool

@tool
def leandna_item_master_data(site_ids: str = "") -> str:
    """Retrieve Item Master Data from LeanDNA (inventory metrics, DOI, risk scores).
    
    Args:
        site_ids: Comma-separated site IDs (optional; defaults to all authorized sites).
    """
    from ..leandna_data_client import get_item_master_data
    data = get_item_master_data(sites=site_ids or None)
    # Summarize for agent (top N items by risk, DOI, etc.)
    # Return compact JSON or markdown table
    return json.dumps({"item_count": len(data), "items": data[:10]}, indent=2)
```

### QBR Integration

In `src/qbr_template.py` or new `src/leandna_qbr_enrich.py`:

```python
def enrich_supply_chain_with_leandna(report: dict, customer: str) -> dict:
    """Optionally augment CSR supply_chain with LeanDNA Item Master Data."""
    if not LEANDNA_DATA_API_BEARER_TOKEN:
        return report  # skip if not configured
    
    from .leandna_data_client import get_item_master_data
    # Map customer → site IDs (from teams.yaml or identity endpoint)
    sites = _resolve_customer_sites(customer)
    items = get_item_master_data(sites=sites)
    
    # Add fields not in CSR: daysOfInventoryBackward, aggregateRiskScore, etc.
    report.setdefault("leandna_enrichment", {})["item_master"] = {
        "count": len(items),
        "high_risk_items": [i for i in items if i.get("aggregateRiskScore", 0) > 80],
        "doi_backward_avg": ...,  # calculate
    }
    return report
```

---

## Schema Documentation

Add fields to `docs/data-schema/DATA_REGISTRY.md`:

| Identifier | Description | Source field | Where used | Status |
|------------|-------------|--------------|------------|--------|
| `DAYS-OF-INVENTORY-BACKWARD` | Backward-looking DOI | `daysOfInventoryBackward` (ItemMasterData) | `leandna_data_client.py`, supply chain slide | `AVAILABLE` (API only) |
| `AGGREGATE-RISK-SCORE` | Item-level risk score | `aggregateRiskScore` (ItemMasterData) | `leandna_data_client.py`, platform health slide | `AVAILABLE` (API only) |
| `LEAN-PROJECT-SAVINGS-ACTUAL` | Actual project savings for period | `totalActualSavingsForPeriod` (LeanProject) | `leandna_data_client.py`, Lean Projects slide | `AVAILABLE` (API only) |
| ... | | | | |

---

## Testing & Rollout

1. **Phase 1 (Prototype):** Implement `leandna_item_master_tool` + `leandna_shortage_trends_tool`; manual test with one customer.
2. **Phase 2 (QBR Pilot):** Add Lean Projects tool; generate test QBR with new slides; compare CSR vs API data quality.
3. **Phase 3 (Agent Mode):** Expose tools to LangChain agent; test "What is our DOI backward?" query.
4. **Phase 4 (CS Report Replacement):** Evaluate Data Share parquet; if coverage is complete, deprecate XLSX dependency.

---

## Open Questions

1. **Site mapping:** How to resolve BPO `customer` → LeanDNA `siteId` list? (via `/data/identity` + `teams.yaml` or new mapping file?)
2. **Rate limits:** Swagger doesn't document rate limits; confirm with LeanDNA support before production.
3. **Field coverage:** Does `daysOfInventoryBackward` exist in real API responses? (User asked about it; schema shows it, but validate with live call.)
4. **Write-back scope:** Should BPO ever auto-update POs via `/data/WriteBack/v1/TransitionActions`, or strictly read-only? (Recommend read-only for now.)

---

## Related Files

- **Script:** `scripts/fetch_leandna_data_api_swagger.py` — fetch & parse swagger (already created)
- **Client:** `src/leandna_data_client.py` — HTTP client for Data API (to be created)
- **Tools:** `src/tools/leandna_tool.py` — LangChain tool wrappers (to be created)
- **QBR enrich:** `src/leandna_qbr_enrich.py` — optional enrichment for QBR (to be created)
- **Tests:** `tests/test_leandna_data_client.py` — unit tests with mocked responses (to be created)
- **Docs:** This file + `DATA_REGISTRY.md` updates

---

## Summary

The **LeanDNA Data API is a high-value integration** for BPO. It provides:

1. **Gaps filled:** DOI backwards, Lean Projects, shortage trends, supplier performance, item-level risk.
2. **Real-time data:** Live API vs static XLSX export (fresher for QBRs).
3. **Granularity:** Daily/weekly buckets, multi-level CTB, scheduled deliveries.
4. **Extensibility:** Custom metrics, parquet bulk exports, write-back hooks (future).

**Recommended first step:** Implement `leandna_item_master_tool` and `leandna_shortage_trends_tool` as proof-of-concept; validate schema and data quality with one pilot customer. If successful, roll out Lean Projects tool and evaluate CS Report replacement path via Data Share parquet.
